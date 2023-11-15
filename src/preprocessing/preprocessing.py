from math import radians

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import haversine_distances


def split_lat_lon(data):
    modified_data = data.copy()
    modified_data["LON_SEQUENCE"] = modified_data.SEQUENCE.apply(
        lambda sequence_: np.array(
            [value_[1] for value_ in enumerate(sequence_) if value_[0] % 2 == 0]
        )
    )
    modified_data["LAT_SEQUENCE"] = modified_data.SEQUENCE.apply(
        lambda sequence_: np.array(
            [value_[1] for value_ in enumerate(sequence_) if value_[0] % 2 != 0]
        )
    )
    return modified_data


def create_fix_length_sequences(data, n_limited) -> pd.DataFrame:
    modified_data = data.copy()
    modified_data["START_SEQUENCE"] = modified_data.SEQUENCE.apply(
        lambda sequence: sequence[0 : 2 * n_limited]
    )
    modified_data["STOP_SEQUENCE"] = modified_data.SEQUENCE.apply(
        lambda sequence: sequence[-2 * n_limited :]
    )
    return modified_data


def calculate_polyline_features(data) -> pd.DataFrame:
    """
    calculates length of POLYLINE as N_COORDINATE_POINTS
    calculates total flight time in seconds and minutes as TOTAL_FLIGHT_TIME_SECONDS and TOTAL_FLIGHT_TIME_MINUTES
    """
    modified_data = data.copy()
    modified_data["N_COORDINATE_POINTS"] = modified_data["POLYLINE"].apply(
        lambda value: len(value)
    )
    # total flight time
    modified_data["TOTAL_FLIGHT_TIME_SECONDS"] = modified_data.apply(
        lambda row: (row.N_COORDINATE_POINTS - 1) * 15, axis=1
    )
    modified_data["TOTAL_FLIGHT_TIME_MINUTES"] = (
        modified_data.TOTAL_FLIGHT_TIME_SECONDS / 60
    )
    return modified_data


def filter_invalid_trips(data, n_points: int) -> pd.DataFrame:
    """
    filters trips with less than 10 coordinate points and takes data sample with longest
    POLYLINE for duplicated TRIP IDs
    """
    modified_data = data.copy()
    modified_data = modified_data[modified_data.N_COORDINATE_POINTS >= n_points]
    vc = modified_data.TRIP_ID.value_counts().reset_index()
    duplicated_ids = vc[vc["count"] > 1].TRIP_ID.unique()
    if len(duplicated_ids) > 0:
        duplicated_data = modified_data[modified_data.TRIP_ID.isin(duplicated_ids)]
        valid_data = modified_data[~modified_data.TRIP_ID.isin(duplicated_ids)]
        filtered_data = duplicated_data.groupby("TRIP_ID").apply(
            lambda data_chunk: data_chunk[
                data_chunk.N_COORDINATE_POINTS == data_chunk.N_COORDINATE_POINTS.max()
            ]
        )
        modified_data = pd.concat([filtered_data, valid_data], axis=0)
    return modified_data


def haversine_distance(lat1, lat2, lon1, lon2) -> float:
    """
    :param lat1: Latitude of first point
    :param lat2: Latitude of second point
    :param lon1: Longitude of first point
    :param lon2: Longitude of second point
    :return: Method returns haversine distance between geo coordinates of two points
    """
    # analog to following source -> https://scikit-
    # learn.org/stable/modules/generated/sklearn.metrics.pairwise.haversine_distances.html

    point_1 = [lon1, lat1]
    point_2 = [lon2, lat2]
    point1_in_radians = [radians(_) for _ in point_1]
    point2_in_radians = [radians(_) for _ in point_2]
    result = haversine_distances([point1_in_radians, point2_in_radians])
    result = result * 6371000 / 1000
    return result[0][1]


def calculate_total_distance(data) -> pd.DataFrame:
    modified_data = data.copy()
    modified_data["START_POINT"] = modified_data["POLYLINE"].apply(
        lambda value: value[0]
    )
    modified_data["DEST_POINT"] = modified_data["POLYLINE"].apply(
        lambda value: value[-1]
    )
    modified_data["TOTAL_DISTANCE_KM"] = modified_data.apply(
        lambda row: haversine_distance(
            lat1=row.START_POINT[1],
            lat2=row.DEST_POINT[1],
            lon1=row.START_POINT[0],
            lon2=row.DEST_POINT[0],
        ),
        axis=1,
    )
    return modified_data
