from math import radians

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import haversine_distances
import geojson


def convert_string_to_geojson(value: str) -> list:
    json_string = geojson.loads(value)
    return json_string


def convert_polyline_to_geojson_format(data: pd.DataFrame, name_column: str) -> pd.DataFrame:
    current_df = data.copy()
    current_df[name_column] = current_df[name_column].apply(lambda row_: convert_string_to_geojson(row_), axis=0)
    return current_df

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


def filter_invalid_trips(data: pd.DataFrame, n_points: int) -> pd.DataFrame:
    """
    filters trips with less than n_points coordinate points and takes data sample with longest
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



