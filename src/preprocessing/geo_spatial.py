import geojson
import pandas as pd
from sklearn.metrics.pairwise import haversine_distances
from math import radians

def convert_string_to_geojson(value: str) -> list:
    json_string = geojson.loads(value)
    return json_string


def convert_polyline_to_geojson_format(data: pd.DataFrame, name_column: str) -> pd.DataFrame:
    current_df = data.copy()
    current_df[name_column] = current_df[name_column].apply(lambda row_: convert_string_to_geojson(row_))
    return current_df


def calculate_polyline_features(data: pd.DataFrame) -> pd.DataFrame:
    """
    calculates length of polyline as n_coordinate_points
    calculates total flight time in seconds and minutes as total_flight_time_seconds and total_flight_time_minutes
    """
    modified_data = data.copy()
    modified_data["n_coordinate_points"] = modified_data["polyline"].apply(
        lambda value: len(value)
    )
    # total flight time
    modified_data["total_flight_time_seconds"] = modified_data.apply(
        lambda row: (row.n_coordinate_points - 1) * 15, axis=1
    )
    modified_data["total_flight_time_minutes"] = (
        modified_data.total_flight_time_seconds / 60
    )
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
