import geojson
import pandas as pd


def convert_string_to_geojson(value: str) -> list:
    json_string = geojson.loads(value)
    return json_string


def convert_polyline_to_geojson_format(data: pd.DataFrame, name_column: str) -> pd.DataFrame:
    current_df = data.copy()
    current_df[name_column] = current_df[name_column].apply(lambda row_: convert_string_to_geojson(row_), axis=0)
    return current_df
