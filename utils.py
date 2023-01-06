import os
import pandas as pd
import numpy as np
import datetime
from datetime import datetime
import time
import json
import matplotlib.pyplot as plt
import seaborn as sns
import sklearn


def adjust_datatypes(data):
    """
    adjust to appropriate datatypes
    """
    data.TRIP_ID = data.TRIP_ID.astype(object)
    data.CALL_TYPE = data.CALL_TYPE.astype(object)
    data.ORIGIN_STAND = data.ORIGIN_STAND.astype(object)
    data.ORIGIN_CALL = data.ORIGIN_CALL.astype(object)
    data.TAXI_ID = data.TAXI_ID.astype(object)
    data['TIMESTAMP_DT'] = data['TIMESTAMP'].apply(lambda value_unix: 
                                                   datetime.fromtimestamp(value_unix).strftime('%Y-%m-%d %H:%M:%S'))
    data.TIMESTAMP_DT = pd.to_datetime(data.TIMESTAMP_DT)
    data['POLYLINE_LIST'] = data['POLYLINE'].apply(lambda value: json.loads(value))
    return data


def haversine_distance(lat1, lat2, lon1, lon2):
    #analog to following source -> https://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.haversine_distances.html
    from sklearn.metrics.pairwise import haversine_distances
    from math import radians
    point_A = [lon1, lat1] 
    point_B = [lon2, lat2]
    pointA_in_radians = [radians(_) for _ in point_A]
    pointB_in_radians = [radians(_) for _ in point_B]
    result = haversine_distances([pointA_in_radians, pointB_in_radians])
    result = result * 6371000/1000  
    return result[0][1]