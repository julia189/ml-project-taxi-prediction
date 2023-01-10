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
import boto3
import sagemaker

def split_lat_lon(data):
    data['LON_ARRAY'] = data.SEQUENCE.apply(lambda sequence_:
                                            np.array([value_[1] for value_ in enumerate(sequence_) 
                                                      if value_[0]%2 == 0]))
    data['LAT_ARRA'] = data.SEQUENCE.apply(lambda sequence_:
                                            np.array([value_[1] for value_ in enumerate(sequence_) 
                                                      if value_[0]%2 != 0]))
    return data
def create_fix_length_sequences(data, n_limited):
    data['START_SEQUENCE'] = data.SEQUENCE.apply(lambda sequence: sequence[0:2*n_limited])
    data['STOP_SEQUENCE'] = data.SEQUENCE.apply(lambda sequence: sequence[-2*n_limited:])
    return data
    
def calculate_POLYLINE_features(data):
    """
    calculates length of POLYLINE as N_COORDINATE_POINTS
    calculates total flight time in seconds and minutes as TOTAL_FLIGHT_TIME_SECONDS and TOTAL_FLIGHT_TIME_MINUTES
    """
    data['N_COORDINATE_POINTS'] = data['POLYLINE'].apply(lambda value: len(value))
    #total flight time
    data['TOTAL_FLIGHT_TIME_SECONDS'] = data.apply(lambda row: (row.N_COORDINATE_POINTS-1)*15, axis=1)
    data['TOTAL_FLIGHT_TIME_MINUTES'] = data.TOTAL_FLIGHT_TIME_SECONDS / 60
    return data

def filter_invalid_trips(data):
    """
    filters trips with less than 10 coordinate points and takes data sample with longest POLYLINE for duplicated TRIP IDs
    """
    data = data[data.N_COORDINATE_POINTS >= 10]
    vc = data.TRIP_ID.value_counts().reset_index()
    DUPLICATES_IDs = vc[vc.TRIP_ID > 1]['index'].unique()
    if len(DUPLICATES_IDs) > 0:
        data_duplicated = data[data.TRIP_ID.isin(DUPLICATES_IDs)]
        data_valid = data[~data.TRIP_ID.isin(DUPLICATES_IDs)]
        data_filtered = data_duplicated.groupby('TRIP_ID').apply(lambda datachunk: datachunk[datachunk.N_COORDINATE_POINTS == datachunk.N_COORDINATE_POINTS.max()])
        data = pd.concat([data_filtered,data_valid],axis=0)
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

def calculate_total_distance(data):
    data['START_POINT'] = data['POLYLINE'].apply(lambda value: value[0])
    data['DEST_POINT'] = data['POLYLINE'].apply(lambda value: value[-1])   
    data['TOTAL_DISTANCE_KM'] = data.apply(lambda row:
                                       haversine_distance(lat1=row.START_POINT[1],
                                                lat2=row.DEST_POINT[1],
                                                lon1=row.START_POINT[0],
                                                lon2=row.DEST_POINT[0])
                                        ,axis=1
                                       )
    return data
    