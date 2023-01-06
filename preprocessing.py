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

def create_timeseries(data):
    data = data.explode('POLYLINE')
    data['LONGITUDE'] = data['POLYLINE'].str[0]
    data['LATITUDE'] = data['POLYLINE'].str[1]
    df_longitude = data.groupby('TRIP_ID').apply(lambda datachunk: pd.DataFrame(datachunk['LONGITUDE'].values.reshape(1,-1)))\
                                          .reset_index().drop(['level_1'],axis=1)
    df_latitude = data.groupby('TRIP_ID').apply(lambda datachunk: pd.DataFrame(datachunk['LATITUDE'].values.reshape(1,-1)))\
                                          .reset_index().drop(['level_1'],axis=1)
    df_longitude.columns = 'lon_' + df_longitude.columns.astype(str)
    df_longitude = df_longitude.rename(columns={'lon_TRIP_ID':'TRIP_ID'})
    df_latitude.columns = 'lat_' + df_latitude.columns.astype(str)
    df_latitude = df_latitude.rename(columns={'lat_TRIP_ID':'TRIP_ID'})
    data = pd.merge(df_longitude, 
         data[['TRIP_ID','TOTAL_FLIGHT_TIME_MINUTES', 'N_COORDINATE_POINTS']], how='left',on='TRIP_ID')
    data = pd.merge(data, df_latitude, how='left', on='TRIP_ID')
    del df_longitude, df_latitude
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
    filters trips with less than 5 coordinate points and takes data sample with longest POLYLINE for duplicated TRIP IDs
    """
    data = data[data.N_COORDINATE_POINTS >= 5]
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
    data['START_POINT'] = data['POLYLINE_LIST'].apply(lambda value: value[0])
    data['DEST_POINT'] = data['POLYLINE_LIST'].apply(lambda value: value[-1])   
    data['TOTAL_DISTANCE_KM'] = data.apply(lambda row:
                                       haversine_distance(lat1=row.START_POINT[1],
                                                lat2=row.DEST_POINT[1],
                                                lon1=row.START_POINT[0],
                                                lon2=row.DEST_POINT[0])
                                        ,axis=1
                                       )
    return data


def create_timeseries_loop(data):
    from tqdm import tqdm
    df_longitude = pd.DataFrame()
    df_latitude = pd.DataFrame()

    
    data.assign(POLYLINE_adjst=pd.DataFrame(json.loads(data.POLYLINE)).T)
    
    for index, row in tqdm(data.iterrows()):
        next_row = pd.DataFrame(json.loads(row.POLYLINE)).T
        df_latitude = pd.concat([df_latitude, next_row.iloc[1,:]],axis=1,ignore_index=True)
        df_longitude = pd.concat([df_longitude, next_row.iloc[0,:]],axis=1,ignore_index=True)
        
    df_latitude = df_latitude.T
    df_longitude = df_longitude.T
    df_latitude.index = data.TRIP_ID
    df_longitude.index = data.TRIP_ID
    lat_columns = [f'lat_{n}' for n in df_latitude.columns]
    lon_columns = [f'lon_{n}' for n in df_longitude.columns]
    df_latitude.columns = lat_columns
    df_longitude.columns = lon_columns
    return df_latitude, df_longitude

def preprocess_data():
    train_data = pd.read_parquet('s3://think-tank-casestudy/raw_data/train_data.parquet')
    test_data = pd.read_parquet('s3://think-tank-casestudy/raw_data/test_data.parquet')
    
    train_data = train_data[['TRIP_ID','POLYLINE']]
    test_data = test_data[['TRIP_ID','POLYLINE']]
    
    train_data['POLYLINE'] = train_data['POLYLINE'].apply(lambda value: json.loads(value))
    test_data['POLYLINE'] = test_data['POLYLINE'].apply(lambda value: json.loads(value))
    
    train_data = calculate_POLYLINE_features(train_data)
    test_data = calculate_POLYLINE_features(test_data)
    
    train_data = filter_invalid_trips(train_data)
    test_data = filter_invalid_trips(test_data)
    
    train_data = train_data.reset_index(drop=True)
    test_data = test_data.reset_index(drop=True)
    
    test_data = create_timeseries(test_data)
    train_data = create_timeseries(train_data)
    
    test_data.to_parquet('s3://think-tank-casestudy/raw_data/time_series_test_data.parquet')
    train_data.to_parquet('s3://think-tank-casestudy/raw_data/time_series_train_data.parquet')
    
    return train_data, test_data

if __name__ == "__main__":
    preprocess_data()
    