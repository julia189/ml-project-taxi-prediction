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

def print_sample_size(data, add_text=''):
    print(f'{add_text} Size data samples {data.shape[0]}')
    
def adjust_datatypes(data, skip_polyline=False, skip_timestamp=False):
    """
    adjust to appropriate datatypes
    """
    data.TRIP_ID = data.TRIP_ID.astype(object)
    data.CALL_TYPE = data.CALL_TYPE.astype(object)
    data.ORIGIN_STAND = data.ORIGIN_STAND.astype(object)
    data.ORIGIN_CALL = data.ORIGIN_CALL.astype(object)
    data.TAXI_ID = data.TAXI_ID.astype(object)
    
    if not skip_timestamp:
        data['TIMESTAMP_DT'] = data['TIMESTAMP'].apply(lambda value_unix: 
                                                    datetime.fromtimestamp(value_unix).strftime('%Y-%m-%d %H:%M:%S'))
        data.TIMESTAMP_DT = pd.to_datetime(data.TIMESTAMP_DT)
    if not skip_polyline:
        data['POLYLINE'] = data['POLYLINE'].apply(lambda value: json.loads(value))
    return data

def extend_timestamps(data):
    data['TIMESTAMP_MONTH'] = pd.to_datetime(data['TIMESTAMP_DT']).dt.month
    data['TIMESTAMP_YEAR'] = pd.to_datetime(data['TIMESTAMP_DT']).dt.year
    data['YEAR_MONTH'] = data['TIMESTAMP_YEAR'].astype(str) + '_' + data['TIMESTAMP_MONTH'].astype(str)
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

def reduce_high_cardinality(data, columns=[]):
    for column_ in columns:
        vc = data[str(column_)].value_counts()
        median_freq = vc.median()
        majority_freq = vc[vc >= median_freq].index
        data[str(column_)+ '_agg'] = data.apply(lambda row: row[str(column_)] if row[str(column_)] in majority_freq else 'OTHER', axis=1)
        data[str(column_)+ '_agg'] = data[str(column_)+ '_agg'].astype(str)
    return data

def feature_encoding_oh(data, categories_oh):
    from sklearn.preprocessing import OneHotEncoder
    data_encoded = pd.DataFrame()
    for attribute_ in categories_oh:
        enc = OneHotEncoder()
        fenc = enc.fit_transform(X=data[str(attribute_)].values.reshape(-1,1)).toarray()
        df_fenc = pd.DataFrame(fenc, columns=enc.categories_)
        data_encoded = pd.concat([df_fenc, data_encoded], axis=1)
    return data_encoded
        

def feature_encoding_oe(data, categories_oe):
    from sklearn.preprocessing import OrdinalEncoder
    data_encoded = pd.DataFrame()
    for attribute_ in categories_oe:
        enc = OrdinalEncoder()
        fenc = enc.fit_transform(X=data[str(attribute_)].values.reshape(-1,1))
        df_fenc = pd.DataFrame(fenc, columns=[str(attribute_)+'_OE'])
        data_encoded = pd.concat([df_fenc, data_encoded], axis=1)
    return data_encoded

def add_binary_features(train_data, test_data):
    missing_features_test = train_data.columns.difference(test_data.columns)
    missing_features_train = test_data.columns.difference(train_data.columns)
    zero_data_test = np.zeros(shape=(test_data.shape[0],len(missing_features_test)))
    dummy_data_test = pd.DataFrame(zero_data_test, columns=missing_features_test)
    zero_data_train = np.zeros(shape=(train_data.shape[0],len(missing_features_train))) 
    dummy_data_train = pd.DataFrame(zero_data_train, columns=missing_features_train)
    
    return pd.concat([test_data, dummy_data_test],axis=1), pd.concat([train_data, dummy_data_train],axis=1)
