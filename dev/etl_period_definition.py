import time
import pickle
import boto3
# import math
import pandas as pd
import datetime

s3=boto3.client('s3')
def write_pickle_to_s3(data, bucket, key):
    pickle_data = pickle.dumps(data)
    s3.put_object(Bucket=bucket, Body=pickle_data, Key=key)


def read_pickle_from_s3(bucket, key):
    res = s3.get_object(Bucket=bucket, Key=key)
    data = pickle.loads(res['Body'].read())
    return data


df_pd = pd.read_excel('input/wholesale_profile_perioddefinition.xlsx')
df_pd['Period Ending'] = df_pd['Period Ending'].apply(lambda x: x.to_pydatetime())
df_pd['Date'] = df_pd['Period Ending'].apply(lambda x: (x - datetime.timedelta(minutes=30)).date())

df_id = pd.read_excel('input/profile_id_mapping.xlsx')


bucket = '007-spot-price-forecast-physical'
key = r'projects/NextBusinessEnergy/period_definition.parquet'
key_id = r'projects/NextBusinessEnergy/profile_id_mapping.parquet'

df_pd.to_parquet(f"s3://{bucket}/{key}")
df_id.to_parquet(f"s3://{bucket}/{key_id}")