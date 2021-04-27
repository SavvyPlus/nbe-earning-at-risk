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


def list_object_keys(bucket, prefix):
    key_list = []
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    contents = response['Contents']
    is_truncated = response['IsTruncated']

    for content in contents:
        key_list.append(content['Key'])

    if is_truncated:
        cont_token = response['NextContinuationToken']
    while is_truncated:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=cont_token
        )
        contents = response['Contents']
        is_truncated = response['IsTruncated']
        for content in contents:
            key_list.append(content['Key'])
        if is_truncated:
            cont_token = response['NextContinuationToken']

    return sorted(key_list)


if __name__=="__main__":
    # bucket_inputs = '007-spot-price-forecast-physical'
    # states = ['NSW1', 'QLD1', 'SA1', 'VIC1']
    # job_id = 30
    # run_id = 50014
    # all = []
    # for state in states:
    #     key = f"projects/NextBusinessEnergy/post_process/{run_id}/{job_id}/weekly_average_{state}.pickle"
    #     file = read_pickle_from_s3(bucket_inputs, key)
    #     all.append(file)
    # df = pd.concat(all)
    #
    # # df.to_excel(f's3://{bucket_inputs}/projects/NextBusinessEnergy/post_process/10072/{job_id}/weekly_average_all.xlsx', index=False)
    # df.to_excel(f'weekly_average_job_{job_id}.xlsx', index=False)

    bucket_inputs = '007-spot-price-forecast-physical'
    job_id = 39
    run_id = 50014
    all = []
    prefix = f"projects/NextBusinessEnergy/post_process/{run_id}/{job_id}/weekly_average_"
    key_list = list_object_keys(bucket_inputs, prefix)
    for key in key_list:
        file = read_pickle_from_s3(bucket_inputs, key)
        all.append(file)
    df = pd.concat(all)

    # df.to_excel(f's3://{bucket_inputs}/projects/NextBusinessEnergy/post_process/10072/{job_id}/weekly_average_all.xlsx', index=False)
    df.to_csv(f'weekly_average_job_{job_id}.xlsx', index=False)