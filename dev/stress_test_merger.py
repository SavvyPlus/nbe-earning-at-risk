from scipy.optimize import linprog
import numpy as np
import boto3
import pickle
import datetime
import time
import json

try:
    import pandas as pd
except:
    pass
from config import bucket_nbe, results_stress_test_by_sim_s3_pickle_path

s3 = boto3.client('s3')


def write_pickle_to_s3(data, bucket, key):
    pickle_data = pickle.dumps(data)
    s3.put_object(Bucket=bucket, Body=pickle_data, Key=key)


def read_pickle_from_s3(bucket, key):
    res = s3.get_object(Bucket=bucket, Key=key)
    data = pickle.loads(res['Body'].read())
    return data


def calculate_adjusted_earning_at_risk(df):
    df['Adjusted Pool Cost'] = df.apply(lambda row: row['Customer Net MWh'] * row['adjusted price'], axis=1)
    df['Adjusted Swap Cfd'] = df.apply(
        lambda row: (row['adjusted price'] - row['Swap Weighted Strike Price']) * row['Swap Hedged Qty (MWh)'], axis=1)
    df['Adjusted Cap Cfd'] = df.apply(
        lambda row: max(row['adjusted price'] - row['Cap Weighted Strike Price'], 0) * row['Cap Hedged Qty (MWh)'],
        axis=1)
    df['Adjusted Total Cost (excl GST)'] = df.apply(
        lambda row: row['Adjusted Pool Cost'] + row['Adjusted Swap Cfd'] + row['Adjusted Cap Cfd'], axis=1)
    df['Cap Premium Cost'] = df.apply(lambda row: row['Cap Premium'] * row['Customer Net MWh'], axis=1)
    df['Adjusted Total Cost (Incl Cap)'] = df.apply(
        lambda row: row['Adjusted Total Cost (excl GST)'] + row['Cap Premium Cost'], axis=1)
    df['Transfer Cost'] = df.apply(lambda row: row['Transfer Price'] * row['Customer Net MWh'], axis=1)
    df['Adjusted EAR Cost'] = df.apply(lambda row: row['Adjusted Total Cost (Incl Cap)'] + row['Transfer Cost'], axis=1)

    return df


def lambda_handler(event, context):
    # parse lambda parameters
    run_id = event['run_id']
    sim_index = event['sim_index']

    # set constants
    bucket = bucket_nbe
    # output_key = f'projects/NextBusinessEnergy/stress_test_output_by_sim/{run_id}/{sim_index}/{sim_index}.pickle'

    states = ['NSW1', 'QLD1', 'SA1', 'VIC1']
    df_list = []
    for state in states:
        # key = f'projects/NextBusinessEnergy/stress_test_output_by_sim/{run_id}/{sim_index}/{state}.pickle'
        df_list.append(read_pickle_from_s3(bucket,
                                           results_stress_test_by_sim_s3_pickle_path.format(run_id, sim_index, state)))
    df = pd.concat(df_list).reset_index(drop=True)
    df['Transfer Price'] = df['TradingRegion'].apply(lambda row: 120 if row == 'SA1' else 100)
    df_output = calculate_adjusted_earning_at_risk(df)
    write_pickle_to_s3(df_output,
                       bucket,
                       results_stress_test_by_sim_s3_pickle_path.format(run_id, sim_index, sim_index))


if __name__ == "__main__":
    # starttime = time.time()
    # event = {
    #     "run_id": "50014",
    #     "sim_index": 0,
    # }
    # lambda_handler(event, None)
    # endtime = time.time()
    # print('\nTotal time: %.2f seconds.' % (endtime - starttime))

    function_name = 'nbe_stress_test_merger'
    sim_num = 930
    client = boto3.client('lambda')
    for i in range(sim_num):
        if i >= 900:
            i = 900 + (i - 900) * 9
        payload = {
            "run_id": "50014",
            "sim_index": i
        }
        client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            LogType='Tail',
            Payload=json.dumps(payload),
        )
        print(i)
