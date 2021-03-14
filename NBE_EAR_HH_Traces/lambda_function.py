import pandas as pd
import numpy as np
import boto3
import json
from utils import read_pickle_from_s3, write_pickle_to_s3
from config import project_bucket, results_EAR_simulation_s3_pickle_path, \
    results_EAR_summary_by_simulation_s3_pickle_path, results_EAR_summary_mapping_s3_pickle_path, \
    results_EAR_hh_traces_s3_pickle_path, results_stress_test_by_sim_s3_pickle_path, \
    results_stress_test_summary_by_sim_s3_pickle_path, results_stress_test_summary_mapping_s3_pickle_path, \
    results_stress_test_hh_traces_s3_pickle_path, results_EAR_normal_percentiles, results_EAR_PBI_percentiles
from datetime import datetime, timedelta
from io import BytesIO, StringIO
import os

send_email_func_name = os.environ['SendOutputsFunc']


def lambda_handler(event, context):
    key_name = event['Records'][0]['s3']['object']['key']
    print(key_name)
    # e.g. key = 'EAR_statistics/10072/5004/mapping.pickle'
    run_id = int(key_name.split('/')[1])
    job_id = int(key_name.split('/')[2])
    print(run_id, job_id)
    # run_id = event['run_id']
    # job_id = event['job_id']

    mapping_info = read_pickle_from_s3(project_bucket,
                                       results_EAR_summary_mapping_s3_pickle_path.format(run_id, job_id))
    df_hh_traces = pd.DataFrame()
    for elem in mapping_info:
        print(elem)
        if (elem[0] == 'GrandTotal') & (elem[1] <= datetime(2022, 3, 12).date()):
            week_ending = elem[1]
            week_starting = week_ending - timedelta(weeks=1)
            p = elem[2]
            sim_index = int(elem[3])
            df_sim = read_pickle_from_s3(project_bucket,
                                         results_EAR_simulation_s3_pickle_path.format(run_id, job_id, sim_index))
            df_sim['Date'] = \
                df_sim['SettlementDateTime'].apply(lambda row: (row.to_pydatetime() - timedelta(minutes=30)).date())
            df_tmp = df_sim[(df_sim['Date'] > week_starting)
                            & (df_sim['Date'] <= week_ending)][['TradingRegion', 'SettlementDateTime',
                                                                'Swap Hedged Qty (MWh)',
                                                                'Cap Hedged Qty (MWh)', 'Customer Net MWh',
                                                                'Spot Price', 'Total Cost ($)']]
            df_tmp_grandtotal = df_tmp.groupby(['SettlementDateTime']).sum()
            df_tmp_grandtotal.reset_index(inplace=True)
            df_tmp_grandtotal.insert(0, 'TradingRegion', 'GrandTotal')
            df_tmp.reset_index(inplace=True, drop=True)
            df_tmp = df_tmp.append(df_tmp_grandtotal).reset_index(drop=True)
            df_tmp['Swap Hedged Qty (MW)'] = df_tmp['Swap Hedged Qty (MWh)'] * 2
            df_tmp['Cap Hedged Qty (MW)'] = df_tmp['Cap Hedged Qty (MWh)'] * 2
            df_tmp['Customer Net MW'] = df_tmp['Customer Net MWh'] * 2
            df_tmp = df_tmp.drop(columns=['Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)', 'Customer Net MWh'])
            df_tmp['Percentile'] = p
            df_hh_traces = df_hh_traces.append(df_tmp)
        else:
            continue
    df_hh_traces['Spot Run No.'] = run_id
    df_hh_traces['Job No.'] = job_id
    # df_hh_traces.csv('HH_Simulation_Traces_{}.csv'.format(run_id))
    # write_pickle_to_s3(df_hh_traces, project_bucket, results_EAR_hh_traces_s3_pickle_path.format(run_id, job_id))
    csv_buffer = StringIO()
    df_hh_traces.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(project_bucket,
                       results_EAR_hh_traces_s3_pickle_path.format(run_id, job_id, run_id, job_id)).put(
        Body=csv_buffer.getvalue())

    # trigger the lambda to send emails
    client = boto3.client('lambda')

    payload = {"run_id": run_id, "job_id": job_id}
    client.invoke(
        FunctionName=send_email_func_name,
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps(payload),
    )
    print('Lambda function for sending emails has been triggered '
          'with parameters of {}'.format(payload))
