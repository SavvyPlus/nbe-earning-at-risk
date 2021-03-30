import json
import boto3
import os
from preprocess_trade_data import transform_format

# TODO
total_number_simulations = 930
client = boto3.client('lambda')
earning_at_risk_func_name = os.environ['EarningAtRiskFunc']
check_ear_summary_output_func_name = os.environ['CheckEARSummaryOutputFunc']


def lambda_handler(event, context):
    key_name = event['Records'][0]['s3']['object']['key']
    print(key_name)
    # e.g. key = 'deal_capture_input/DealCapture_SpotRun10072_Job34_2021-02-12_2022-02-19.xlsx'
    run_id = int(key_name.split('/')[1].split('.')[0].split('_')[1][7:])
    job_id = int(key_name.split('/')[1].split('.')[0].split('_')[2][3:])
    date_input = key_name.split('/')[1].split('.')[0].split('_')[3]
    filename = key_name.split('/')[1]
    sheet_name = 'Position Output'
    start_year = 2021
    start_month = 1
    start_day = 1
    end_year = int(key_name.split('/')[1].split('.')[0].split('_')[4].split('-')[0])
    end_month = int(key_name.split('/')[1].split('.')[0].split('_')[4].split('-')[1])
    end_day = int(key_name.split('/')[1].split('.')[0].split('_')[4].split('-')[2])

    transform_format(job_id,
                     date_input,
                     filename,
                     sheet_name,
                     start_year, start_month, start_day,
                     end_year, end_month, end_day)
    # trigger the NBE earning at risk calculation lambda
    for sim_index in range(total_number_simulations):
        if sim_index >= 900:
            sim_index = 900 + (sim_index - 900) * 9
        payload = {'run_id': run_id,
                   'sim_index': sim_index,
                   'job_id': job_id,
                   'date_input': date_input,
                   'start_year': 2021,
                   'start_month': 1,
                   'start_day': 1,
                   'end_year': end_year,
                   'end_month': end_month,
                   'end_day': end_day}
        client.invoke(
            FunctionName=earning_at_risk_func_name,
            InvocationType='Event',
            LogType='Tail',
            Payload=json.dumps(payload),
        )

    # trigger the lambda of checking summary output files
    payload = {'run_id': run_id,
               'sim_num': total_number_simulations,
               'job_id': job_id}
    client.invoke(
        FunctionName=check_ear_summary_output_func_name,
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps(payload),
    )
