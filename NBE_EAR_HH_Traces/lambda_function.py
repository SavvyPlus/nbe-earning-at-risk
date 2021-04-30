import boto3
import json
from calc_hh_traces import get_hh_traces
import os

send_email_func_name = os.environ['SendOutputsFunc']


def lambda_handler(event, context):
    key_name = event['Records'][0]['s3']['object']['key']
    print(key_name)
    # e.g. key = 'EAR_statistics/10072/5004/mapping.pickle'
    run_id = int(key_name.split('/')[1])
    job_id = int(key_name.split('/')[2])
    print(run_id, job_id)
    get_hh_traces(run_id, job_id)

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
