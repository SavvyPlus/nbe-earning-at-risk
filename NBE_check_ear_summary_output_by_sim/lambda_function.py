import json
import boto3
import time
import os
from config import bucket_nbe, results_EAR_week_summary_by_sim__path

get_percentile_func_name = os.environ['GetPercentileOutputsFunc']


def lambda_handler(event, context):
    run_id = event['run_id']
    job_id = event['job_id']
    sim_num = event['sim_num']
    print(event)

    retry = 10
    interval = 60
    client = boto3.client('lambda')
    s3 = boto3.resource('s3')

    for i in range(retry):
        print(f"Retry {i}:")
        files_path = results_EAR_week_summary_by_sim__path.format(run_id, job_id)  # run_id, job_id

        bucket = s3.Bucket(bucket_nbe)
        key_lst = []
        for object_summary in bucket.objects.filter(Prefix=files_path):
            print(object_summary)
            key_lst.append(object_summary.key)

        if len(key_lst) == int(sim_num):
            payload = {"run_id": run_id,
                       "job_id": job_id,
                       "sim_num": int(sim_num)}
            client.invoke(
                FunctionName=get_percentile_func_name,
                InvocationType='Event',
                LogType='Tail',
                Payload=json.dumps(payload),
            )
            print('Lambda function for percentile outputs has been triggered '
                  'with parameters of {}'.format(payload))
            break
        else:
            print('Outputs are incomplete yet, waiting {} seconds...'.format(interval))
            time.sleep(interval)
