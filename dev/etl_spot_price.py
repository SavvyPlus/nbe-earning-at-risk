import pandas as pd
import boto3
import time

client = boto3.client('s3')


def list_object_key(bucket, prefix):
    key_list = []
    response = client.list_objects_v2(
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
        response = client.list_objects_v2(
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


def lambda_handler(event, context):
    run_id = event['run_id']
    sim_index = event['sim_index'] // 9
    esc_index = event['sim_index'] % 9
    states = sorted(['VIC1', 'NSW1', 'QLD1', 'SA1'])

    bucket_input = 'spot-price-forecast-simulation-prod'
    bucket_output = 'nbe-earning-at-risk-prod'
    spot_price_df_list = []
    for year in range(2021, 2023):  # 2021-2022
        for month in range(1, 13):  # 1-12
            key = f'spot-escalation-totaldemand/{run_id}/sim={sim_index}/escalation={esc_index}/year={year}/month={month}/{year}-{month}.parquet'
            df = pd.read_parquet(f's3://{bucket_input}/{key}')
            spot_price_df_list.append(df[['Half Hour Starting'] + states])
            if year == 2022 and month == 12:  # only to 2022.12 (incl.)
                break
    df = pd.concat(spot_price_df_list, ignore_index=True)
    print(df)
    df.to_parquet(f"s3://{bucket_output}/spot_price_by_sim/{run_id}/{event['sim_index']}.parquet", index=False)
    # df.to_csv(f"s3://{bucket_output}/spot_price_by_sim/{run_id}/{event['sim_index']}.csv", index=False)


if __name__ == "__main__":
    # starttime = time.time()
    # event = {
    #     "run_id": "50014",
    #     "sim_index": 0,
    # }
    # lambda_handler(event, None)
    # endtime = time.time()
    # print('\nTotal time: %.2f seconds.' % (endtime - starttime))

    import boto3
    import json

    client = boto3.client('lambda')
    sim_no = 900
    for sim_index in range(sim_no):
        if sim_index >= 900:
            sim_index = 900 + (sim_index - 900) * 9
        payload = {'run_id': '50015',
                   'sim_index': sim_index}
        client.invoke(
            FunctionName='nbe_merge_spot_temp',
            InvocationType='Event',
            LogType='Tail',
            Payload=json.dumps(payload),
        )
        print(sim_index)
