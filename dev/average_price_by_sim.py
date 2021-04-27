import numpy as np
import boto3
import pickle
import datetime
import time
import pandas as pd
import json

s3 = boto3.client('s3')


def write_pickle_to_s3(data, bucket, key):
    pickle_data = pickle.dumps(data)
    s3.put_object(Bucket=bucket, Body=pickle_data, Key=key)


def read_pickle_from_s3(bucket, key):
    res = s3.get_object(Bucket=bucket, Key=key)
    data = pickle.loads(res['Body'].read())
    return data


def remove_profile_region_suffix(profile_name):
    new_profile = profile_name
    if '(' in profile_name:
        end_ind = profile_name.index('(') - 1
        new_profile = profile_name[:end_ind]
    return new_profile


def find_next_period_ending(date):
    while date.weekday() != 5: # Monday is 0 and Sunday is 6, period start = Sun, end = Sat
        date += datetime.timedelta(days=1)
    return date


def lambda_handler(event, context):
    # parse lambda parameters
    run_id = event['run_id']
    job_id = event['job_id']
    prod_mode = event['prod_mode'] if 'prod_mode' in event.keys() else True
    sim_index_list = event['sim_index_list']
    start_date = datetime.date(*event["start_date"])
    end_date = datetime.date(*event["end_date"])

    # set constants
    bucket = '007-spot-price-forecast-physical'
    # mapping_key = f'projects/NextBusinessEnergy/EAR_output_summary_by_sim/{run_id}/{job_id}/mapping.pickle'
    pd_key = r'projects/NextBusinessEnergy/period_definition.parquet'
    profile_mapping_key = r'projects/NextBusinessEnergy/profile_id_mapping.parquet'
    output_key = f'projects/NextBusinessEnergy/post_process/{run_id}/{job_id}/weekly_average_{sim_index_list[0]}-{sim_index_list[-1]}.pickle'
    states = ['NSW1', 'QLD1', 'SA1', 'VIC1']
    target_percentile = [0, 0.05, 0.25, 0.5, 0.75, 0.95, 1]

    target_period = ['AFMA Peak ({state})', 'AFMA Off Peak ({state})', 'Flat ({state})',
                     '7 Day Peak ({state})', 'Super Peak ({state})', ]
    # 'ASX Off Peak ({state})', 'ASX Peak ({state})']  # same as AFMA
    target_period_state = {'NSW1': ['MSolarShapeNSW', 'QSolarShapeNSW'],
                           'QLD1': [],
                           'SA1': [],
                           'VIC1': ['InverseSolarVIC', 'MSolarShapeVIC', 'MSolarShapeWeekendVIC']}

    period_days = 7

    df_pd = pd.read_parquet(f"s3://{bucket}/{pd_key}")
    df_profile_mapping = pd.read_parquet(f"s3://{bucket}/{profile_mapping_key}")
    profile_id = {row['name']: row['id'] for _, row in df_profile_mapping.iterrows()}
    profile_region = {row['name']: row['region_id'] for _, row in df_profile_mapping.iterrows()}

    result_list = []

    for sim_index in sim_index_list:
        # read that simulation's spot price data
        spot_price_key = f"spot_price_by_sim/{run_id}/{int(sim_index)}.parquet"
        df_sp = pd.read_parquet(f"s3://nbe-earning-at-risk-prod/{spot_price_key}")
        df_sp['Half Hour Starting'] = df_sp['Half Hour Starting'].apply(lambda x: x.to_pydatetime())
        df_sp['Date'] = df_sp['Half Hour Starting'].apply(lambda x: x.date())

        period_ending = find_next_period_ending(start_date)
        while period_ending <= end_date:
            period_starting = max(period_ending - datetime.timedelta(days=period_days - 1), start_date)  # incl.

            # filter selected dates
            df_period_sp = df_sp[(period_starting <= df_sp['Date']) & (df_sp['Date'] <= period_ending)].reset_index()
            df_period_pd = df_pd[(period_starting <= df_pd['Date']) & (df_pd['Date'] <= period_ending)].reset_index()

            for state in states:
                prices = np.array(df_period_sp[state])
                cap_payouts = np.array([max(0, price-300) for price in prices])
                # apply period definition mask
                for period in target_period:
                    period_name = period.format(state=state[:-1])
                    mask = np.array(df_period_pd[period_name])
                    weighted_avg = sum(prices * mask) / sum(mask) if sum(mask) != 0 else 0  # divide by 0 error
                    weighted_cap_payouts = sum(cap_payouts * mask) / sum(mask) if sum(mask) != 0 else 0  # divide by 0 error
                    weighted_avg, weighted_cap_payouts = weighted_avg.__round__(4), weighted_cap_payouts.__round__(4)
                    number_of_hours = sum(mask/2).__round__(2)
                    result_list.append([period_ending, state, sim_index, period_name, weighted_avg, weighted_cap_payouts, number_of_hours])
                if target_period_state[state]:
                    for period_name in target_period_state[state]:
                        mask = np.array(df_period_pd[period_name])
                        weighted_avg = sum(prices * mask) / sum(mask) if sum(mask) != 0 else 0  # divide by 0 error
                        weighted_cap_payouts = sum(cap_payouts * mask) / sum(mask) if sum(mask) != 0 else 0  # divide by 0 error
                        weighted_avg, weighted_cap_payouts = weighted_avg.__round__(4), weighted_cap_payouts.__round__(4)
                        number_of_hours = sum(mask/2).__round__(2)
                        result_list.append([period_ending, state, sim_index, period_name, weighted_avg, weighted_cap_payouts, number_of_hours])

            period_ending += datetime.timedelta(days=period_days)

    # store result
    df_output = pd.DataFrame(result_list, columns=['WeekEnding', 'TradingRegion', 'SimNo',
                                                   'Profile', 'Average Spot Price', 'Average Cap Payouts', 'NoOfHour'])
    df_output['ProfileID'] = df_output['Profile'].apply(lambda x: int(profile_id[x]))
    df_output['RegionID'] = df_output['Profile'].apply(lambda x: int(profile_region[x]))
    df_output['Profile'] = df_output['Profile'].apply(remove_profile_region_suffix)
    if prod_mode:
        write_pickle_to_s3(df_output, bucket, output_key)
    else:
        print(df_output)
        print(df_output.iloc[0])


if __name__ == "__main__":
    # starttime = time.time()
    # event = {
    #     "run_id": "50014",
    #     "job_id": 39,
    #     # "target_state": 'VIC1',
    #     "prod_mode": False,
    #     "sim_index_list": list(range(10)),
    #     "start_date": [2021, 1, 1],  # [y, m, d] in int
    #     "end_date": [2022, 3, 26],
    # }
    # lambda_handler(event, None)
    # endtime = time.time()
    # print('\nTotal time: %.2f seconds.' % (endtime - starttime))

    """a handy local invoker"""
    lambda_func_name = 'nbe_weekly_average_price'
    payload = {
        "run_id": "50015",
        "job_id": 41,
        "sim_index_list": list(range(0,300)), #+list(range(900, 1162, 9)),
        "start_date": [2021, 1, 1],  # [y, m, d] in int
        "end_date": [2022, 4, 23],
    }
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName=lambda_func_name,
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps(payload),
    )
    print(response)