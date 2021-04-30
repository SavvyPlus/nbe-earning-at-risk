import numpy as np
import boto3
import pickle
import datetime
import time
import pandas as pd
import json
from utils import write_pickle_to_s3, read_pickle_from_s3
from config import bucket_nbe, period_definition_path, profile_mapping_path, spot_price_by_sim_parquet_path, \
    results_avg_price_by_profile_by_sim_path

s3 = boto3.client('s3')

period_resolutions = ['week', 'month', 'quarter']
states = ['NSW1', 'QLD1', 'SA1', 'VIC1']
target_profile = ['AFMA Peak ({state})', 'AFMA Off Peak ({state})', 'Flat ({state})',
                 '7 Day Peak ({state})', 'Super Peak ({state})', ]
# 'ASX Off Peak ({state})', 'ASX Peak ({state})']  # same as AFMA
target_profile_state = {'NSW1': ['MSolarShapeNSW', 'QSolarShapeNSW'],
                        'QLD1': [],
                        'SA1': [],
                        'VIC1': ['InverseSolarVIC', 'MSolarShapeVIC', 'MSolarShapeWeekendVIC']}


def find_next_period_ending(date, by):
    if by not in ('week', 'month', 'quarter'):
        raise ValueError(f"Expect one of ('week', 'month', 'quarter'), got {by} instead.")
    if by == 'week':
        while date.weekday() != 5:  # Monday is 0 and Sunday is 6, period start = Sun, end = Sat
            date += datetime.timedelta(days=1)
    elif by == 'month':
        while (date + datetime.timedelta(days=1)).day != 1:
            date += datetime.timedelta(days=1)
    elif by == 'quarter':
        end_of_quarter_dates = ('03-31', '06-30', '09-30', '12-31')
        while date.strftime('%m-%d') not in end_of_quarter_dates:
            date += datetime.timedelta(days=1)

    return date


def avg_price_calcs(period_resolution, period_starting, period_ending, end_date, df_sp, df_pd,
                    profile_id, profile_region_id, sim_index):
    """

    :param period_resolution:
    :param period_starting:
    :param period_ending:
    :param end_date:
    :param df_sp:
    :param df_pd:
    :param profile_id:
    :param profile_region_id:
    :param sim_index:
    :return: 'WeekEnding', 'TradingRegion', 'SimNo', 'Profile', 'Average Spot Price', 'Average Cap Payouts',
    'NoOfHour', 'ProfileID', 'RegionID'
    """
    result_list = []
    while period_ending <= end_date:
        # filter selected dates
        df_period_sp = df_sp[(period_starting <= df_sp['Date']) & (df_sp['Date'] <= period_ending)].reset_index()
        df_period_pd = df_pd[(period_starting <= df_pd['Date']) & (df_pd['Date'] <= period_ending)].reset_index()

        for state in states:
            prices = np.array(df_period_sp[state])
            cap_payouts = np.array([max(0, price - 300) for price in prices])
            # apply period definition mask
            for profile in target_profile:
                profile_name = profile.format(state=state[:-1])
                mask = np.array(df_period_pd[profile_name])
                weighted_avg = sum(prices * mask) / sum(mask) if sum(mask) != 0 else 0  # divide by 0 error
                weighted_cap_payouts = sum(cap_payouts * mask) / sum(mask) if sum(
                    mask) != 0 else 0  # divide by 0 error
                weighted_avg, weighted_cap_payouts = weighted_avg.__round__(4), weighted_cap_payouts.__round__(4)
                number_of_hours = sum(mask / 2).__round__(2)
                result_list.append(
                    [period_ending, state, sim_index, profile_name, weighted_avg, weighted_cap_payouts,
                     number_of_hours, profile_id[profile_name], profile_region_id[profile_name]])
            if target_profile_state[state]:
                for profile_name in target_profile_state[state]:
                    mask = np.array(df_period_pd[profile_name])
                    weighted_avg = sum(prices * mask) / sum(mask) if sum(mask) != 0 else 0  # divide by 0 error
                    weighted_cap_payouts = sum(cap_payouts * mask) / sum(mask) if sum(
                        mask) != 0 else 0  # divide by 0 error
                    weighted_avg, weighted_cap_payouts = weighted_avg.__round__(4), weighted_cap_payouts.__round__(
                        4)
                    number_of_hours = sum(mask / 2).__round__(2)
                    result_list.append(
                        [period_ending, state, sim_index, profile_name, weighted_avg, weighted_cap_payouts,
                         number_of_hours, profile_id[profile_name], profile_region_id[profile_name]])

        period_starting = period_ending + datetime.timedelta(days=1)
        period_ending = find_next_period_ending(period_starting, by=period_resolution)
    return result_list


def main(params):
    # parse parameters
    run_id = params['run_id']
    job_id = params['job_id']
    sim_index = params['sim_index']
    start_date = datetime.date(*params["start_date"])
    end_date = datetime.date(*params["end_date"])

    # period_days = 7
    df_pd = pd.read_parquet('s3://{}/{}'.format(bucket_nbe, period_definition_path))
    df_profile_mapping = pd.read_parquet('s3://{}/{}'.format(bucket_nbe, profile_mapping_path))
    profile_id = {row['name']: row['id'] for _, row in df_profile_mapping.iterrows()}
    profile_region_id = {row['name']: row['region_id'] for _, row in df_profile_mapping.iterrows()}

    # read spot price data
    df_sp = pd.read_parquet('s3://{}/{}'.format(bucket_nbe,
                                                spot_price_by_sim_parquet_path.format(run_id, sim_index)))
    df_sp['Half Hour Starting'] = df_sp['Half Hour Starting'].apply(lambda x: x.to_pydatetime())
    df_sp['Date'] = df_sp['Half Hour Starting'].apply(lambda x: x.date())
    for period_resolution in period_resolutions:
        period_starting = start_date
        period_ending = find_next_period_ending(start_date, by=period_resolution)  # incl.
        result = avg_price_calcs(period_resolution,
                                 period_starting,
                                 period_ending,
                                 end_date, df_sp, df_pd, profile_id, profile_region_id, sim_index)
        write_pickle_to_s3(result,
                           bucket_nbe,
                           results_avg_price_by_profile_by_sim_path.format(period_resolution,
                                                                           job_id, run_id, sim_index))


if __name__ == "__main__":
    run_id = 50015
    job_id = 41
    starttime = time.time()
    event = {
        "run_id": run_id,
        "job_id": job_id,
        "sim_index": 2,
        "start_date": [2021, 1, 1],  # [y, m, d] in int
        "end_date": [2022, 4, 23],
    }
    main(event)
    endtime = time.time()
    print('\nTotal time: %.2f seconds.' % (endtime - starttime))
