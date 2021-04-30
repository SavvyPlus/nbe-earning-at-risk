from scipy.optimize import linprog
import numpy as np
import boto3
import json
import pickle
import datetime
import time

try:
    import pandas as pd
except:
    pass

from config import bucket_nbe, spot_price_by_sim_parquet_path, spot_price_by_sim_csv_path, \
    deal_capture_converted_path, results_stress_test_by_sim_s3_pickle_path, meter_data_simulation_s3_pickle_path

s3 = boto3.client('s3')


def write_pickle_to_s3(data, bucket, key):
    pickle_data = pickle.dumps(data)
    s3.put_object(Bucket=bucket, Body=pickle_data, Key=key)


def read_pickle_from_s3(bucket, key):
    res = s3.get_object(Bucket=bucket, Key=key)
    data = pickle.loads(res['Body'].read())
    return data


# cpt constraint, wrap up the whole period, without Cap cfd
def create_cpt_constraint(sp_list):
    x_len = len(sp_list)
    window_size = 7 * 48
    cpt_limit = 224600
    A_ub = []
    b_ub = []
    if x_len < window_size:
        raise ValueError(f'x len shorter than 7*48 hh, x_len: {x_len}')

    for i in range(0, x_len):
        if i <= x_len - window_size:
            A_ub.append(np.concatenate([np.zeros(i), np.ones(window_size), np.zeros(x_len - window_size - i)]))
            b_ub.append(cpt_limit - sum(sp_list[i:i + window_size]))
        else:
            A_ub.append(
                np.concatenate([np.ones(window_size + i - x_len), np.zeros(x_len - window_size), np.ones(x_len - i)]))
            b_ub.append(cpt_limit - sum(sp_list[:window_size + i - x_len]) - sum(sp_list[i:]))

    return A_ub, b_ub


# cpt constraint, with Cap cfd
def create_cpt_constraint_withcap(sp_list):
    x_len = len(sp_list) * 2
    window_size = 7 * 48 * 2
    cpt_limit = 224600
    A_ub = []
    b_ub = []
    if x_len < window_size:
        raise ValueError(f'x len shorter than 7*48 hh, x_len: {x_len}')

    for i in range(0, x_len, 2 * 48):
        if i <= x_len - window_size:
            A_ub.append(np.concatenate([np.zeros(i), np.ones(window_size), np.zeros(x_len - window_size - i)]))
            b_ub.append(cpt_limit - sum(sp_list[i:i + window_size]))
        else:
            A_ub.append(
                np.concatenate([np.ones(window_size + i - x_len), np.zeros(x_len - window_size), np.ones(x_len - i)]))
            b_ub.append(cpt_limit - sum(sp_list[:window_size + i - x_len]) - sum(sp_list[i:]))
    A_ub, b_ub = np.array(A_ub), np.array(b_ub)
    return A_ub, b_ub


def post_cpt_check(res, A_ub, b_ub):
    res = np.array(res)
    bo = A_ub @ res > b_ub
    if any(bo):  # if any item in c is True (breach the cpt)
        print(f"CPT check failed")
        A_ub_new = [A_ub[i] for i in range(len(bo)) if bo[i]]
        b_ub_new = [b_ub[i] for i in range(len(bo)) if bo[i]]
        # print(np.where(bo==True))
        return A_ub_new, b_ub_new
    else:
        print(f"CPT check passed")
        return [], []


def adjust_spot_price(df):
    x_len = len(df)
    price_floor = -1000
    price_ceiling = 15000
    hedging_price = 15
    # need double check
    price_cap = 300

    # solve without cap
    # x_bounds = [(0, price_ceiling - spot_price) for spot_price in df['Spot Price']]
    # c = np.array([row['Customer Net MWh'] + row['Swap Hedged Qty (MWh)'] for _, row in df.iterrows()])
    # A_ub, b_ub = create_cpt_constraint([sp for sp in df['Spot Price']])
    # res = linprog(c, bounds=x_bounds)

    # solve with cap
    x_bounds = [i for t in
                [((0, max(price_cap - spot_price, 0)), (0, price_ceiling - price_cap - spot_price)) for spot_price in
                 df['Spot Price']] for i in t]
    c = [i for t in [(row['Customer Net MWh'] + row['Swap Hedged Qty (MWh)'],
                      row['Customer Net MWh'] + row['Swap Hedged Qty (MWh)'] + row['Cap Hedged Qty (MWh)']) for _, row
                     in df.iterrows()] for i in t]
    A_ub, b_ub = create_cpt_constraint_withcap([sp for sp in df['Spot Price']])
    res = linprog(c, bounds=x_bounds)

    A_ub, b_ub = post_cpt_check(res.x, A_ub, b_ub)
    if A_ub:
        res = linprog(c, A_ub=A_ub, b_ub=b_ub, bounds=x_bounds)
        post_cpt_check(res.x, A_ub, b_ub)
        # print(res.x)
    adjustment = res.x
    adjustment = [sum(adjustment[i:i + 2]).__round__(4) for i in range(0, len(adjustment), 2)]
    adjustment = [x if x > 1 else 0 for x in adjustment]

    return adjustment


def find_next_index(df, current_index, weeks=4):
    current_state = df['TradingRegion'][current_index]
    current_date = df['SettlementDate'][current_index]
    current_day = current_date.isoweekday()  # Monday is 1 and Sunday is 7
    days_delta = [n for n in range(weeks * 7 - 6, weeks * 7 + 1) if (current_day + n) % 7 == 6][0]
    next_date = current_date + datetime.timedelta(days=days_delta + 1)

    next_index = current_index
    while next_index < len(df):
        if df['SettlementDate'][next_index] == next_date or df['TradingRegion'][next_index] != current_state:
            break
        else:
            next_index += 1
    return next_index


def calculate_adjusted_price(df, adjustment):
    df['adjustment'] = adjustment
    df['adjusted price'] = df.apply(lambda row: row['Spot Price'] + row['adjustment'], axis=1)
    return df


def lambda_handler(event, context):
    # parse lambda parameters
    run_id = event['run_id']
    sim_index = event['sim_index']
    target_state = event['target_state']
    prod_mode = event['prod_mode'] if 'prod_mode' in event.keys() else True
    job_id = event['job_id']
    date_input = event['date_input']

    # set constants
    bucket = bucket_nbe
    # spot_price_uri = f"s3://{bucket}/projects/NextBusinessEnergy/spot_price_by_sim/{sim_index}.csv"
    # spot_price_key = f"projects/NextBusinessEnergy/spot_price_by_sim/{sim_index}.csv"
    # deal_capture_key = r'projects/NextBusinessEnergy/deal_capture2021.pickle'
    spot_price_key = spot_price_by_sim_csv_path.format(run_id, sim_index)
    deal_capture_key = deal_capture_converted_path.format(job_id, date_input)
    output_key = results_stress_test_by_sim_s3_pickle_path.format(run_id, sim_index, target_state)
    # output_key = f'projects/NextBusinessEnergy/stress_test_output_by_sim/{run_id}/{sim_index}/{target_state}.pickle'

    start_date = datetime.date(2021, 1, 1)
    end_date = datetime.date(2022, 4, 17)  # incl.

    # read deal position data
    df_all = read_pickle_from_s3(bucket, deal_capture_key)
    # df_all['SettlementDate'] = df_all['SettlementDate'].apply(lambda x: x.to_pydatetime().date())
    # df_all['SettlementDateTime'] = df_all['SettlementDateTime'].apply(lambda x: x.to_pydatetime())

    # filter target state
    df_all = df_all[df_all['TradingRegion'] == target_state].reset_index(drop=True)

    # read simulated spot price data
    obj = s3.get_object(Bucket=bucket, Key=spot_price_key)
    df_sp = pd.read_csv(obj['Body'])
    df_all['Spot Price'] = df_sp[target_state]

    # filter the date range
    df_all = df_all[(df_all['SettlementDate'] >= start_date) & (df_all['SettlementDate'] < end_date)]

    # read simulated customer load data
    df_load = read_pickle_from_s3(bucket,
                                  meter_data_simulation_s3_pickle_path.format(run_id, sim_index // 9,
                                                                              'NBE_{}'.format(target_state[:-1])))
    load_data = [df_load[key]['GRID_USAGE'] for key in df_load if start_date <= key < end_date]
    df_all['Customer Net MWh'] = pd.concat(load_data, ignore_index=True)
    df_all['Customer Net MWh'] = df_all['Customer Net MWh'].apply(lambda x: -x)

    # solve price adjustment
    adjustment_all = []
    current_index = next_index = 0
    while next_index < len(df_all):
        next_index = find_next_index(df_all, current_index)
        print(current_index, next_index)
        df_part = df_all.iloc[current_index:next_index]
        adjustment_all += adjust_spot_price(df_part)
        current_index = next_index

    # update result columns and output
    df = calculate_adjusted_price(df_all, adjustment_all)
    df_output = df[['TradingRegion', 'SettlementDateTime', 'Swap Premium', 'Swap Hedged Qty (MWh)',
                    'Swap Weighted Strike Price', 'Cap Premium', 'Cap Hedged Qty (MWh)', 'Cap Weighted Strike Price',
                    'Spot Price', 'Customer Net MWh', 'adjustment', 'adjusted price']]
    if prod_mode:
        write_pickle_to_s3(df_output, bucket, output_key)
    else:
        df_output.to_excel(f'{target_state}.xlsx')


if __name__ == "__main__":
    # starttime = time.time()
    # event = {
    #     "run_id": "50015",
    #     "sim_index": 0,
    #     "target_state": 'VIC1',
    #     "prod_mode": True,
    #     "job_id": 41,
    #     "date_input": '2021-04-23'
    # }
    # lambda_handler(event, None)
    # endtime = time.time()
    # print('\nTotal time: %.2f seconds.' % (endtime - starttime))

    function_name = 'NBE_stress_test'
    sim_num = 900
    client = boto3.client('lambda')
    for i in range(sim_num):
        if i >= 900:
            i = 900 + (i - 900) * 9
        payload = {
            "run_id": "50015",
            "sim_index": i,
            "target_state": 'VIC1',
            "prod_mode": True,
            "job_id": 41,
            "date_input": '2021-04-23'
        }
        client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            LogType='Tail',
            Payload=json.dumps(payload),
        )
        print(i)
