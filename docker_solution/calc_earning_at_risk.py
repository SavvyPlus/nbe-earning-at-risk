import time
from datetime import timedelta, date
from config import project_bucket, deal_capture_input_path, deal_capture_converted_path, \
    spot_price_by_sim_parquet_path, meter_data_simulation_s3_pickle_path, results_EAR_simulation_s3_pickle_path, \
    results_EAR_summary_by_simulation_s3_pickle_path
from utils import write_pickle_to_s3, read_pickle_from_s3
import pandas as pd


def load_calculate_summarize(run_id, job_id, date_input, sim_index,
                             start_year, start_month, start_day, end_year, end_month, end_day):
    """

    :param run_id:
    :param job_id:
    :param date_input:
    :param sim_index:
    :param start_year:
    :param start_month:
    :param start_day:
    :param end_year:
    :param end_month:
    :param end_day:
    :return:
    """
    start_time = time.time()
    print('Job {} using Spot Run {} starting... SimNo: {}'.format(job_id, run_id, sim_index))
    start_date = date(start_year, start_month, start_day)
    end_date = date(end_year, end_month, end_day)  # excl.

    # read deal position data
    df_all = read_pickle_from_s3(project_bucket, deal_capture_converted_path.format(job_id, date_input))
    states = sorted(list(set(df_all['TradingRegion'])))  # states = ['NSW1', 'QLD1', 'SA1', 'VIC1']

    # read simulated spot price data
    df_sp = pd.read_parquet(f"s3://{project_bucket}/{spot_price_by_sim_parquet_path.format(run_id, sim_index)}")
    print('Spot price from {} SimNo. {} loaded.'.format(run_id, sim_index))
    df_sp['Date'] = df_sp['Half Hour Starting'].apply(lambda x: x.date())
    df_sp = df_sp[(start_date <= df_sp['Date']) & (df_sp['Date'] < end_date)]
    if len(pd.concat([df_sp[state] for state in states])) != len(df_all):
        raise ValueError(
            f"load data size: {len(pd.concat([df_sp[state] for state in states]))}, expected:{len(df_all)}")

    # sort by region to make sure spot price data and customer data are consistent
    df_all = df_all.sort_values(by=['TradingRegion', 'SettlementDateTime']).reset_index(drop=True)
    df_all['Spot Price'] = pd.concat([df_sp[state] for state in states], ignore_index=True)

    # read simulated customer load data
    load_data = {}
    for state in states:
        state_load_all = read_pickle_from_s3(project_bucket,
                                             meter_data_simulation_s3_pickle_path.format(run_id,
                                                                                         sim_index // 9,
                                                                                         'NBE_{}'.format(state[:-1])))
        load_data[state] = pd.concat(
            [state_load_all[key]['GRID_USAGE'] for key in state_load_all if start_date <= key < end_date],
            ignore_index=True)
    print('Customer data from {} SimNo. {} loaded.'.format(run_id, sim_index))
    if len(pd.concat([load_data[state] for state in states])) != len(df_all):
        raise ValueError(
            f"load data size: {len(pd.concat([load_data[state] for state in states]))}, expected:{len(df_all)}")
    df_all['Customer Net MWh'] = pd.concat([load_data[state] for state in states], ignore_index=True)
    df_all['Customer Net MWh'] = df_all['Customer Net MWh'].apply(lambda x: -x)

    # calculate earning at risk and output
    df = calculate_earning_at_risk(df_all)
    df_output = df[['TradingRegion', 'SettlementDateTime', 'Swap Premium', 'Swap Hedged Qty (MWh)',
                    'Swap Weighted Strike Price', 'Cap Premium', 'Cap Hedged Qty (MWh)', 'Cap Weighted Strike Price',
                    'Spot Price', 'Customer Net MWh', 'Pool Cost', 'Swap Cfd',
                    'Cap Cfd', 'EAR Cost', 'Cap Premium Cost', 'Total Cost ($)']]
    print('Calculation finished. Uploading... {} SimNo. {}'.format(run_id, sim_index))
    # to keep a copy of half hour resolution raw data.
    write_pickle_to_s3(df_output, project_bucket,
                       results_EAR_simulation_s3_pickle_path.format(run_id, job_id, sim_index))

    # weekly summary
    print('Summarising the data into weekly resolution... {} SimNo. {}'.format(run_id, sim_index))
    df_output['WeekEnding'] = df_output['SettlementDateTime'].apply(get_week_ending)  # get the week ending date
    # sum by region by week ending
    df_summarized = df_output[['TradingRegion', 'WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                               'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'EAR Cost', 'Cap Premium Cost',
                               'Total Cost ($)']].groupby(['TradingRegion', 'WeekEnding']).sum()
    # all regions' sum by week ending
    df_grandtotal = df_output[['WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                               'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'EAR Cost', 'Cap Premium Cost',
                               'Total Cost ($)']].groupby(['WeekEnding']).sum()
    df_grandtotal.reset_index(inplace=True)
    df_grandtotal.insert(0, 'TradingRegion', 'GrandTotal')
    df_summarized.reset_index(inplace=True)
    df_summarized = df_summarized.append(df_grandtotal).reset_index(drop=True)
    df_summarized['SimNo'] = sim_index
    print('Uploading weekly summary... {} SimNo. {}'.format(run_id, sim_index))
    write_pickle_to_s3(df_summarized,
                       project_bucket,
                       results_EAR_summary_by_simulation_s3_pickle_path.format(run_id, job_id, sim_index))
    end_time = time.time()
    print("Processing time {} SimNo. {} : {} seconds.".format(run_id, sim_index, end_time-start_time))


def calculate_earning_at_risk(df):
    df['Pool Cost'] = df.apply(lambda row: row['Customer Net MWh'] * row['Spot Price'], axis=1)
    df['Swap Cfd'] = df.apply(
        lambda row: (row['Spot Price'] - row['Swap Weighted Strike Price']) * row['Swap Hedged Qty (MWh)'], axis=1)
    df['Cap Cfd'] = df.apply(
        lambda row: max(row['Spot Price'] - row['Cap Weighted Strike Price'], 0) * row['Cap Hedged Qty (MWh)'], axis=1)
    df['EAR Cost'] = df.apply(lambda row: row['Pool Cost'] + row['Swap Cfd'] + row['Cap Cfd'], axis=1)
    df['Cap Premium Cost'] = df.apply(lambda row: row['Cap Premium'] * row['Customer Net MWh'], axis=1)
    df['Total Cost ($)'] = df.apply(lambda row: row['EAR Cost'] + row['Cap Premium Cost'], axis=1)
    return df


def get_week_ending(dt):
    d = (dt - timedelta(minutes=30)).to_pydatetime()  # to get hh starting
    # In NEM, a week ends in Saturday (isoweekday is 6)
    delta = 6 if d.isoweekday() == 7 else 6 - d.isoweekday()
    week_ending_date = (d + timedelta(days=delta)).date()
    return week_ending_date


if __name__ == "__main__":
    run_id = 10072
    job_id = 34
    date_input = '2021-02-12'
    start_year = 2021
    start_month = 1
    start_day = 1
    end_year = 2022
    end_month = 2
    end_day = 15
    sim_index = 1
    starttime = time.time()
    load_calculate_summarize(run_id,
                             job_id,
                             date_input,
                             sim_index,
                             start_year,
                             start_month,
                             start_day,
                             end_year,
                             end_month,
                             end_day)
    endtime = time.time()
    print('\nTotal time: %.2f seconds.' % (endtime - starttime))
