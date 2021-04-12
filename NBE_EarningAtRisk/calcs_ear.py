import time
from datetime import timedelta, date
import calendar
from config import bucket_nbe, deal_capture_input_path, deal_capture_converted_path, \
    spot_price_by_sim_parquet_path, meter_data_simulation_s3_pickle_path, results_EAR_simulation_s3_pickle_path, \
    results_EAR_summary_by_simulation_s3_pickle_path, results_EAR_mth_summary_by_simulation_s3_pickle_path, \
    results_EAR_qtr_summary_by_simulation_s3_pickle_path
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
    df_all = read_pickle_from_s3(bucket_nbe, deal_capture_converted_path.format(job_id, date_input))
    states = sorted(list(set(df_all['TradingRegion'])))  # states = ['NSW1', 'QLD1', 'SA1', 'VIC1']

    # read simulated spot price data
    df_sp = pd.read_parquet(f"s3://{bucket_nbe}/{spot_price_by_sim_parquet_path.format(run_id, sim_index)}")
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
        state_load_all = read_pickle_from_s3(bucket_nbe,
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

    # add transfer price
    # SA: $120/MWh, NSW/VIC/QLD: $100/MWh
    # TODO: transfer prices will change in future months
    df_all['Transfer Price'] = df_all['TradingRegion'].apply(lambda row: 120 if row == 'SA1' else 100)

    # calculate earning at risk and output
    df = calculate_earning_at_risk(df_all)
    df_output = df[['TradingRegion', 'SettlementDateTime', 'Swap Premium', 'Swap Hedged Qty (MWh)',
                    'Swap Weighted Strike Price', 'Cap Premium', 'Cap Hedged Qty (MWh)', 'Cap Weighted Strike Price',
                    'Spot Price', 'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd',
                    'Total Cost (excl GST)', 'Cap Premium Cost', 'Total Cost (Incl Cap)',
                    'Transfer Price', 'Transfer Cost', 'EAR Cost']]
    print('Calculation finished. Uploading... {} SimNo. {}'.format(run_id, sim_index))
    # to keep a copy of half hour resolution raw data.
    write_pickle_to_s3(df_output, bucket_nbe,
                       results_EAR_simulation_s3_pickle_path.format(run_id, job_id, sim_index))

    # TODO:
    # weekly summary
    print('Summarising the data into weekly resolution... {} SimNo. {}'.format(run_id, sim_index))
    df_output['WeekEnding'] = df_output['SettlementDateTime'].apply(get_week_ending)  # get the week ending date
    # sum by region by week ending
    df_summarized = df_output[['TradingRegion', 'WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                               'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'Total Cost (excl GST)',
                               'Cap Premium Cost', 'Total Cost (Incl Cap)',
                               'Transfer Price', 'Transfer Cost', 'EAR Cost']].groupby(
        ['TradingRegion', 'WeekEnding']).sum()
    # all regions' sum by week ending
    df_grandtotal = df_output[['WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                               'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'Total Cost (excl GST)',
                               'Cap Premium Cost', 'Total Cost (Incl Cap)',
                               'Transfer Price', 'Transfer Cost', 'EAR Cost']].groupby(['WeekEnding']).sum()
    df_grandtotal.reset_index(inplace=True)
    df_grandtotal.insert(0, 'TradingRegion', 'GrandTotal')
    df_summarized.reset_index(inplace=True)
    df_summarized = df_summarized.append(df_grandtotal).reset_index(drop=True)
    df_summarized['SimNo'] = sim_index
    print('Uploading weekly summary... {} SimNo. {}'.format(run_id, sim_index))
    write_pickle_to_s3(df_summarized,
                       bucket_nbe,
                       results_EAR_summary_by_simulation_s3_pickle_path.format(run_id, job_id, sim_index))

    # TODO
    # monthly summary
    df_output = read_pickle_from_s3(bucket_nbe, results_EAR_simulation_s3_pickle_path.format(run_id, job_id, sim_index))
    df_output['MonthEnding'] = df_output['SettlementDateTime'].apply(get_month_ending)  # get the month ending date
    # sum by region by week ending
    df_summarized = df_output[['TradingRegion', 'MonthEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                               'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'Total Cost (excl GST)',
                               'Cap Premium Cost', 'Total Cost (Incl Cap)',
                               'Transfer Price', 'Transfer Cost', 'EAR Cost']].groupby(
        ['TradingRegion', 'MonthEnding']).sum()
    # all regions' sum by month ending
    df_grandtotal = df_output[['MonthEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                               'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'Total Cost (excl GST)',
                               'Cap Premium Cost', 'Total Cost (Incl Cap)',
                               'Transfer Price', 'Transfer Cost', 'EAR Cost']].groupby(['MonthEnding']).sum()
    df_grandtotal.reset_index(inplace=True)
    df_grandtotal.insert(0, 'TradingRegion', 'GrandTotal')
    df_summarized.reset_index(inplace=True)
    df_summarized = df_summarized.append(df_grandtotal).reset_index(drop=True)
    df_summarized['SimNo'] = sim_index
    print('Uploading monthly summary... {} SimNo. {}'.format(run_id, sim_index))
    write_pickle_to_s3(df_summarized,
                       bucket_nbe,
                       results_EAR_mth_summary_by_simulation_s3_pickle_path.format(run_id, job_id, sim_index))

    # TODO
    # quarterly summary
    df_output = read_pickle_from_s3(bucket_nbe, results_EAR_simulation_s3_pickle_path.format(run_id, job_id, sim_index))
    df_output['QuarterEnding'] = df_output['SettlementDateTime'].apply(get_quarter_ending)  # get the qtr ending date
    # sum by region by week ending
    df_summarized = df_output[['TradingRegion', 'QuarterEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                               'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'Total Cost (excl GST)',
                               'Cap Premium Cost', 'Total Cost (Incl Cap)',
                               'Transfer Price', 'Transfer Cost', 'EAR Cost']].groupby(
        ['TradingRegion', 'QuarterEnding']).sum()
    # all regions' sum by month ending
    df_grandtotal = df_output[['QuarterEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                               'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'Total Cost (excl GST)',
                               'Cap Premium Cost', 'Total Cost (Incl Cap)',
                               'Transfer Price', 'Transfer Cost', 'EAR Cost']].groupby(['QuarterEnding']).sum()
    df_grandtotal.reset_index(inplace=True)
    df_grandtotal.insert(0, 'TradingRegion', 'GrandTotal')
    df_summarized.reset_index(inplace=True)
    df_summarized = df_summarized.append(df_grandtotal).reset_index(drop=True)
    df_summarized['SimNo'] = sim_index
    print('Uploading quarterly summary... {} SimNo. {}'.format(run_id, sim_index))
    write_pickle_to_s3(df_summarized,
                       bucket_nbe,
                       results_EAR_qtr_summary_by_simulation_s3_pickle_path.format(run_id, job_id, sim_index))

    end_time = time.time()
    print("Processing time {} SimNo. {} : {} seconds.".format(run_id, sim_index, end_time - start_time))


def calculate_earning_at_risk(df):
    """

    :param df:
    :return:
    """
    df['Pool Cost'] = df.apply(lambda row: row['Customer Net MWh'] * row['Spot Price'], axis=1)
    df['Swap Cfd'] = df.apply(
        lambda row: (row['Spot Price'] - row['Swap Weighted Strike Price']) * row['Swap Hedged Qty (MWh)'], axis=1)
    df['Cap Cfd'] = df.apply(
        lambda row: max(row['Spot Price'] - row['Cap Weighted Strike Price'], 0) * row['Cap Hedged Qty (MWh)'], axis=1)
    df['Total Cost (excl GST)'] = df.apply(lambda row: row['Pool Cost'] + row['Swap Cfd'] + row['Cap Cfd'], axis=1)
    df['Cap Premium Cost'] = df.apply(lambda row: row['Cap Premium'] * row['Customer Net MWh'], axis=1)
    df['Total Cost (Incl Cap)'] = df.apply(lambda row: row['Total Cost (excl GST)'] + row['Cap Premium Cost'], axis=1)
    df['Transfer Cost'] = df.apply(lambda row: row['Transfer Price'] * row['Customer Net MWh'], axis=1)
    df['EAR Cost'] = df.apply(lambda row: row['Total Cost (Incl Cap)'] - row['Transfer Cost'], axis=1)
    return df


def get_week_ending(dt):
    d = (dt - timedelta(minutes=30)).to_pydatetime()  # to get hh starting
    # In NEM, a week ends in Saturday (isoweekday is 6)
    delta = 6 if d.isoweekday() == 7 else 6 - d.isoweekday()
    week_ending_date = (d + timedelta(days=delta)).date()
    return week_ending_date


def get_month_ending(dt):
    d = (dt - timedelta(minutes=30)).to_pydatetime()  # to get hh starting
    last_day = calendar.monthrange(d.year, d.month)[1]
    month_ending_date = date(d.year, d.month, last_day)
    return month_ending_date


def get_quarter_ending(dt):
    d = (dt - timedelta(minutes=30)).to_pydatetime()  # to get hh starting
    # In Australia, Q1: Jan, Feb, Mar, Q2: Apr, May, Jun, Q3: Jul, Aug, Sep, Q4: Oct, Nov, Dec
    if dt.month in (1, 2, 3):
        qtr_ending_date = date(d.year, 3, 31)
    elif dt.month in (4, 5, 6):
        qtr_ending_date = date(d.year, 6, 30)
    elif dt.month in (7, 8, 9):
        qtr_ending_date = date(d.year, 9, 30)
    elif dt.month in (10, 11, 12):
        qtr_ending_date = date(d.year, 12, 31)
    return qtr_ending_date


if __name__ == "__main__":
    run_id = 50014
    job_id = 39
    date_input = '2021-03-26'
    start_year = 2021
    start_month = 1
    start_day = 1
    end_year = 2022
    end_month = 3
    end_day = 26
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
