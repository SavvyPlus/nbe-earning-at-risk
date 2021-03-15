import pandas as pd
import numpy as np
import boto3
import json
from utils import read_pickle_from_s3, write_pickle_to_s3
from config import bucket_nbe, results_EAR_simulation_s3_pickle_path, \
    results_EAR_summary_by_simulation_s3_pickle_path, results_EAR_summary_mapping_s3_pickle_path, \
    results_EAR_hh_traces_s3_pickle_path
from datetime import datetime, timedelta
from io import BytesIO, StringIO


def get_output(run_id, job_id, sim_num):
    print("loading data of all simulations.")
    df_all_sim = pd.DataFrame()
    for i in range(sim_num):
        if i < 900:
            df_tmp = read_pickle_from_s3(bucket_nbe,
                                         results_EAR_summary_by_simulation_s3_pickle_path.format(run_id, job_id, i))
        else:
            df_tmp = read_pickle_from_s3(bucket_nbe,
                                         results_EAR_summary_by_simulation_s3_pickle_path.format(run_id, job_id,
                                                                                                 900 + (i - 900) * 9))
        df_all_sim = df_all_sim.append(df_tmp)
        print(i)
    # # output by simulation
    # df_all_sim['Spot Run No.'] = run_id
    # df_all_sim.to_excel('NBE_EAR_Output_by_simulations_{}.xlsx'.format(run_id))

    # save the mapping of percentile & sim_index of all percentiles
    percentile_list = [0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.25, 0.5, 0.75, 0.95, 1]
    df_percentile = df_all_sim[['TradingRegion', 'WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'EAR Cost', 'Cap Premium Cost',
                                'Total Cost ($)']].groupby(['TradingRegion', 'WeekEnding']).quantile(percentile_list)
    df_percentile.reset_index(inplace=True)
    df_percentile = df_percentile.rename(columns={'level_2': 'Percentile'})
    df_percentile_lst = capture_sim_no_for_percentile(df_all_sim, df_percentile)
    df_percentile_update = pd.DataFrame.from_records(df_percentile_lst,
                                                     columns=['TradingRegion', 'WeekEnding', 'Percentile',
                                                              'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                                              'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd',
                                                              'EAR Cost', 'Cap Premium Cost', 'Total Cost ($)',
                                                              'SimNo. (based on Total Cost)'])
    mapping_info = df_percentile_update[['TradingRegion', 'WeekEnding',
                                         'Percentile', 'SimNo. (based on Total Cost)']].to_dict(orient='split')['data']
    write_pickle_to_s3(mapping_info, bucket_nbe, results_EAR_summary_mapping_s3_pickle_path.format(run_id, job_id))
    print('mapping information saved.')

    # output by normal percentiles
    percentile_list = [0, 0.05, 0.25, 0.5, 0.75, 0.95, 1]
    df_percentile = df_all_sim[['TradingRegion', 'WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'EAR Cost', 'Cap Premium Cost',
                                'Total Cost ($)']].groupby(['TradingRegion', 'WeekEnding']).quantile(percentile_list)
    df_percentile.reset_index(inplace=True)
    df_percentile = df_percentile.rename(columns={'level_2': 'Percentile'})
    df_percentile_lst = capture_sim_no_for_percentile(df_all_sim, df_percentile)
    df_percentile_update = pd.DataFrame.from_records(df_percentile_lst,
                                                     columns=['TradingRegion', 'WeekEnding', 'Percentile',
                                                              'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                                              'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd',
                                                              'EAR Cost', 'Cap Premium Cost', 'Total Cost ($)',
                                                              'SimNo. (based on Total Cost)'])
    df_percentile_update['Spot Run No.'] = run_id
    df_percentile_update['Job No.'] = job_id
    df_percentile_update['Year'] = df_percentile_update['WeekEnding'].apply(lambda x: x.year)
    df_percentile_update['Month'] = df_percentile_update['WeekEnding'].apply(lambda x: x.month)
    # df_percentile_update.to_excel('NBE_EAR_Output_by_normal_percentiles_{}_{}.xlsx'.format(run_id, job_id))
    # df_percentile_update.to_csv(results_EAR_normal_percentiles.format(run_id, job_id))
    csv_buffer = StringIO()
    df_percentile_update.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_EAR_normal_percentiles.format(run_id, job_id,
                                                             run_id, job_id)).put(Body=csv_buffer.getvalue())

    # output by percentile for PBI
    percentile_list_risk = [0, 0.01, 0.02, 0.03, 0.05]
    df_percentile_risk = df_all_sim[['TradingRegion', 'WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                     'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'EAR Cost',
                                     'Cap Premium Cost',
                                     'Total Cost ($)']].groupby(['TradingRegion',
                                                                 'WeekEnding']).quantile(percentile_list_risk)
    df_percentile_risk.reset_index(inplace=True)
    df_percentile_risk = df_percentile_risk.rename(columns={'level_2': 'Percentile'})
    df_percentile_risk_lst = capture_sim_no_for_percentile(df_all_sim, df_percentile_risk)
    new_output = duplicate_percentile_for_pbi(df_percentile_risk_lst, p_position=2)
    df_percentile_pbi = pd.DataFrame.from_records(new_output,
                                                  columns=['TradingRegion', 'WeekEnding', 'Percentile',
                                                           'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                                           'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd',
                                                           'EAR Cost', 'Cap Premium Cost', 'Total Cost ($)',
                                                           'SimNo. (based on Total Cost)'])
    df_percentile_pbi['Spot Run No.'] = run_id
    df_percentile_pbi['Job No.'] = job_id
    df_percentile_pbi['Year'] = df_percentile_pbi['WeekEnding'].apply(lambda x: x.year)
    df_percentile_pbi['Month'] = df_percentile_pbi['WeekEnding'].apply(lambda x: x.month)
    # df_percentile_pbi.to_excel('NBE_EAR_Output_by_PBI_percentiles_{}_{}.xlsx'.format(run_id, job_id))
    # df_percentile_pbi.to_csv(results_EAR_PBI_percentiles.format(run_id, job_id))
    csv_buffer = StringIO()
    df_percentile_pbi.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_EAR_PBI_percentiles.format(run_id, job_id,
                                                          run_id, job_id)).put(Body=csv_buffer.getvalue())


def duplicate_percentile_for_pbi(input_lst, p_position):
    new_output = []
    for elem in input_lst:
        if (elem[p_position] == 0.01) or (elem[p_position] == 0.03):
            [new_output.append(elem) for i in range(3)]
        # elif (elem[p_position] == 0.25) or (elem[p_position] == 0.75):
        #     [new_output.append(elem) for i in range(4)]
        elif elem[p_position] == 0.02:
            [new_output.append(elem) for i in range(5)]
        else:
            new_output.append(elem)
    return new_output


def capture_sim_no_for_percentile(original_df, input_df):
    input_df = input_df.rename(columns={'level_2': 'Percentile'})
    output_lst = input_df.to_dict(orient='split')['data']
    for one_row in output_lst:
        trading_region = one_row[0]
        week_ending = one_row[1]
        total_cost = one_row[-1]
        df_tmp = original_df[(original_df['TradingRegion'] == trading_region)
                             & (original_df['WeekEnding'] == week_ending)]
        target_array = df_tmp[['Total Cost ($)', 'SimNo']].reset_index(drop=True)
        sim_no_index_this = np.argmin(abs(target_array['Total Cost ($)'] - total_cost))
        sim_no = target_array.loc[sim_no_index_this]['SimNo']
        one_row.append(sim_no)
    return output_lst


def get_hh_traces(run_id, job_id):
    mapping_info = read_pickle_from_s3(bucket_nbe,
                                       results_EAR_summary_mapping_s3_pickle_path.format(run_id, job_id))
    df_hh_traces = pd.DataFrame()
    for elem in mapping_info:
        print(elem)
        if (elem[0] == 'GrandTotal') & (elem[1] <= datetime(2022, 1, 1).date()):
            week_ending = elem[1]
            week_starting = week_ending - timedelta(weeks=1)
            p = elem[2]
            sim_index = int(elem[3])
            df_sim = read_pickle_from_s3(bucket_nbe,
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
    # df_hh_traces.to_csv('HH_Simulation_Traces_{}.csv'.format(run_id))
    write_pickle_to_s3(df_hh_traces, bucket_nbe, results_EAR_hh_traces_s3_pickle_path.format(run_id, job_id))
    # print(mapping_info)


def get_four_week_blocks(d, *args):
    start_week_ending = args[0]
    week_no = (d-start_week_ending).days / 7
    if week_no == 0:
        return start_week_ending
    else:
        return start_week_ending + timedelta(days=int(4 * 7 * ((week_no-1)//4+1)))


if __name__ == '__main__':
    runid = 10072
    job_id = 5004
    get_output(runid, job_id, 915)
    # get_hh_traces(runid, job_id)
