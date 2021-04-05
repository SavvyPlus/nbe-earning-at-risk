import pandas as pd
import numpy as np
import boto3
import json
from utils import read_pickle_from_s3, write_pickle_to_s3
from config import bucket_nbe, bucket_spot_simulation, results_stress_test_by_sim_s3_pickle_path, \
    results_stress_test_summary_by_sim_s3_pickle_path, results_stress_test_summary_mapping_s3_pickle_path
from datetime import datetime, timedelta


def get_week_ending(dt):
    d = (dt - timedelta(minutes=30)).to_pydatetime()  # to get hh starting
    # In NEM, a week ends in Saturday (isoweekday is 6)
    delta = 6 if d.isoweekday() == 7 else 6 - d.isoweekday()
    week_ending_date = (d + timedelta(days=delta)).date()
    return week_ending_date


def get_four_week_blocks(d, *args):
    start_week_ending = args[0]
    week_no = (d-start_week_ending).days / 7
    if week_no == 0:
        return start_week_ending
    else:
        return start_week_ending + timedelta(days=int(4 * 7 * ((week_no-1)//4+1)))


def stress_test_summary(run_id, sim_index):
    df_raw = read_pickle_from_s3(bucket_nbe,
                                 results_stress_test_by_sim_s3_pickle_path.format(run_id, sim_index, sim_index))
    df = df_raw[['TradingRegion', 'SettlementDateTime', 'Adjusted EAR Cost']]
    df['WeekEnding'] = df['SettlementDateTime'].apply(get_week_ending)
    week_ending_init = df['WeekEnding'].iloc[0]
    df['FourWeekBlocks'] = df['WeekEnding'].apply(get_four_week_blocks, args=(week_ending_init, ))
    df_summarized = df[['TradingRegion', 'Adjusted EAR Cost',
                        'FourWeekBlocks']].groupby(['TradingRegion', 'FourWeekBlocks']).sum()
    df_grandtotal = df[['FourWeekBlocks', 'Adjusted EAR Cost']].groupby(['FourWeekBlocks']).sum()
    df_grandtotal.reset_index(inplace=True)
    df_grandtotal.insert(0, 'TradingRegion', 'GrandTotal')
    df_summarized.reset_index(inplace=True)
    df_summarized = df_summarized.append(df_grandtotal).reset_index(drop=True)
    df_summarized['SimNo'] = sim_index
    write_pickle_to_s3(df_summarized,
                       bucket_nbe,
                       results_stress_test_summary_by_sim_s3_pickle_path.format(run_id, sim_index))
    return df_summarized


def duplicate_percentile_for_pbi_stress_test(input_lst, p_position):
    new_output = []
    for elem in input_lst:
        if (elem[p_position] == 0.05) or (elem[p_position] == 0.95):
            [new_output.append(elem) for i in range(3)]
        elif (elem[p_position] == 0.25) or (elem[p_position] == 0.75):
            [new_output.append(elem) for i in range(4)]
        elif elem[p_position] == 0.5:
            [new_output.append(elem) for i in range(5)]
        else:
            new_output.append(elem)
    return new_output


def capture_sim_no_for_percentile_stress_test(original_df, input_df):
    input_df = input_df.rename(columns={'level_2': 'Percentile'})
    output_lst = input_df.to_dict(orient='split')['data']
    for one_row in output_lst:
        trading_region = one_row[0]
        week_ending = one_row[1]
        adjusted_ear_cost = one_row[-1]
        df_tmp = original_df[(original_df['TradingRegion'] == trading_region)
                             & (original_df['FourWeekBlocks'] == week_ending)]
        target_array = df_tmp[['Adjusted EAR Cost', 'SimNo']].reset_index(drop=True)
        sim_no_index_this = np.argmin(abs(target_array['Adjusted EAR Cost'] - adjusted_ear_cost))
        sim_no = target_array.loc[sim_no_index_this]['SimNo']
        one_row.append(sim_no)
    return output_lst


def get_output_stress_test(run_id, sim_num):
    # output by simulation
    df_all_sim = pd.DataFrame()
    for i in range(sim_num):
        if i < 900:
            df_tmp = read_pickle_from_s3(bucket_nbe,
                                         results_stress_test_summary_by_sim_s3_pickle_path.format(runid, i))
        else:
            df_tmp = read_pickle_from_s3(bucket_nbe,
                                         results_stress_test_summary_by_sim_s3_pickle_path.format(runid,
                                                                                                  900 + (i - 900) * 9))
        df_all_sim = df_all_sim.append(df_tmp)
        print(i)

    # output by percentile, normal percentile format, and save the mapping of percentile & sim_index
    percentile_list = [0, 0.05, 0.25, 0.5, 0.75, 0.95, 1]
    df_percentile = df_all_sim[['TradingRegion', 'FourWeekBlocks',
                                'Adjusted EAR Cost']].groupby(['TradingRegion',
                                                               'FourWeekBlocks']).quantile(percentile_list)
    df_percentile.reset_index(inplace=True)
    df_percentile = df_percentile.rename(columns={'level_2': 'Percentile'})
    df_percentile_lst = capture_sim_no_for_percentile_stress_test(df_all_sim, df_percentile)
    df_percentile_update = pd.DataFrame.from_records(df_percentile_lst,
                                                     columns=['TradingRegion', 'FourWeekBlocks', 'Percentile',
                                                              'Adjusted EAR Cost',
                                                              'SimNo. (based on Adjusted EAR Cost)'])
    mapping_info = df_percentile_update[['TradingRegion', 'FourWeekBlocks', 'Percentile',
                                         'SimNo. (based on Adjusted EAR Cost)']].to_dict(orient='split')['data']
    write_pickle_to_s3(mapping_info, bucket_nbe, results_stress_test_summary_mapping_s3_pickle_path.format(run_id))
    print('mapping information saved.')

    new_output = duplicate_percentile_for_pbi_stress_test(df_percentile_lst, p_position=2)
    df_percentile_pbi = pd.DataFrame.from_records(new_output,
                                                  columns=['TradingRegion', 'FourWeekBlocks', 'Percentile',
                                                           'Adjusted EAR Cost',
                                                           'SimNo. (based on Total Cost)'])
    df_percentile_pbi['Spot Run No.'] = run_id
    df_percentile_pbi.to_excel('NBE_StressTest_Output_by_PBI_percentiles_{}.xlsx'.format(run_id))


def get_hh_traces_stress_test(run_id, region):
    mapping_info = read_pickle_from_s3(project_bucket,
                                       results_stress_test_summary_mapping_s3_pickle_path.format(run_id))
    df_hh_traces = pd.DataFrame()
    for elem in mapping_info:
        print(elem)
        if (elem[0] == region) & (elem[1] <= datetime(2022, 1, 1).date()):
            week_ending = elem[1]
            week_starting = week_ending - timedelta(weeks=1)
            p = elem[2]
            sim_index = int(elem[3])
            df_sim = read_pickle_from_s3(project_bucket,
                                         results_stress_test_by_sim_s3_pickle_path.format(run_id,
                                                                                          sim_index,
                                                                                          sim_index))
            df_sim['Date'] = \
                df_sim['SettlementDateTime'].apply(lambda row: (row.to_pydatetime() - timedelta(minutes=30)).date())
            df_tmp = df_sim[(df_sim['Date'] > week_starting)
                            & (df_sim['Date'] <= week_ending)][['TradingRegion', 'SettlementDateTime',
                                                                'Spot Price', 'adjusted price', 'Adjusted EAR Cost']]
            df_tmp['Percentile'] = p
            df_tmp['PercentileBase'] = elem[0]
            df_hh_traces = df_hh_traces.append(df_tmp)
        else:
            continue

    write_pickle_to_s3(df_hh_traces, project_bucket,
                       results_stress_test_hh_traces_s3_pickle_path.format(run_id, region))
    # df_hh_traces.to_excel('HH_Simulation_Traces_StressTest_{}.xlsx'.format(run_id))
    # print(mapping_info)


if __name__ == '__main__':
    runid = 50014
    job_id = 39
    sim_num = 930

    # for i in range(sim_num):
    #     if i < 900:
    #         stress_test_summary(runid, i)
    #     else:
    #         stress_test_summary(runid, 900 + (i-900)*9)
    #     print(i)

    # function_name = 'NBE_StressTest_Summary_by_Sim'
    # sim_num = 930
    # client = boto3.client('lambda')
    # for i in range(sim_num):
    #     if i >= 900:
    #         i = 900 + (i - 900) * 9
    #     payload = {
    #         "run_id": "50014",
    #         "sim_index": i
    #     }
    #     client.invoke(
    #         FunctionName=function_name,
    #         InvocationType='Event',
    #         LogType='Tail',
    #         Payload=json.dumps(payload),
    #     )
    #     print(i)

    get_output_stress_test(runid, sim_num)

    # get_hh_traces_stress_test(run_id=10072, region='GrandTotal')
    # df = pd.DataFrame()
    # for region in ['GrandTotal', 'VIC1', 'NSW1', 'SA1', 'QLD1']:
    #     df1 = read_pickle_from_s3(project_bucket,
    #                               results_stress_test_hh_traces_s3_pickle_path.format(runid, region))
    #     df = df.append(df1)
    #     print(region)
    # df.to_excel('HH_Simulation_Traces_StressTest_{}.xlsx'.format(runid))
