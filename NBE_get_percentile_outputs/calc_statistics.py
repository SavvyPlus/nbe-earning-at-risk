import pandas as pd
import numpy as np
import boto3
import json
from utils import read_pickle_from_s3, write_pickle_to_s3
from config import bucket_nbe, results_EAR_simulation_s3_pickle_path, \
    results_EAR_week_summary_by_simulation_s3_pickle_path, results_EAR_summary_mapping_s3_pickle_path, \
    results_EAR_hh_traces_s3_pickle_path, results_EAR_normal_percentiles, results_EAR_PBI_percentiles, \
    results_EAR_mth_summary_by_simulation_s3_pickle_path, results_EAR_qtr_summary_by_simulation_s3_pickle_path, \
    results_by_sim_by_week, results_by_sim_by_month, results_by_sim_by_quarter, results_by_sim_by_week_complete, \
    results_by_sim_by_month_complete, results_by_sim_by_quarter_complete, results_avg_price_by_profile_by_sim_path, \
    results_avg_price_by_profile_by_sim_csv_path
from datetime import datetime, timedelta
from io import BytesIO, StringIO


def get_output(run_id, job_id, sim_num):
    # output by simulation - weekly/monthly/quarterly average price
    for p in ['week', 'month', 'quarter']:
        print("loading data of all simulations by {}.".format(p))
        all_sim_lst = []
        for i in range(sim_num):
            if i < 900:
                tmp_lst = \
                    read_pickle_from_s3(bucket_nbe,
                                        results_avg_price_by_profile_by_sim_path.format(p, job_id, run_id, i))
            else:
                tmp_lst = \
                    read_pickle_from_s3(bucket_nbe,
                                        results_avg_price_by_profile_by_sim_path.format(p, job_id, run_id,
                                                                                        900 + (i - 900) * 9))
            all_sim_lst += tmp_lst
            print(i)
        col_period = 'WeekEnding' if p == 'week' else 'PeriodEnding'
        df_output = pd.DataFrame(all_sim_lst, columns=[col_period, 'TradingRegion', 'SimNo', 'Profile',
                                                       'Average Spot Price', 'Average Cap Payouts', 'NoOfHour',
                                                       'ProfileID', 'RegionID'])
        df_output['Profile'] = df_output['Profile'].apply(remove_profile_region_suffix)
        df_output['Spot Run No.'] = df_output.apply(lambda _: run_id, axis=1)
        df_output['Job No.'] = df_output.apply(lambda _: job_id, axis=1)
        csv_buffer = StringIO()
        df_output.to_csv(csv_buffer, index=False)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket_nbe,
                           results_avg_price_by_profile_by_sim_csv_path.format(job_id,
                                                                               run_id,
                                                                               p,
                                                                               job_id,
                                                                               run_id)).put(Body=csv_buffer.getvalue())

    # output by simulation - weekly
    print("loading data of all simulations by week.")
    df_all_sim = pd.DataFrame()
    for i in range(sim_num):
        if i < 900:
            df_tmp = \
                read_pickle_from_s3(bucket_nbe,
                                    results_EAR_week_summary_by_simulation_s3_pickle_path.format(run_id, job_id, i))
        else:
            df_tmp = \
                read_pickle_from_s3(bucket_nbe,
                                    results_EAR_week_summary_by_simulation_s3_pickle_path.format(run_id,
                                                                                                 job_id,
                                                                                                 900 + (i - 900) * 9))
        df_all_sim = df_all_sim.append(df_tmp)
        print(i)
    df_all_sim['Spot Run No.'] = run_id
    df_all_sim['Job No.'] = job_id
    # EAR_Output_by_simulations_by_week.csv
    df_all_sim_week_complete = df_all_sim
    df_all_sim_week_complete.reset_index(inplace=True)
    df_all_sim_week_complete = df_all_sim_week_complete.rename(columns={'index': 'Case'})
    csv_buffer = StringIO()
    df_all_sim_week_complete.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_by_sim_by_week_complete.format(job_id,
                                                              run_id,
                                                              job_id,
                                                              run_id)).put(Body=csv_buffer.getvalue())
    # EAR_Output_by_sim_by_week_for_CFD_impact.csv
    df_all_sim_week = df_all_sim[
        ['TradingRegion', 'WeekEnding', 'Total Cost (Incl Cap)', 'Transfer Cost', 'Wholesale Margin', 'SimNo',
         'Spot Run No.', 'Job No.']]
    df_all_sim_week.reset_index(inplace=True)
    df_all_sim_week = df_all_sim_week.rename(columns={'index': 'Case'})
    csv_buffer = StringIO()
    df_all_sim_week.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_by_sim_by_week.format(job_id, run_id, job_id, run_id)).put(Body=csv_buffer.getvalue())

    # output by simulation - monthly
    print("loading data of all simulations by month.")
    df_all_sim_month = pd.DataFrame()
    for i in range(sim_num):
        if i < 900:
            df_tmp = read_pickle_from_s3(bucket_nbe,
                                         results_EAR_mth_summary_by_simulation_s3_pickle_path.format(run_id, job_id, i))
        else:
            df_tmp = read_pickle_from_s3(bucket_nbe,
                                         results_EAR_mth_summary_by_simulation_s3_pickle_path.format(run_id,
                                                                                                     job_id,
                                                                                                     900+(i - 900) * 9))
        df_all_sim_month = df_all_sim_month.append(df_tmp)
        print(i)
    df_all_sim_month['Spot Run No.'] = run_id
    df_all_sim_month['Job No.'] = job_id
    # EAR_Output_by_simulations_by_month.csv
    df_all_sim_month_complete = df_all_sim_month
    df_all_sim_month_complete.reset_index(inplace=True)
    df_all_sim_month_complete = df_all_sim_month_complete.rename(columns={'index': 'Case'})
    csv_buffer = StringIO()
    df_all_sim_month_complete.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_by_sim_by_month_complete.format(job_id,
                                                               run_id,
                                                               job_id,
                                                               run_id)).put(Body=csv_buffer.getvalue())
    # EAR_Output_by_sim_by_month_for_CFD_impact.csv
    df_all_sim_month.reset_index(inplace=True)
    df_all_sim_month = df_all_sim_month.rename(columns={'index': 'Case'})
    df_all_sim_month = df_all_sim_month[
        ['Case', 'TradingRegion', 'MonthEnding', 'Total Cost (Incl Cap)', 'Transfer Cost', 'Wholesale Margin',
         'SimNo', 'Spot Run No.', 'Job No.']]
    csv_buffer = StringIO()
    df_all_sim_month.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_by_sim_by_month.format(job_id, run_id, job_id, run_id)).put(Body=csv_buffer.getvalue())

    # output by simulation - quarterly
    print("loading data of all simulations by quarter.")
    df_all_sim_qtr = pd.DataFrame()
    for i in range(sim_num):
        if i < 900:
            df_tmp = read_pickle_from_s3(bucket_nbe,
                                         results_EAR_qtr_summary_by_simulation_s3_pickle_path.format(run_id, job_id, i))
        else:
            df_tmp = read_pickle_from_s3(bucket_nbe,
                                         results_EAR_qtr_summary_by_simulation_s3_pickle_path.format(run_id,
                                                                                                     job_id,
                                                                                                     900+(i - 900) * 9))
        df_all_sim_qtr = df_all_sim_qtr.append(df_tmp)
        print(i)
    df_all_sim_qtr['Spot Run No.'] = run_id
    df_all_sim_qtr['Job No.'] = job_id
    # EAR_Output_by_simulations_by_quarter.csv
    df_all_sim_quarter_complete = df_all_sim_qtr
    df_all_sim_quarter_complete.reset_index(inplace=True)
    df_all_sim_quarter_complete = df_all_sim_quarter_complete.rename(columns={'index': 'Case'})
    csv_buffer = StringIO()
    df_all_sim_quarter_complete.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_by_sim_by_quarter_complete.format(job_id,
                                                                 run_id,
                                                                 job_id,
                                                                 run_id)).put(Body=csv_buffer.getvalue())
    # EAR_Output_by_sim_by_quarter_for_CFD_impact.csv
    df_all_sim_qtr.reset_index(inplace=True)
    df_all_sim_qtr = df_all_sim_qtr.rename(columns={'index': 'Case'})
    df_all_sim_qtr = df_all_sim_qtr[
        ['Case', 'TradingRegion', 'QuarterEnding', 'Total Cost (Incl Cap)', 'Transfer Cost', 'Wholesale Margin',
         'SimNo', 'Spot Run No.', 'Job No.']]
    csv_buffer = StringIO()
    df_all_sim_qtr.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_by_sim_by_quarter.format(job_id, run_id, job_id, run_id)).put(Body=csv_buffer.getvalue())

    # save the mapping of percentile & sim_index of all percentiles
    percentile_list = [0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.25, 0.5, 0.75, 0.95, 1]
    df_percentile = df_all_sim[['TradingRegion', 'WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'Total Cost (excl GST)',
                                'Cap Premium Cost', 'Total Cost (Incl Cap)', 'Transfer Cost',
                                'Wholesale Margin']].groupby(['TradingRegion', 'WeekEnding']).quantile(percentile_list)
    df_percentile.reset_index(inplace=True)
    df_percentile = df_percentile.rename(columns={'level_2': 'Percentile'})
    df_percentile_lst = capture_sim_no_for_percentile(df_all_sim, df_percentile)
    df_percentile_update = pd.DataFrame.from_records(df_percentile_lst,
                                                     columns=['TradingRegion', 'WeekEnding', 'Percentile',
                                                              'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                                              'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd',
                                                              'Total Cost (excl GST)',
                                                              'Cap Premium Cost', 'Total Cost (Incl Cap)',
                                                              'Transfer Cost', 'Wholesale Margin',
                                                              'SimNo. (based on Wholesale Margin)'])
    mapping_info = df_percentile_update[['TradingRegion', 'WeekEnding',
                                         'Percentile', 'SimNo. (based on Wholesale Margin)']].to_dict(orient='split')[
        'data']
    write_pickle_to_s3(mapping_info, bucket_nbe, results_EAR_summary_mapping_s3_pickle_path.format(job_id, run_id))
    print('mapping information saved.')

    # EAR_Output_by_normal_percentiles.csv
    percentile_list = [0, 0.05, 0.25, 0.5, 0.75, 0.95, 1]
    df_percentile = df_all_sim[['TradingRegion', 'WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'Total Cost (excl GST)',
                                'Cap Premium Cost', 'Total Cost (Incl Cap)',
                                'Transfer Cost', 'Wholesale Margin']].groupby(['TradingRegion', 'WeekEnding']).quantile(
        percentile_list)
    df_percentile.reset_index(inplace=True)
    df_percentile = df_percentile.rename(columns={'level_2': 'Percentile'})
    df_percentile_lst = capture_sim_no_for_percentile(df_all_sim, df_percentile)
    df_percentile_update = pd.DataFrame.from_records(df_percentile_lst,
                                                     columns=['TradingRegion', 'WeekEnding', 'Percentile',
                                                              'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                                              'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd',
                                                              'Total Cost (excl GST)',
                                                              'Cap Premium Cost', 'Total Cost (Incl Cap)',
                                                              'Transfer Cost', 'Wholesale Margin',
                                                              'SimNo. (based on Wholesale Margin)'])
    df_percentile_update['Spot Run No.'] = run_id
    df_percentile_update['Job No.'] = job_id
    df_percentile_update.reset_index(inplace=True)
    df_percentile_update = df_percentile_update.rename(columns={'index': 'Case'})
    # df_percentile_update['Year'] = df_percentile_update['WeekEnding'].apply(lambda x: x.year)
    # df_percentile_update['Month'] = df_percentile_update['WeekEnding'].apply(lambda x: x.month)
    # df_percentile_update.to_excel('NBE_EAR_Output_by_normal_percentiles_{}_{}.xlsx'.format(run_id, job_id))
    # df_percentile_update.to_csv(results_EAR_normal_percentiles.format(run_id, job_id))
    csv_buffer = StringIO()
    df_percentile_update.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_EAR_normal_percentiles.format(job_id, run_id, job_id,
                                                             run_id)).put(Body=csv_buffer.getvalue())

    # output by risk percentiles for PBI
    '''
    percentile_list_risk = [0, 0.01, 0.02, 0.03, 0.05]
    df_percentile_risk = df_all_sim[['TradingRegion', 'WeekEnding', 'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                     'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd', 'Total Cost (excl GST)',
                                     'Cap Premium Cost', 'Total Cost (Incl Cap)',
                                     'Transfer Cost', 'Wholesale Margin']].groupby(['TradingRegion',
                                                                                    'WeekEnding']).quantile(
        percentile_list_risk)
    df_percentile_risk.reset_index(inplace=True)
    df_percentile_risk = df_percentile_risk.rename(columns={'level_2': 'Percentile'})
    df_percentile_risk_lst = capture_sim_no_for_percentile(df_all_sim, df_percentile_risk)
    new_output = duplicate_percentile_for_pbi(df_percentile_risk_lst, p_position=2)
    df_percentile_pbi = pd.DataFrame.from_records(new_output,
                                                  columns=['TradingRegion', 'WeekEnding', 'Percentile',
                                                           'Swap Hedged Qty (MWh)', 'Cap Hedged Qty (MWh)',
                                                           'Customer Net MWh', 'Pool Cost', 'Swap Cfd', 'Cap Cfd',
                                                           'Total Cost (excl GST)',
                                                           'Cap Premium Cost', 'Total Cost (Incl Cap)',
                                                           'Transfer Cost', 'Wholesale Margin',
                                                           'SimNo. (based on Wholesale Margin)'])
    df_percentile_pbi['Spot Run No.'] = run_id
    df_percentile_pbi['Job No.'] = job_id
    # df_percentile_pbi['Year'] = df_percentile_pbi['WeekEnding'].apply(lambda x: x.year)
    # df_percentile_pbi['Month'] = df_percentile_pbi['WeekEnding'].apply(lambda x: x.month)
    # df_percentile_pbi.to_excel('NBE_EAR_Output_by_PBI_percentiles_{}_{}.xlsx'.format(run_id, job_id))
    # df_percentile_pbi.to_csv(results_EAR_PBI_percentiles.format(run_id, job_id))
    csv_buffer = StringIO()
    df_percentile_pbi.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_EAR_PBI_percentiles.format(run_id, job_id,
                                                          run_id, job_id)).put(Body=csv_buffer.getvalue())
    '''


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
    """

    :param original_df:
    :param input_df:
    :return:
    """
    input_df = input_df.rename(columns={'level_2': 'Percentile'})
    output_lst = input_df.to_dict(orient='split')['data']
    for one_row in output_lst:
        trading_region = one_row[0]
        week_ending = one_row[1]
        total_cost = one_row[-1]
        df_tmp = original_df[(original_df['TradingRegion'] == trading_region)
                             & (original_df['WeekEnding'] == week_ending)]
        target_array = df_tmp[['Wholesale Margin', 'SimNo']].reset_index(drop=True)
        sim_no_index_this = np.argmin(abs(target_array['Wholesale Margin'] - total_cost))
        sim_no = target_array.loc[sim_no_index_this]['SimNo']
        one_row.append(sim_no)
    return output_lst


def get_four_week_blocks(d, *args):
    start_week_ending = args[0]
    week_no = (d - start_week_ending).days / 7
    if week_no == 0:
        return start_week_ending
    else:
        return start_week_ending + timedelta(days=int(4 * 7 * ((week_no - 1) // 4 + 1)))


def remove_profile_region_suffix(profile_name):
    new_profile = profile_name
    if '(' in profile_name:
        end_ind = profile_name.index('(') - 1
        new_profile = profile_name[:end_ind]
    return new_profile


if __name__ == '__main__':
    runid = 50015
    job_id = 41
    get_output(runid, job_id, 38)
