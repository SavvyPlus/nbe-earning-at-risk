# TODO the end date is hardcoded.
import pandas as pd
import numpy as np
import boto3
import json
from utils import read_pickle_from_s3, write_pickle_to_s3
from config import bucket_nbe, results_EAR_simulation_s3_pickle_path, \
    results_EAR_week_summary_by_simulation_s3_pickle_path, results_EAR_summary_mapping_s3_pickle_path, \
    results_EAR_hh_traces_s3_pickle_path
from datetime import datetime, timedelta
from io import BytesIO, StringIO

analysis_end_date = datetime(2022, 4, 23).date()


def get_hh_traces(run_id, job_id):
    """
    mapping_info is a list of percentile - sim no information, the values are:

    TradingRegion -> str  e.g. 'GrandTotal', 'VIC1', etc
    WeekEnding -> datetime.date  e.g. datetime.date(2021,1,9)
    Percentile -> float   e.g. 0.05
    Sim No. -> float   e.g. 269(.0)

    :param run_id:
    :param job_id:
    :return:
    """
    mapping_info = read_pickle_from_s3(bucket_nbe,
                                       results_EAR_summary_mapping_s3_pickle_path.format(job_id, run_id))
    df_hh_traces = pd.DataFrame()
    for elem in mapping_info:
        print(elem)
        if (elem[0] == 'GrandTotal') & (elem[1] <= analysis_end_date):
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
                                                                'Spot Price', 'Total Cost (excl GST)',
                                                                'Total Cost (Incl Cap)']]
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
    df_hh_traces.reset_index(inplace=True)
    df_hh_traces = df_hh_traces.rename(columns={'index': 'Case'})
    # df_hh_traces.csv('HH_Simulation_Traces_{}.csv'.format(run_id))
    # write_pickle_to_s3(df_hh_traces, project_bucket, results_EAR_hh_traces_s3_pickle_path.format(run_id, job_id))
    csv_buffer = StringIO()
    df_hh_traces.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_nbe,
                       results_EAR_hh_traces_s3_pickle_path.format(run_id, job_id, job_id, run_id)).put(
        Body=csv_buffer.getvalue())


if __name__ == '__main__':
    get_hh_traces(50015, 41)
