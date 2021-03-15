import time
import boto3
from utils import read_pickle_from_s3, write_pickle_to_s3
from config import bucket_spot_simulation, bucket_nbe, parameters_for_batch_v2, spot_simulation_input_extra_info, \
    meter_data_pickle_path, meter_data_simulation_s3_pickle_path
import pandas as pd
import datetime
import io

client = boto3.client('s3')
project_start_date = datetime.date(2020, 1, 1)
project_end_date = datetime.date(2023, 12, 31)
run_id = 10072


def simulate_demand_profile(key_name):
    region = key_name.split('/')[1]
    distributor = key_name.split('/')[2].split('.')[0]

    # get reference day parameters from Spot Simulations
    object_key = spot_simulation_input_extra_info
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_nbe, Key=object_key)
    data = obj['Body'].read()
    extra_sim_mapping = pd.read_excel(io.BytesIO(data))
    # extra_sim_mapping = pd.read_excel('../input/mapping.xlsx')
    extra_sim_mapping['original_sim'] = (extra_sim_mapping[1] / 9).astype('int')
    extra_sims = extra_sim_mapping[['original_sim', 3]].to_dict(orient='split')['data']
    params = read_pickle_from_s3(bucket_spot_simulation, parameters_for_batch_v2.format(run_id))

    meter_data_dic = read_pickle_from_s3(bucket_nbe, meter_data_pickle_path.format(region, distributor))
    for sim_i in params.keys():
        sim_meter_data = dict()
        for week in params[sim_i][:209]:  # first 209 weeks in simulation: to 2024-1-2
            for day in week:
                sim_day = day['sim_date']
                ref_day = day['ref_date']
                sim_meter_data[sim_day] = meter_data_dic[ref_day]
        write_pickle_to_s3(sim_meter_data, bucket_nbe,
                           meter_data_simulation_s3_pickle_path.format(run_id, sim_i, distributor))
        print(sim_i)
        for extra in extra_sims:
            if sim_i == extra[0]:
                sim_i_new = extra[1]
                write_pickle_to_s3(sim_meter_data, bucket_nbe,
                                   meter_data_simulation_s3_pickle_path.format(run_id, sim_i_new, distributor))
                print(sim_i_new)


if __name__ == '__main__':
    # e.g. key = 'meter-data-history-pickle/NSW1/NBE_NSW.pickle'
    key = 'meter-data-history-pickle/NSW1/NBE_NSW.pickle'
    simulate_demand_profile(key)
