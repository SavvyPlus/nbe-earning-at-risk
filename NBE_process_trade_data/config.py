# configurations from Spot Price Simulation
ref_start_date_str = '2017-01-01'
ref_end_date_str = '2021-01-31'
bucket_spot_simulation = 'spot-price-forecast-simulation-prod'
public_holiday_path = 'public_holiday/{}.pickle'  # state
weather_stations = {'VIC1': 'MELBOURNE AIRPORT', 'NSW1': 'SYDNEY OLYMPIC PARK (ARCHERY CENTRE)',
                    'SA1': 'ADELAIDE AIRPORT', 'QLD1': 'BRISBANE', 'TAS1': 'HOBART (ELLERSLIE ROAD)'}
weather_data_path = 'historical-weather/{}.pickle'  # weather station name
parameters_for_batch_v2 = 'cache/{}/parameters_for_batch_test2.pickle'
spot_simulation_input_extra_info = 'spot_simulation_input/mapping_{}.xlsx'  # run_id

# configurations for a specific consulting project
bucket_nbe = 'nbe-earning-at-risk-prod'
deal_capture_input_path = 'deal_capture_input/{}'  # filename
deal_capture_converted_path = 'deal_capture/Job_{}_{}.pickle'  # Job No., Date

spot_price_by_sim_parquet_path = 'spot_price_by_sim/{}/{}.parquet'  # run_id, sim_index

meter_data_file_path = 'meter_data_input/{}'  # filename
meter_data_info_path = 'meter_data_info.pickle'
meter_data_pickle_path = 'meter-data-history-pickle/{}/{}.pickle'  # region, distributor
meter_data_simulation_s3_pickle_path = 'meter-data-simulation-pickle/{}/{}/{}.pickle'  # run_id, sim_index, distributor
meter_data_simulation_s3_partition_path = \
    'meter-data-simulation/{}/sim={}/region={}/year={}/month={}/{}.parquet'

results_data_simulation_s3_pickle_path = 'results_period_def/{}/{}/{}.pickle'  # run_id, sim_index, distributor

results_EAR_simulation_s3_pickle_path = 'EAR_output_by_sim/{}/{}/{}.pickle'  # run_id, job_id, sim_index
results_EAR_summary_by_simulation_s3_pickle_path = 'EAR_output_summary_by_sim/{}/{}/{}.pickle'  # run_id, job_id, sim_index
results_EAR_summary_mapping_s3_pickle_path = 'EAR_statistics/{}/{}/mapping.pickle'  # run_id, job_id
results_EAR_normal_percentiles = 'Outputs_PBI/{}/{}/NBE_EAR_Output_by_normal_percentiles_{}_{}.csv'  # run_id, job_id
results_EAR_PBI_percentiles = 'Outputs_PBI/{}/{}/NBE_EAR_Output_by_PBI_percentiles_{}_{}.csv'  # run_id, job_id
results_EAR_hh_traces_s3_pickle_path = 'Outputs_PBI/{}/{}/HH_Simulation_Traces_{}_{}.csv'  # run_id, job_id
