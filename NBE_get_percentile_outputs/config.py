""""""
# consulting project specific path
project_bucket = 'nbe-earning-at-risk'
deal_capture_input_path = 'deal_capture_input/{}'  # filename
deal_capture_converted_path = 'deal_capture/Job_{}_{}.pickle'  # Job No., Date

spot_price_by_sim_parquet_path = 'spot_price_by_sim/{}/{}.parquet'  # run_id, sim_index

meter_data_info_path = 'meter_data_info.pickle'
meter_data_path = 'meter-data-history/{}/{}.pickle'  # region, distributor
meter_data_pickle_path = 'meter-data-history-pickle/{}/{}.pickle'  # region, distributor
meter_data_simulation_s3_pickle_path = 'meter-data-simulation-pickle/{}/{}/{}.pickle'  # run_id, sim_index, distributor
meter_data_simulation_s3_partition_path = \
    'meter-data-simulation/{}/sim={}/region={}/year={}/month={}/{}.parquet'

results_data_simulation_s3_pickle_path = 'results_period_def/{}/{}/{}.pickle'  # run_id, sim_index, distributor

results_EAR_simulation_s3_pickle_path = 'EAR_output_by_sim/{}/{}/{}.pickle'  # run_id, job_id, sim_index
results_EAR_summary_by_simulation_s3_pickle_path = 'EAR_output_summary_by_sim/{}/{}/{}.pickle'  # run_id, job_id, sim_index
results_EAR_summary_mapping_s3_pickle_path = 'EAR_statistics/{}/{}/mapping.pickle'  # run_id, job_id
results_EAR_hh_traces_s3_pickle_path = 'HH_Traces/{}/{}.pickle'  # run_id, job_id

results_stress_test_by_sim_s3_pickle_path = 'stress_test_output_by_sim/{}/{}/{}.pickle'  # run_id, sim_index, sim_index
results_stress_test_summary_by_sim_s3_pickle_path = 'stress_test_output_summary_by_sim/{}/{}.pickle'  # run_id, sim_index
results_stress_test_summary_mapping_s3_pickle_path = 'stress_test_output_summary_by_sim/{}/mapping.pickle'  # run_id
results_stress_test_hh_traces_s3_pickle_path = 'HH_Traces_StressTest/{}/{}.pickle'  # run_id, region

# excel or csv outputs filename and paths
# results_EAR_normal_percentiles = 's3://nbe-earning-at-risk/Outputs_PBI/NBE_EAR_Output_by_normal_percentiles_{}_{}.csv'  # run_id, job_id
# results_EAR_PBI_percentiles = 's3://nbe-earning-at-risk/Outputs_PBI/NBE_EAR_Output_by_PBI_percentiles_{}_{}.csv'  # run_id, job_id
results_EAR_normal_percentiles = 'Outputs_PBI/{}/{}/NBE_EAR_Output_by_normal_percentiles_{}_{}.csv'  # run_id, job_id
results_EAR_PBI_percentiles = 'Outputs_PBI/{}/{}/NBE_EAR_Output_by_PBI_percentiles_{}_{}.csv'  # run_id, job_id

# # consulting project specific path
# project_bucket = '007-spot-price-forecast-physical'
# meter_data_info_path = 'projects/NextBusinessEnergy/meter_data_info.pickle'
# meter_data_path = 'projects/NextBusinessEnergy/meter-data-history/{}/{}.pickle'  # region, distributor
# meter_data_pickle_path = 'projects/NextBusinessEnergy/meter-data-history-pickle/{}/{}.pickle'  # region, distributor
# meter_data_simulation_s3_pickle_path = 'projects/NextBusinessEnergy/meter-data-simulation-pickle/{}/{}/{}.pickle'  # run_id, sim_index, distributor
# meter_data_simulation_s3_partition_path = \
#     'projects/NextBusinessEnergy/meter-data-simulation/{}/sim={}/region={}/year={}/month={}/{}.parquet'
# results_data_simulation_s3_pickle_path = 'projects/NextBusinessEnergy/results_period_def/{}/{}/{}.pickle'  # run_id, sim_index, distributor
# results_EAR_simulation_s3_pickle_path = 'projects/NextBusinessEnergy/EAR_output_by_sim/{}/{}.pickle'  # run_id, sim_index
# results_EAR_summary_by_simulation_s3_pickle_path = 'projects/NextBusinessEnergy/EAR_output_summary_by_sim/{}/{}/{}.pickle'  # run_id, job_id, sim_index
# results_EAR_summary_mapping_s3_pickle_path = 'projects/NextBusinessEnergy/EAR_output_summary_by_sim/{}/{}/mapping.pickle'  # run_id, job_id
# results_EAR_hh_traces_s3_pickle_path = 'projects/NextBusinessEnergy/HH_Traces/{}/{}.pickle'  # run_id, job_id
# results_stress_test_by_sim_s3_pickle_path = 'projects/NextBusinessEnergy/stress_test_output_by_sim/{}/{}/{}.pickle'  # run_id, sim_index, sim_index
# results_stress_test_summary_by_sim_s3_pickle_path = 'projects/NextBusinessEnergy/stress_test_output_summary_by_sim/{}/{}.pickle'  # run_id, sim_index
# results_stress_test_summary_mapping_s3_pickle_path = 'projects/NextBusinessEnergy/stress_test_output_summary_by_sim/{}/mapping.pickle'  # run_id
# results_stress_test_hh_traces_s3_pickle_path = 'projects/NextBusinessEnergy/HH_Traces_StressTest/{}/{}.pickle'  # run_id, region