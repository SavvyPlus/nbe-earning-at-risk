#!/bin/bash
# step 1: chmod +x ./code_update.sh
# step 2: ./code_update.sh
echo "Updating the code in different Lambda folders from central dev version..."

# NBE_check_ear_summary_output_by_sim
mkdir -p NBE_check_ear_summary_output_by_sim/
ln -f dev/config.py NBE_check_ear_summary_output_by_sim/config.py

# NBE_EAR_HH_Traces
mkdir -p NBE_EAR_HH_Traces/
ln -f dev/config.py NBE_EAR_HH_Traces/config.py
ln -f dev/utils.py NBE_EAR_HH_Traces/utils.py

# NBE_EarningAtRisk
mkdir -p NBE_EarningAtRisk/
ln -f dev/calcs_ear.py NBE_EarningAtRisk/calcs_ear.py
ln -f dev/config.py NBE_EarningAtRisk/config.py
ln -f dev/utils.py NBE_EarningAtRisk/utils.py

# NBE_get_percentile_outputs
mkdir -p NBE_get_percentile_outputs/
ln -f dev/utils.py NBE_get_percentile_outputs/utils.py
ln -f dev/config.py NBE_get_percentile_outputs/config.py
ln -f dev/calc_statistics.py NBE_get_percentile_outputs/calc_statistics.py

# NBE_process_trade_data
mkdir -p NBE_process_trade_data/
ln -f dev/utils.py NBE_process_trade_data/utils.py
ln -f dev/config.py NBE_process_trade_data/config.py
ln -f dev/preprocess_trade_data.py NBE_process_trade_data/preprocess_trade_data.py

# NBE_send_outputs_via_emails
mkdir -p NBE_send_outputs_via_emails/
ln -f dev/config.py NBE_send_outputs_via_emails/config.py

# NBE_simulate_customer_data
mkdir -p NBE_simulate_customer_data/
ln -f dev/utils.py NBE_simulate_customer_data/utils.py
ln -f dev/config.py NBE_simulate_customer_data/config.py
ln -f dev/simulate_customer_data.py NBE_simulate_customer_data/simulate_customer_data.py

# NBE_simulate_history
mkdir -p NBE_simulate_history/
ln -f dev/utils.py NBE_simulate_history/utils.py
ln -f dev/config.py NBE_simulate_history/config.py
ln -f dev/simulate_history.py NBE_simulate_history/simulate_history.py

echo "Done!"