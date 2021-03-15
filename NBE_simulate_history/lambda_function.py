import json
from simulate_history import main_process


def lambda_handler(event, context):
    key_name = event['Records'][0]['s3']['object']['key']
    print(key_name)
    # e.g. key = 'meter_data_input/demand_profile_NBE.xlsx'
    main_process(key_name)
