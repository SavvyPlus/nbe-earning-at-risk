import json
from simulate_customer_data import simulate_demand_profile


def lambda_handler(event, context):
    key_name = event['Records'][0]['s3']['object']['key']
    print(key_name)
    # e.g. key = 'meter-data-history-pickle/NSW1/NBE_NSW.pickle'
    simulate_demand_profile(key_name)
