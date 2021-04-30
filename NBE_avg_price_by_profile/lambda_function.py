import boto3
from avg_price_by_profile import main


def lambda_handler(event, context):
    main(event)
