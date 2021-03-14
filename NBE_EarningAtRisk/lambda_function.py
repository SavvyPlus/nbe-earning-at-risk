import boto3
import pickle
import datetime
import time
from config import project_bucket, deal_capture_input_path, deal_capture_converted_path
from utils import write_pickle_to_s3, read_pickle_from_s3
from calcs_ear import load_calculate_summarize
import pandas as pd


def lambda_handler(event, context):
    run_id = event['run_id']
    job_id = event['job_id']
    date_input = event['date_input']
    sim_index = event['sim_index']
    start_year = event['start_year']
    start_month = event['start_month']
    start_day = event['start_day']
    end_year = event['end_year']
    end_month = event['end_month']
    end_day = event['end_day']
    load_calculate_summarize(run_id,
                             job_id,
                             date_input,
                             sim_index,
                             start_year,
                             start_month,
                             start_day,
                             end_year,
                             end_month,
                             end_day)
