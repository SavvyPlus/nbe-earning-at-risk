import json
from calc_statistics import get_output


def lambda_handler(event, context):
    run_id = event['run_id']
    job_id = event['job_id']
    sim_num = event['sim_num']
    get_output(run_id, job_id, sim_num)
