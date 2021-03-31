import pandas as pd
from utils import read_pickle_from_s3, write_pickle_to_s3, put_object_to_s3
from config import bucket_spot_simulation, bucket_nbe, meter_data_simulation_s3_pickle_path, \
    meter_data_simulation_s3_partition_path, future_public_holiday_path, \
    public_holidays_def
from io import BytesIO
import json
import boto3


def day_type(datetime_date, *args):
    """
    Given a datetime.date(), get the day type of it:
    1: Sunday/Public Holiday; 2: Working Week Day; 7: Saturday
    :param datetime_date: datetime.date()
    :param args: arguments tuple passed from pandas.apply()
    :return: int
    """
    day_in_a_week = datetime_date.weekday()
    public_holiday = args[0]
    # return day type
    if datetime_date in public_holiday or day_in_a_week == 6:
        return 1
    if day_in_a_week in (0, 1, 2, 3, 4):
        return 2
    if day_in_a_week == 5:
        return 7


def pickle2parquet(run_id, sim_index):
    states = ['NSW1', 'QLD1', 'SA1', 'VIC1']
    print(sim_index)
    for state in states:
        print(state)
        # get public holidays during simulation period
        simulation_public_holiday = read_pickle_from_s3(bucket_spot_simulation,
                                                        future_public_holiday_path.format(public_holidays_def,
                                                                                          state))
        state_load_all = read_pickle_from_s3(bucket_nbe,
                                             meter_data_simulation_s3_pickle_path.format(run_id,
                                                                                         sim_index,
                                                                                         'NBE_' + state[:-1]))
        for curr_day, v in state_load_all.items():
            df = pd.DataFrame({'Date': curr_day,
                               'PeriodID': [i + 1 for i in range(48)],
                               'Customer Net MWh': list(v['GRID_USAGE'])})
            df['DayType'] = df['Date'].apply(day_type, args=(simulation_public_holiday,))

            out_buffer = BytesIO()
            df.to_parquet(out_buffer, engine='pyarrow', index=False)
            put_object_to_s3(out_buffer.getvalue(),
                             bucket_nbe,
                             meter_data_simulation_s3_partition_path.format(run_id, sim_index,
                                                                            state,
                                                                            curr_day.year,
                                                                            curr_day.month,
                                                                            curr_day))
            out_buffer.close()
            print(curr_day)


if __name__ == '__main__':
    # pickle2parquet(run_id=50014, sim_index=114)
    client = boto3.client('lambda')
    for i in range(930):
        function_name = 'NBE_customer_data_partition'
        payload = {'run_id': 50014,
                   'sim_index': i}
        client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            LogType='Tail',
            Payload=json.dumps(payload),
        )
        print(i)
