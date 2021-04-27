import boto3
import datetime
import time
import pandas as pd
import io
from config import bucket_nbe, deal_capture_input_path, deal_capture_converted_path
from utils import write_pickle_to_s3


def transform_format(job_id, date_input, filename, sheet_name,
                     start_year, start_month, start_day,
                     end_year, end_month, end_day):
    """

    :param job_id:
    :param date_input:
    :param filename:
    :param sheet_name:
    :param start_year:
    :param start_month:
    :param start_day:
    :param end_year:
    :param end_month:
    :param end_day:
    :return:
    """
    start = time.time()
    # These are the user defined start datetime and end datetime of the analysis
    start_datetime = datetime.datetime(start_year, start_month, start_day, 0, 30)
    end_datetime = datetime.datetime(end_year, end_month, end_day, 0, 0)  # incl.
    start_date = (start_datetime - datetime.timedelta(minutes=30)).date()
    end_date = (end_datetime - datetime.timedelta(minutes=30)).date()

    # read deal position data (pre-sorted) from S3 target folder
    object_key = deal_capture_input_path.format(filename)
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_nbe, Key=object_key)
    data = obj['Body'].read()
    df_all = pd.read_csv(io.BytesIO(data))
    df_all['SettlementDate'] = \
        df_all['SettlementDate'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").date())
    df_all['SettlementDateTime'] = \
        df_all['SettlementDateTime'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M"))
    df_output = pd.DataFrame()
    regions = list(set(df_all['TradingRegion']))
    for region in regions:
        print(region)
        df_1_region = df_all[df_all['TradingRegion'] == region]
        # select the interested period for analysis
        df_1_region = df_1_region[
            (df_1_region['SettlementDate'] >= start_date) & (df_1_region['SettlementDate'] <= end_date)].reset_index(
            drop=True)
        df_transformed = df_1_region.pivot_table(
            ['Premium', 'Hedged Qty (MWh)', 'Weighted Strike Price', 'Notional Quantity MW', 'Weighted Multiplier'],
            ['TradingRegion', 'SettlementDate', 'SettlementDateTime'], 'Type').reset_index()
        df_transformed.columns = ['TradingRegion', 'SettlementDate', 'SettlementDateTime',
                                  'Cap Hedged Qty (MWh)', 'Swap Hedged Qty (MWh)',
                                  'Cap Notional Quantity MW', 'Swap Notional Quantity MW',
                                  'Cap Premium', 'Swap Premium', 'Cap Weighted Multiplier', 'Swap Weighted Multiplier',
                                  'Cap Weighted Strike Price', 'Swap Weighted Strike Price']
        df_transformed = df_transformed.set_index('SettlementDateTime')
        df_transformed = df_transformed.reindex(pd.date_range(start_datetime, end_datetime, freq='30T'))
        df_transformed = df_transformed.reset_index().rename(columns={'index': 'SettlementDateTime'})
        df_transformed['TradingRegion'] = df_transformed['TradingRegion'].fillna(method='ffill')
        df_transformed['SettlementDate'] = df_transformed['SettlementDateTime'].apply(
            lambda x: (x - datetime.timedelta(minutes=30)).date())
        df_transformed = df_transformed.fillna(0)
        df_transformed = df_transformed[['TradingRegion', 'SettlementDate', 'SettlementDateTime',
                                         'Swap Premium', 'Swap Hedged Qty (MWh)', 'Swap Weighted Strike Price',
                                         'Swap Notional Quantity MW', 'Swap Weighted Multiplier',
                                         'Cap Premium', 'Cap Hedged Qty (MWh)', 'Cap Weighted Strike Price',
                                         'Cap Notional Quantity MW', 'Cap Weighted Multiplier']]
        df_output = df_output.append(df_transformed)
    df_output = df_output.reset_index(drop=True)
    end = time.time()
    print('{} seconds.'.format(end - start))
    # df_output.to_excel('output/Deal Capture test3.xlsx', index=False)
    write_pickle_to_s3(df_output, bucket_nbe, deal_capture_converted_path.format(job_id, date_input))


if __name__ == "__main__":
    transform_format(job_id=41,
                     date_input='2021-04-23',
                     filename='DealCapture_SpotRun50015_Job41_2021-04-23_2022-04-23.csv',
                     sheet_name='Position Output',
                     start_year=2021, start_month=1, start_day=1,
                     end_year=2022, end_month=4, end_day=23)
