# TODO: needs to think about the meter data source and input data structure
from utils import read_pickle_from_s3, write_pickle_to_s3
from config import ref_start_date_str, ref_end_date_str, bucket_spot_simulation, public_holiday_path, \
    weather_stations, meter_data_file_path, meter_data_pickle_path, bucket_nbe, weather_data_path
import datetime
import random
import pandas as pd
import numpy as np
import os
import boto3
import io


class DemandProfile:
    hist_start_date = None
    hist_end_date = None
    ref_start_date = None
    ref_end_date = None
    name = None
    region = None
    weather_station = None

    def __init__(self, kwargs):
        self.__dict__.update(kwargs)

    def populate_history(self):
        ref = self.get_all_reference_day()
        reference_dic = {}
        temp_lambda = get_date()
        for row in ref.values:
            key = row[0]
            value = row[1]
            reference_dic[temp_lambda(key)] = list(map(temp_lambda, value))
        ref_dic = dict()
        for key in reference_dic:
            ref_date_index = random.randint(0, len(reference_dic[key]) - 1)
            ref_dic[key] = reference_dic[key][ref_date_index]
        return ref_dic

    def get_all_reference_day(self):
        """
        :return:
        """
        # get the simulated weather data
        weather = self.get_simulations(self.weather_station, self.hist_start_date, self.hist_end_date,
                                       self.ref_start_date, self.ref_end_date)

        ref_history_weather = weather[0]
        simulated_weather = weather[1]
        # simulated maximum temperature
        sim_max_temperature = dict()
        for day in simulated_weather.keys():
            sim_max_temperature[day] = simulated_weather.get(day)[0]
        # simulated minimum temperature
        sim_min_temperature = dict()
        for day in simulated_weather.keys():
            sim_min_temperature[day] = simulated_weather.get(day)[1]
        reference_public_holiday = read_pickle_from_s3(bucket_spot_simulation,
                                                       public_holiday_path.format(self.region))
        # construct the DataFrame of simulation days and their reference days
        sim_days_df = pd.DataFrame(list(sim_max_temperature.keys()),
                                   columns=['Date'])  # create DataFrame using the dates
        sim_days_df['Date'] = pd.to_datetime(sim_days_df['Date'], errors='coerce')
        sim_days_df['Max'] = list(sim_max_temperature.values())
        sim_days_df['BucketNo'] = sim_days_df['Max'].apply(temperature_bucket)
        sim_days_df['Season Type'] = sim_days_df['Date'].apply(season_type)
        sim_days_df['Day Type'] = sim_days_df['Date'].apply(day_type, args=(reference_public_holiday,))
        buckets = get_hist_temp_bucket(ref_history_weather, reference_public_holiday, 'max')

        sim_days_df['Reference Days'] = sim_days_df.apply(find_the_bucket, axis=1, args=(buckets,))

        return sim_days_df.drop(columns=['Max', 'BucketNo', 'Season Type', 'Day Type'])

    def get_df_map(self, df, hist_ref):
        ref_start_date = self.hist_start_date
        ref_end_date = self.hist_end_date
        if isinstance(ref_start_date, str):
            ref_start_date = datetime.datetime.strptime(ref_start_date, "%Y-%m-%d").date()
        elif isinstance(ref_start_date, datetime.datetime):
            ref_start_date = ref_start_date.date()
        if isinstance(ref_end_date, str):
            ref_end_date = datetime.datetime.strptime(ref_end_date, "%Y-%m-%d").date()
        elif isinstance(ref_end_date, datetime.datetime):
            ref_end_date = ref_end_date.date()

        target_start_date = datetime.datetime.strptime(ref_start_date_str, "%Y-%m-%d").date()
        target_end_date = datetime.datetime.strptime(ref_end_date_str, "%Y-%m-%d").date()
        df_dic = {}
        current_date = target_start_date
        for i in range((target_end_date - target_start_date).days + 1):
            if current_date < ref_start_date or current_date >= ref_end_date:
                try:
                    ref_of_ref = hist_ref[current_date]
                    df_dic[current_date] = get_one_day_data(ref_of_ref, df)
                except KeyError:
                    ref_of_ref = hist_ref[target_start_date]
                    df_dic[current_date] = get_one_day_data(ref_of_ref, df)
            else:
                df_dic[current_date] = get_one_day_data(current_date, df)
            current_date += datetime.timedelta(days=1)
        return df_dic

    def check_missing_30_min(self, df):
        df['Half Hour Starting'] = df.apply(self.datestr2datetime, axis=1)
        df = df.drop(columns=['INTERVAL_DATE', 'INTERVAL_NUM'])
        # Check whether there is missing data
        df = df.set_index('Half Hour Starting')
        s = self.hist_start_date
        e = self.hist_end_date
        df = df.reindex(pd.date_range(datetime.datetime(s.year, s.month, s.day, 0, 0),
                                      datetime.datetime(e.year, e.month, e.day, 23, 30), freq='30T'))
        col_name = df.columns.values[0]
        nan_count = df[col_name].isnull().sum()
        print(col_name, 'missing rows: ', nan_count)
        print(df.loc[df[col_name].isnull()])
        # Fill the missing data using the forward fill method.
        df = df.fillna(method='ffill')
        df = df.reset_index().rename(columns={'index': 'Half Hour Starting'})
        return df

    @staticmethod
    def datestr2datetime(row):
        if isinstance(row['INTERVAL_DATE'], str):
            d = datetime.datetime.strptime(row['Date'], "%Y-%m-%d").date()
        elif isinstance(row['INTERVAL_DATE'], datetime.datetime):
            d = row['INTERVAL_DATE'].date()
        p_id = row['INTERVAL_NUM']
        dt = datetime.datetime(d.year, d.month, d.day, int((p_id - 1) / 2), 30 if (p_id % 2) == 0 else 0)
        return dt

    @staticmethod
    def get_simulations(station_name, hist_start_date, hist_end_date, ref_start_date, ref_end_date):
        weather_data = read_pickle_from_s3(bucket_spot_simulation, weather_data_path.format(station_name))
        hist_weather_data = dict()
        for k, v in weather_data.items():
            if (k >= hist_start_date) & (k < hist_end_date):
                hist_weather_data[k] = v
        sim_weather = dict()  # now it's actual weather
        for k, v in weather_data.items():
            if (k >= ref_start_date) & (k < ref_end_date):
                sim_weather[k] = v
        print("Weather \'simulation\' finished. Weather station: {}. {} ~ {}".format(station_name,
                                                                                     ref_start_date,
                                                                                     ref_end_date))
        return hist_weather_data, sim_weather


def find_the_bucket(row, *args):
    """
    For each day in the simulated period, pick a like day in the historical period based on:
    season type, day type, and temperature. Return a reference day.
    :param row: list of all columns elements of each row
    :param args: the temperature bucket
    :return: Timestamp of the reference day
    """
    buckets = args[0]
    bucket_no = row['BucketNo']
    season = row['Season Type']
    day = row['Day Type']
    seasons_list = ['Winter', 'Summer', 'Shoulder']
    day_type_list = [1, 2, 7]

    for s, season_item in enumerate(seasons_list):
        for d, day_item in enumerate(day_type_list):
            for t in range(10):
                try:
                    if season == season_item and day == day_item and bucket_no == t:
                        if not buckets[t, s, d]:
                            raise IndexError
                        return buckets[t, s, d]
                except IndexError:
                    try:
                        # find closest temperature bucket with data
                        (delta_i, p) = (1, 1) if t <= 4 else (-1, -1)
                        while not buckets[t + delta_i, s, d]:
                            delta_i += p
                    except IndexError:
                        print(s, d, t)
                    return buckets[t + delta_i, s, d]


def get_date():
    return lambda dt: dt.date()


def get_one_day_data(date, df):
    dt_s = datetime.datetime(date.year, date.month, date.day, 0, 0)
    dt_e = datetime.datetime(date.year, date.month, date.day, 23, 30)
    new_data = df[(df['Half Hour Starting'] >= dt_s) & (df['Half Hour Starting'] <= dt_e)].reset_index(drop=True)
    return new_data


def get_max_temperature(hist_weather):
    """
    get the maximum temperature and store in a dictionary
    :return: dictionary, key:date; value: maximum temperature
    """
    max_temperature = dict()
    for day in hist_weather.keys():
        max_temperature[day] = hist_weather.get(day)[0]
    # print(max_temperature)
    return max_temperature


def get_min_temperature(hist_weather):
    """
    get the minimum temperature and store in a dictionary
    :return: dictionary, key:date; value: minimum temperature
    """
    min_temperature = dict()
    for day in hist_weather.keys():
        min_temperature[day] = hist_weather.get(day)[1]
    # print(min_temperature)
    return min_temperature


def season_type(datetime_date):
    """
    Given a timestamp, get the season type of it
    :param datetime_date: <class 'pandas._libs.tslibs.timestamps.Timestamp'>
    :return: str
    """
    month = datetime_date.month
    if month in (1, 2, 3, 11, 12):
        return "Summer"
    if month in (5, 6, 7, 8):
        return "Winter"
    if month in (4, 9, 10):
        return "Shoulder"


def day_type(datetime_date, *args):
    """
    Given a timestamp, get the day type of it:
    1: Sunday/Public Holiday; 2: Working Week Day; 7: Saturday
    :param datetime_date: <class 'pandas._libs.tslibs.timestamps.Timestamp'>
    :param args: arguments tuple passed from pandas.apply()
    :return: int
    """
    day_in_a_week = datetime_date.weekday()
    public_holiday = args[0]
    # return day type
    if str(datetime_date.date()) in public_holiday or day_in_a_week == 6:
        return 1
    if day_in_a_week in (0, 1, 2, 3, 4):
        return 2
    if day_in_a_week == 5:
        return 7


def temperature_bucket(temperature):
    """
    Given a temperature value, get the temperature bucket number.
    If the temperature is less than 0, use the 0~5 bucket, if the temperature is greater than 50, use the 45~50 bucket.
    :param temperature: temperature values, float
    :return: bucket number 1~10, int
    """
    if temperature >= 50:
        bucket_no = int(45 / 5)
    elif temperature < 0:
        bucket_no = int(0 / 5)
    else:
        bucket_no = int(temperature / 5)
    return bucket_no


def get_hist_temp_bucket(historical_weather, reference_public_holiday, max_or_min):
    """
    Get the maximum or minimum historical temperature-season-day bucket with a list of dates in each bucket
    :param historical_weather: dictionary
    :param reference_public_holiday: list of datetime objects
    :param max_or_min: String. 'max' or 'min'
    :return: a 3-dimensional ndarray, each dimension is a bucket and contains a list of dates (in Timestamp format)
    If there's no corresponding data in that bucket, the list will be an empty list.
    Each dimension represents: temperature bucket, season type, day type
    """
    if max_or_min == 'max':
        temperature_data = get_max_temperature(historical_weather)
    elif max_or_min == 'min':
        temperature_data = get_min_temperature(historical_weather)

    days_df = pd.DataFrame(list(temperature_data.keys()), columns=['Date'])  # create DataFrame using the dates
    days_df['Date'] = pd.to_datetime(days_df['Date'], errors='coerce')
    days_df['Temperature'] = list(temperature_data.values())  # add a new column of temperature data
    days_df['Season Type'] = days_df['Date'].apply(season_type)
    days_df['Day Type'] = days_df['Date'].apply(day_type,
                                                args=(
                                                    reference_public_holiday,))  # np.vectorize() would be 2nd option
    days_df['Temperature Bucket'] = days_df['Temperature'].apply(temperature_bucket)
    # days_df.to_csv('hist_max_bucket.csv', sep='\t', encoding='utf-8')
    # print(days_df)
    hist_temp_bucket = np.ndarray(shape=(10, 3, 3), dtype=datetime.date)  # temperature, season, day
    bucket = days_df.groupby(['Season Type', 'Day Type', 'Temperature Bucket'])
    seasons_list = ['Winter', 'Summer', 'Shoulder']
    day_type_list = [1, 2, 7]
    for i, season in enumerate(seasons_list):
        for j, day in enumerate(day_type_list):
            for temperature in range(10):
                try:
                    hist_temp_bucket[temperature, i, j] = bucket.get_group((season, day, temperature))[
                        'Date'].tolist()
                except KeyError:
                    hist_temp_bucket[temperature, i, j] = []
                    print("Bucket '{} {} {}' has no corresponding values, assign it an empty list."
                          .format(season, day, temperature))

    return hist_temp_bucket


def main_process(key_name):
    # input_path = '../input/'
    # filename = 'demand_profile_NBE.xlsx'
    # input_df = pd.read_excel(os.path.join(input_path, filename))
    # read deal position data (pre-sorted) from S3 target folder
    filename = key_name.split('/')[1]
    object_key = meter_data_file_path.format(filename)
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_nbe, Key=object_key)
    data = obj['Body'].read()
    input_df = pd.read_excel(io.BytesIO(data))
    demand_entity_list = list(set(input_df['Distributor']))
    # process data for each distributor
    for distributor in demand_entity_list:
        distributor_df = input_df[input_df['Distributor'] == distributor].reset_index(drop=True).sort_values(
            by=['INTERVAL_DATE', 'INTERVAL_NUM'])
        kwargs = {
            'hist_start_date': distributor_df['INTERVAL_DATE'].iloc[0].date(),
            'hist_end_date': distributor_df['INTERVAL_DATE'].iloc[-1].date(),
            'ref_start_date': datetime.datetime.strptime(ref_start_date_str, "%Y-%m-%d").date(),
            'ref_end_date': datetime.datetime.strptime(ref_end_date_str, "%Y-%m-%d").date(),
            'name': distributor,
            # the naming convention of regions, i.e. VIC1
            'region': distributor_df['STATE'].iloc[0] if distributor_df['STATE'].iloc[0].endswith('1') else
            distributor_df['STATE'].iloc[0] + '1'
        }
        demand_profile = DemandProfile(kwargs)
        demand_profile.region = 'NSW1' if demand_profile.region == 'ACT1' else demand_profile.region
        demand_profile.weather_station = weather_stations[demand_profile.region]
        distributor_df = distributor_df.drop(columns=['STATE', 'Distributor'])

        cleaned_data = demand_profile.check_missing_30_min(distributor_df)
        populated_ref = demand_profile.populate_history()
        populated_data = demand_profile.get_df_map(cleaned_data, populated_ref)
        write_pickle_to_s3(populated_data, bucket_nbe, meter_data_pickle_path.format(demand_profile.region,
                                                                                     demand_profile.name))
        print(demand_profile.name)


if __name__ == '__main__':
    # key_name = event['Records'][0]['s3']['object']['key']
    # e.g. key = 'meter_data_input/demand_profile_NBE.xlsx'
    key = 'meter_data_input/demand_profile_NBE.xlsx'
    print(key)
    main_process(key)
