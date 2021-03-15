import boto3
import pickle
from datetime import datetime, date


client = boto3.client('s3')


def read_pickle_from_s3(bucket, path):
    pickle_data = client.get_object(Bucket=bucket, Key=path)
    return pickle.loads(pickle_data['Body'].read())


def write_pickle_to_s3(data, bucket, path):
    pickle_data = pickle.dumps(data)
    client.put_object(Bucket=bucket, Body=pickle_data, Key=path)


def put_object_to_s3(binary_data, bucket, key):
    client.put_object(Body=binary_data,
                      Bucket=bucket,
                      Key=key)


def datestr2date(dstr):
    """
    Convert string to datetime
    :type dstr: string
    :return: datetime
    """
    return datetime.strptime(dstr, "%Y-%m-%d").date()


def str2date(dstr):
    """
    Convert string to datetime
    :type dstr: string
    :return: datetime
    """
    return datetime.strptime(dstr, "%Y-%m-%d %H:%M:%S").date()


def date2num(dtime):
    """
    Covert datetime to number
    :type dtime: datetime
    :rtype: int
    """
    return date.toordinal(dtime)


def test_func():
    pass
