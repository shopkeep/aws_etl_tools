from functools import wraps
from importlib import reload

import boto3
from moto import mock_s3

from aws_etl_tools import config
from aws_etl_tools.guard import requires_s3_base_path


class MockS3Connection:
    '''This is a decorator for mocking a connection to S3 for the life of a test. You can use it
        in two ways: with and without a bucket. If a bucket is not specified, then at import time,
        the decorator needs to know what bucket to mock, so it will pull it out of the config. So
        there is coupling here. The easiest way to use this is to set the bucket in the initialization
        of the decorator. If you want to be lazy on naming a bucket, then you'll
        have to set an S3_BASE_PATH in the config.
    '''

    def __init__(self, bucket=None):
        self.bucket = bucket or s3_bucket_name_from_config()

    def __call__(self, function):
        @wraps(function)
        @mock_s3()
        def with_mock_s3_connection(*args, **kwargs):
            s3_connection = boto3.resource('s3')
            s3_connection.create_bucket(Bucket=self.bucket)
            return function(*args, **kwargs)

        return with_mock_s3_connection


@requires_s3_base_path
def s3_bucket_name_from_config():
    s3_base_path = reload(config).S3_BASE_PATH
    bucket_name = s3_base_path.replace('s3://', '').split('/')[0]
    return bucket_name
