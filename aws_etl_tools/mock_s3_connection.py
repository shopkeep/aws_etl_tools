from functools import wraps
from importlib import reload

import boto
from moto import mock_s3

from aws_etl_tools import config


class MockS3Connection:
    def __init__(self, bucket=None):
        self.bucket = bucket or s3_bucket_name_from_config()

    def __call__(self, function):
        @wraps(function)
        def with_mock_s3_connection(*args, **kwargs):
            mock = mock_s3()
            mock.start()
            s3_connection = boto.connect_s3()
            s3_connection.create_bucket(self.bucket)
            return function(*args, **kwargs)

        return with_mock_s3_connection


def s3_bucket_name_from_config():
    s3_base_path = reload(config).S3_BASE_PATH
    bucket_name = s3_base_path.replace('s3://', '').split('/')[0]
    return bucket_name
