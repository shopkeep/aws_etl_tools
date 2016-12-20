import csv
import json
import os

from boto.s3.key import Key

from aws_etl_tools.aws import AWS
from aws_etl_tools import config
from aws_etl_tools.exceptions import NoDataFoundError


def parse_s3_path(s3_path):
    s3_path_elements = [string for string in s3_path.split('/') if len(string) > 0]
    bucket_name = s3_path_elements[1]
    key_name = '/'.join(s3_path_elements[2:])
    file_name = s3_path_elements[-1]
    return bucket_name, key_name, file_name

def upload_local_file_to_s3_path(local_path, s3_path):
    bucket_name, key_name, _ = parse_s3_path(s3_path)
    s3_bucket = AWS().s3_connection().get_bucket(bucket_name)
    s3_key_object = Key(s3_bucket)
    s3_key_object.key = key_name
    s3_key_object.set_contents_from_filename(local_path)
    if s3_key_object.size == 0:
        raise NoDataFoundError('The file you\'ve uploaded to S3 has a size of 0 KB')

def upload_data_to_s3_path(data, s3_path):
    ''' takes some data, writes it locally to a CSV, and then uploads that to s3.
    `data`: a simple iterable of iterables: e.g. a list of tuples
    `s3_path`: a full s3_path: e.g. s3://ye-olde-bucket/namespace/data.csv'''
    _, _, file_name = parse_s3_path(s3_path)
    local_path = os.path.join(config.LOCAL_TEMP_DIRECTORY, file_name)
    _write_data_to_local_csv(data, local_path)
    return upload_local_file_to_s3_path(local_path, s3_path)

def download_from_s3_to_local_file(s3_path, local_path):
    bucket_name, key_name, file_name = parse_s3_path(s3_path)
    s3_bucket = AWS().s3_connection().get_bucket(bucket_name)
    s3_key_object = Key(s3_bucket)
    s3_key_object.key = key_name
    s3_key_object.get_contents_to_filename(local_path)

def _write_data_to_local_csv(data, local_path):
    with open(local_path, 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for row in data:
            writer.writerow(row)


class S3File:
    '''An abstraction for files that exist in S3. The parameter s3_path
    is either the string of the path (e.g. 's3://your_bucket/namespace/file.txt')
    or an object with a property `s3_path` which looks like the above.'''

    def __init__(self, s3_path):
        self.s3_path = self._disambiguate_s3_path(s3_path)
        self.bucket_name, self.key_name, self.file_name = parse_s3_path(self.s3_path)

    @property
    def file_size(self):
        s3_bucket = AWS().s3_connection().get_bucket(self.bucket_name)
        s3_key = s3_bucket.get_key(self.key_name)
        return s3_key.size if s3_key else 0

    def download(self, destination_path):
        download_from_s3_to_local_file(self.s3_path, destination_path)

    def download_to_temp(self):
        destination_path = os.path.join(config.LOCAL_TEMP_DIRECTORY, 's3_download_' + self.file_name)
        self.download(destination_path)
        return destination_path

    @classmethod
    def from_json_serializable(cls, data, s3_path):
        '''Serialize a dict to json and upload it to s3.'''
        s3_path = cls._disambiguate_s3_path(s3_path)
        _, _, file_name = parse_s3_path(s3_path)
        local_file_path = os.path.join(config.LOCAL_TEMP_DIRECTORY, 's3_upload_dict_' + file_name)
        with open(local_file_path, 'w') as json_file:
            json.dump(data, json_file)
        upload_local_file_to_s3_path(local_file_path, s3_path)
        return cls(s3_path)

    @classmethod
    def from_in_memory_data(cls, data, s3_path):
        '''Given some data, write it to a CSV in s3 and return an S3File abstraction.
           `data`: a simple iterable of iterables: e.g. a list of tuples
           `s3_path`: a full, partial, or relative s3_path'''
        s3_path = cls._disambiguate_s3_path(s3_path)
        upload_data_to_s3_path(data, s3_path)
        return cls(s3_path)

    @classmethod
    def from_local_file(cls, local_path, s3_path):
        s3_path = cls._disambiguate_s3_path(s3_path)
        upload_local_file_to_s3_path(local_path, s3_path)
        return cls(s3_path)

    @staticmethod
    def _disambiguate_s3_path(path):
        if isinstance(path, str):
            if path.startswith('s3://'):
                return path
            else:
                return S3RelativeFilePath(path).s3_path
        else:
            return path.s3_path


class S3RelativeFilePath:
    '''An abstraction of s3_paths. By standardizing the base_path and passing
    one of these objects into S3File, you can get some guarantees around
    standardization of your s3 interactions and file locations.'''

    def __init__(self, sub_path):
        self.sub_path = sub_path

    @property
    def base_path(self):
        return config.S3_BASE_PATH

    @property
    def s3_path(self):
        return os.path.join(self.base_path, self.sub_path)
