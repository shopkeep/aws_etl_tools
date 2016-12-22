import csv
from importlib import reload
import json
import os
import unittest
from unittest.mock import Mock

from tests import test_helper
from aws_etl_tools.mock_s3_connection import MockS3Connection
from aws_etl_tools import config
from aws_etl_tools.s3_file import S3File, S3RelativeFilePath, upload_local_file_to_s3_path
from aws_etl_tools.exceptions import NoDataFoundError, NoS3BasePathError


class TestS3FileFromFullPath(unittest.TestCase):

    S3_BUCKET_NAME = 'test-s3-bucket'
    S3_KEY_NAME = 'ye/test/key.csv'
    S3_FILE_NAME = S3_KEY_NAME.split('/')[-1]
    S3_PATH = 's3://' + S3_BUCKET_NAME + '/' + S3_KEY_NAME
    IN_MEMORY_DATA = [('1', 'first_string'), ('2', 'second_string'), ('3', 'third_string')]

    S3_FILE_UPLOAD_PATH = os.path.join(config.LOCAL_TEMP_DIRECTORY, 'test_file_upload.csv')
    S3_FILE_DOWNLOAD_PATH = os.path.join(config.LOCAL_TEMP_DIRECTORY, 'test_file_download.csv')
    S3_FILE_CONTENTS = '1,first_string\n2,second_string\n3,third_string\n'

    @staticmethod
    def _read_csv_as_list_of_tuples(local_csv_path):
        list_of_tuples = []
        with open(local_csv_path, 'r') as downloaded_file:
            reader = csv.reader(downloaded_file)
            for row in reader:
                list_of_tuples.append(tuple(row))
        return list_of_tuples

    def setUp(self):
        with open(self.S3_FILE_UPLOAD_PATH, 'w') as test_file:
            test_file.write(self.S3_FILE_CONTENTS)

    def tearDown(self):
        test_helper.clear_temp_directory()

    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_s3_file_upload_and_download(self):
        s3_file = S3File.from_local_file(
            local_path=self.S3_FILE_UPLOAD_PATH,
            s3_path=self.S3_PATH
        )
        s3_file.download(destination_path=self.S3_FILE_DOWNLOAD_PATH)
        with open(self.S3_FILE_DOWNLOAD_PATH, 'r') as downloaded_file:
            self.assertEqual(downloaded_file.read(), self.S3_FILE_CONTENTS)

    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_s3_file_factory(self):
        s3_file = S3File.from_local_file(
            local_path=self.S3_FILE_UPLOAD_PATH,
            s3_path=self.S3_PATH
        )
        self.assertIsInstance(s3_file, S3File)

    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_s3_file_from_in_memory_data_upload_and_download(self):
        s3_file = S3File.from_in_memory_data(
            data=self.IN_MEMORY_DATA,
            s3_path=self.S3_PATH
        )
        s3_file.download(destination_path=self.S3_FILE_DOWNLOAD_PATH)

        data_uploaded_to_s3 = self._read_csv_as_list_of_tuples(self.S3_FILE_DOWNLOAD_PATH)
        self.assertEqual(data_uploaded_to_s3, self.IN_MEMORY_DATA)

    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_s3_file_from_in_memory_data_factory(self):
        s3_file = S3File.from_in_memory_data(
            data=self.IN_MEMORY_DATA,
            s3_path=self.S3_PATH
        )
        self.assertIsInstance(s3_file, S3File)

    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_s3_file_size(self):
        s3_file = S3File.from_local_file(
            local_path=self.S3_FILE_UPLOAD_PATH,
            s3_path=self.S3_PATH
        )
        expected_file_size = os.path.getsize(self.S3_FILE_UPLOAD_PATH)
        self.assertEqual(s3_file.file_size, expected_file_size)

    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_upload_empty_file_raises_error(self):
        local_path_with_empty_file = os.path.join(config.LOCAL_TEMP_DIRECTORY, 'empty_test_file.csv')
        with open(local_path_with_empty_file, 'w') as empty_file:
            empty_file.write('')

        self.assertRaises(
            NoDataFoundError,
            S3File.from_local_file,
            local_path_with_empty_file,
            self.S3_PATH
        )

    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_file_size_of_non_existent_file_equals_0(self):
        self.assertEqual(S3File(self.S3_PATH).file_size, 0)

    def test_s3_bucket_name(self):
        s3_file = S3File(self.S3_PATH)
        self.assertEqual(s3_file.bucket_name, self.S3_BUCKET_NAME)

    def test_s3_key_name(self):
        s3_file = S3File(self.S3_PATH)
        self.assertEqual(s3_file.key_name, self.S3_KEY_NAME)


class TestS3RelativeFilePath(unittest.TestCase):

    CUSTOM_BASE_PATH = 's3://exciting-bucket/of'
    SUB_PATH = 'louisiana/fried/chicken'
    EXPECTED_S3_PATH = 's3://exciting-bucket/of/louisiana/fried/chicken'

    def test_s3_path_with_s3_base_path_works_as_expected(self):
        config.S3_BASE_PATH = self.CUSTOM_BASE_PATH

        actual_s3_path = S3RelativeFilePath(self.SUB_PATH).s3_path

        self.assertEqual(self.EXPECTED_S3_PATH, actual_s3_path)


    def test_s3_path_without_s3_base_path_raises(self):
        config.S3_BASE_PATH = None

        with self.assertRaises(NoS3BasePathError):
            actual_s3_path = S3RelativeFilePath(self.SUB_PATH).s3_path


class TestS3FileFromPathObject(unittest.TestCase):

    S3_BUCKET_NAME = 'test-s3-bucket'
    KEY_NAME = 'v1/today/results.csv'
    S3_PATH = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET_NAME, key=KEY_NAME)
    PATH_OBJECT = Mock(s3_path=S3_PATH)
    S3_FILE_UPLOAD_PATH = os.path.join(config.LOCAL_TEMP_DIRECTORY, 'test_file_upload.csv')
    S3_FILE_DOWNLOAD_PATH = os.path.join(config.LOCAL_TEMP_DIRECTORY, 'test_file_download.csv')
    S3_FILE_CONTENTS = '1,first_string\n2,second_string\n3,third_string\n'

    def setUp(self):
        with open(self.S3_FILE_UPLOAD_PATH, 'w') as test_file:
            test_file.write(self.S3_FILE_CONTENTS)

    def tearDown(self):
        test_helper.clear_temp_directory()

    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_s3_file_upload_and_download_with_path_object(self):
        s3_file = S3File.from_local_file(
            local_path=self.S3_FILE_UPLOAD_PATH,
            s3_path=self.PATH_OBJECT
        )
        s3_file.download(destination_path=self.S3_FILE_DOWNLOAD_PATH)

        with open(self.S3_FILE_DOWNLOAD_PATH, 'r') as downloaded_file:
            self.assertEqual(downloaded_file.read(), self.S3_FILE_CONTENTS)


    def test_init_with_path_object_sets_bucket(self):
        s3_file = S3File(s3_path=self.PATH_OBJECT)

        self.assertEqual(s3_file.bucket_name, self.S3_BUCKET_NAME)


    def test_init_with_path_object_sets_key(self):
        s3_file = S3File(s3_path=self.PATH_OBJECT)

        self.assertEqual(s3_file.key_name, self.KEY_NAME)


class TestS3JsonFileFromDict(unittest.TestCase):

    S3_BUCKET_NAME = 'test-s3-bucket'
    S3_PATH = 's3://{bucket}/namespace/configuration.json'.format(bucket=S3_BUCKET_NAME)
    DATA = {'foo': 'bar', 'baz': 6, 'jim': False}

    def tearDown(self):
        test_helper.clear_temp_directory()

    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_dictionary_becomes_json_file_in_s3(self):
        file = S3File.from_json_serializable(data=self.DATA, s3_path=self.S3_PATH)

        temp_path = file.download_to_temp()
        with open(temp_path) as file:
            actual_data = json.load(file)
        self.assertEqual(actual_data, self.DATA)
