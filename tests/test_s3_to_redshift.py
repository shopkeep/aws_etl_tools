import csv
import os
import unittest
from datetime import datetime
from unittest.mock import Mock, patch, PropertyMock, ANY

from freezegun import freeze_time
from psycopg2 import NotSupportedError

from aws_etl_tools import config
from aws_etl_tools.mock_s3_connection import MockS3Connection
from aws_etl_tools.redshift_ingest import s3_to_redshift, RedshiftTable
from aws_etl_tools.s3_file import S3File
from aws_etl_tools.redshift_database import RedshiftDatabase
from tests import test_helper


class TestS3ToRedshift(unittest.TestCase):

    S3_BUCKET_NAME = 'fake-s3-bucket'
    S3_KEY_NAME = 'ye/test/file.csv'
    S3_PATH = 's3://' + S3_BUCKET_NAME + '/' + S3_KEY_NAME

    DB_CONNECTION = test_helper.BasicRedshiftButActuallyPostgres()
    UPSERT_UNIQUENESS_KEY = ('col_with_ints',)
    LOCAL_FILE_PATH = os.path.join(config.LOCAL_TEMP_DIRECTORY, 'output_file.csv')
    TABLE = 'public.s3_csv_data'
    AUDIT_TABLE = config.REDSHIFT_INGEST_AUDIT_TABLE
    DB_SELECT_ALL_QUERY = """SELECT col_with_ints, col_with_strs FROM %s""" % TABLE
    FILE_CONTENTS = [(1, 'first_string'), (2, 'second_string'), (3, 'third_string')]

    FROZEN_TIME = '2015-11-30 22:36:43.421342'
    AUDIT_TABLE_CONTENTS_QUERY = """SELECT * FROM %s""" % AUDIT_TABLE
    EXPECTED_AUDIT_DATA = [
        (ANY,
        datetime(2015, 11, 30, 22, 36, 43, 421342),
        'public',
        's3_csv_data',
        '{}')
    ]

    def setUp(self):
        self.DB_CONNECTION.execute("""CREATE TABLE %s (
                                      col_with_ints integer PRIMARY KEY,
                                      col_with_strs varchar(20))""" % self.TABLE
                                   )
        with open(self.LOCAL_FILE_PATH, 'w') as f:
            writer = csv.writer(f)
            for row in self.FILE_CONTENTS:
                writer.writerow(row)

    def tearDown(self):
        self.DB_CONNECTION.execute("""DROP TABLE %s""" % self.TABLE)
        self.DB_CONNECTION.execute("""TRUNCATE %s""" % self.AUDIT_TABLE)
        test_helper.clear_temp_directory()


    def test_data_not_in_redshift(self):
        current_data_in_table = self.DB_CONNECTION.fetch(self.DB_SELECT_ALL_QUERY)
        self.assertEqual(current_data_in_table, [])


    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_data_in_redshift(self):
        '''Because our destination database, `BasicRedshiftButActuallyPostgres`, is only pretending
        to be a Redshift database, the s3_to_redshift method should successfully
        move a local .csv file to it.'''

        s3_file = S3File.from_local_file(
            local_path=self.LOCAL_FILE_PATH,
            s3_path=self.S3_PATH
        )

        s3_to_redshift(s3_file, RedshiftTable(self.DB_CONNECTION, self.TABLE, self.UPSERT_UNIQUENESS_KEY))

        current_data_in_table = self.DB_CONNECTION.fetch(self.DB_SELECT_ALL_QUERY)
        self.assertEqual(current_data_in_table, self.FILE_CONTENTS)


    @freeze_time(FROZEN_TIME)
    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_upsert_audit(self):
        s3_file = S3File.from_local_file(
            local_path=self.LOCAL_FILE_PATH,
            s3_path=self.S3_PATH
        )

        s3_to_redshift(s3_file, RedshiftTable(self.DB_CONNECTION, self.TABLE, self.UPSERT_UNIQUENESS_KEY))

        recorded_audit_data = self.DB_CONNECTION.fetch(self.AUDIT_TABLE_CONTENTS_QUERY)
        self.assertEqual(recorded_audit_data, self.EXPECTED_AUDIT_DATA)


    @MockS3Connection(bucket=S3_BUCKET_NAME)
    @patch.object(RedshiftDatabase, 'execute')
    def test_vacuum_errors_are_swallowed(self, database_execute):
        database_execute.side_effect = [
            'pre_upsert_audit_table_insert_statement',
            'post_upsert_audit_table_update_statement',
            NotSupportedError("VACUUM is running. HINT: re-execute after other vacuum finished")
        ]

        s3_file = S3File.from_local_file(
            local_path=self.LOCAL_FILE_PATH,
            s3_path=self.S3_PATH
        )

        try:
            s3_to_redshift(s3_file, RedshiftTable(self.DB_CONNECTION, self.TABLE, self.UPSERT_UNIQUENESS_KEY))
        except BaseException:
            self.fail('nothing should have errored here unexpectedly!')
