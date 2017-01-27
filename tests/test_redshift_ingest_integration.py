import os
import csv
import unittest
from importlib import reload

import pandas as pd
import boto3

from aws_etl_tools.mock_s3_connection import MockS3Connection
from aws_etl_tools import config
from aws_etl_tools.redshift_ingest import *
from tests import test_helper


class TestRedshiftIngestIntegration(unittest.TestCase):

    TARGET_TABLE = 'public.testing_channels'
    AUDIT_TABLE = config.REDSHIFT_INGEST_AUDIT_TABLE
    EXPECTED_COUNT_OF_AUDIT_FIELDS = 5
    TARGET_DATABASE = test_helper.BasicRedshiftButActuallyPostgres()
    DESTINATION = RedshiftTable(
        database=TARGET_DATABASE,
        target_table=TARGET_TABLE,
        upsert_uniqueness_key=('id',)
    )

    def setUp(self):
        self.TARGET_DATABASE.execute("""
            CREATE TABLE %s (
                id integer PRIMARY KEY,
                value varchar(20)
            )""" % self.TARGET_TABLE
        )
        test_helper.set_default_s3_base_path()


    def tearDown(self):
        self.TARGET_DATABASE.execute("""DROP TABLE %s""" % self.TARGET_TABLE)
        self.TARGET_DATABASE.execute("""TRUNCATE TABLE %s""" % self.AUDIT_TABLE)
        test_helper.clear_temp_directory()

    def assert_data_in_target(self):
        actual_target_data = self.TARGET_DATABASE.fetch("""select * from {0}""".format(self.TARGET_TABLE))
        expected_target_data = [(5, 'funzies'), (7, 'sadzies')]
        self.assertEqual(actual_target_data, expected_target_data)

    def assert_audit_row_created(self):
        audit_row = self.TARGET_DATABASE.fetch("""select * from {0} order by loaded_at desc""".format(self.AUDIT_TABLE))[0]
        actual_count_of_audit_fields = len(audit_row)
        self.assertEqual(actual_count_of_audit_fields, self.EXPECTED_COUNT_OF_AUDIT_FIELDS)


    @MockS3Connection()
    def test_in_memory_data_to_redshift(self):
        source_data = [[5, 'funzies'], [7, 'sadzies']]

        from_in_memory(source_data, self.DESTINATION)

        self.assert_data_in_target()
        self.assert_audit_row_created()


    @MockS3Connection()
    def test_dataframe_to_redshift(self):
        source_dataframe = pd.DataFrame(
            [(5, 'funzies'), (7, 'sadzies')],
            columns=['one', 'two'],
            index=['alpha', 'beta']
        )

        from_dataframe(source_dataframe, self.DESTINATION)

        self.assert_data_in_target()
        self.assert_audit_row_created()


    @MockS3Connection()
    def test_postgres_query_to_redshift(self):
        source_db = test_helper.BasicPostgres()
        source_query = """
            SELECT 5 AS number, 'funzies' AS category
            UNION SELECT 7, 'sadzies'
        """

        from_postgres_query(source_db, source_query, self.DESTINATION)

        self.assert_data_in_target()
        self.assert_audit_row_created()


    @MockS3Connection()
    def test_local_file_to_redshift(self):
        file_contents = '5,funzies\n7,sadzies\n'
        file_path = os.path.join(config.LOCAL_TEMP_DIRECTORY, 'csv_data.csv')
        with open(file_path, 'w') as file_writer:
            file_writer.write(file_contents)

        from_local_file(file_path, self.DESTINATION)

        self.assert_data_in_target()
        self.assert_audit_row_created()


    @MockS3Connection()
    def test_s3_path_to_redshift(self):
        file_contents = '5,funzies\n7,sadzies\n'
        s3_bucket_name = test_helper.S3_TEST_BUCKET_NAME
        s3_key_name = 'namespaced/file/here.csv'
        s3 = boto3.resource('s3')
        s3.Object(s3_bucket_name, s3_key_name).put(Body=file_contents)

        full_s3_path = 's3://' + s3_bucket_name + '/' + s3_key_name
        from_s3_path(full_s3_path, self.DESTINATION)

        self.assert_data_in_target()
        self.assert_audit_row_created()


    @MockS3Connection()
    def test_manifest_to_redshift_raises_value_error(self):
        '''This cannot be integration tested because Postgres cannot trivially
            be made to handle manifest files.'''
        expected_exception_args = ('Postgres cannot handle manifests like redshift. Sorry.',)
        manifest_dict = { "entries": [{"url": 's3://this/doesnt/matter.manifest', "mandatory": True}] }

        with self.assertRaises(ValueError) as exception_context_manager:
            from_manifest(manifest_dict, self.DESTINATION)

        self.assertEqual(exception_context_manager.exception.args, expected_exception_args)
