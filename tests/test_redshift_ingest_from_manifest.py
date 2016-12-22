import os
import csv
import unittest
from unittest.mock import patch, Mock

import boto
from freezegun import freeze_time

from aws_etl_tools.mock_s3_connection import MockS3Connection
from aws_etl_tools.redshift_ingest import *
from tests import test_helper


class TestRedshiftIngestManifest(unittest.TestCase):

    FROZEN_TIME = '2015-11-30 22:36:43.421342'
    S3_BUCKET_NAME = test_helper.S3_TEST_BUCKET_NAME
    SCHEMA = 'public'
    TABLENAME = 'test_channels'
    TARGET_TABLE = '{schema}.{table}'.format(schema=SCHEMA, table=TABLENAME)
    TARGET_DATABASE = test_helper.UnloadableRedshift()
    EXPECTED_S3_MANIFEST_PATH = 's3://{bucket}/{db_name}/{schema}/{table}_{timestamp}.manifest'.format(
        bucket=S3_BUCKET_NAME,
        db_name='unloadableredshift',
        schema=SCHEMA,
        table=TABLENAME,
        timestamp='2015_11_30_22_36_43'
    )
    test_helper.set_default_s3_base_path()

    def tearDown(self):
        test_helper.clear_temp_directory()

    def _build_working_manifest(self):
        # upload a file with some data to s3
        file_contents = '5,funzies\n7,sadzies\n'
        s3_bucket_name = test_helper.S3_TEST_BUCKET_NAME
        s3_key_name = 'namespaced/file/data.csv'
        s3_bucket = boto.connect_s3().get_bucket(s3_bucket_name)
        s3_bucket.new_key(s3_key_name).set_contents_from_string(file_contents)
        full_s3_path = 's3://' + s3_bucket_name + '/' + s3_key_name
        # create a manifest that includes that file
        return { "entries": [{"url": full_s3_path, "mandatory": True}] }


    @freeze_time(FROZEN_TIME)
    @MockS3Connection(bucket=S3_BUCKET_NAME)
    def test_manifest_to_redshift_with_mocked_ingestor_gets_called_correctly(self):
        manifest = self._build_working_manifest()
        destination = RedshiftTable(
            database=self.TARGET_DATABASE,
            target_table=self.TARGET_TABLE,
            upsert_uniqueness_key=('id',)
        )

        from_manifest(manifest, destination)

        self.TARGET_DATABASE.ingestion_class.assert_called_once_with(
            self.EXPECTED_S3_MANIFEST_PATH,
            destination,
            with_manifest=True
        )
        self.TARGET_DATABASE.ingestor.assert_called_once_with()


