import os
import unittest

from tests import test_helper
from unittest.mock import patch, Mock
from aws_etl_tools.redshift_database import RedshiftDatabase
from aws_etl_tools.aws import AWS


class TestRedshiftDatabase(unittest.TestCase):

    REDSHIFT_DATABASE = test_helper.BasicRedshift()
    S3_BUCKET = 'useful-things-bucket'
    S3_KEY = 'klaatu/barada/nikto/test_s3_upload_file.csv'
    S3_PATH = 's3://' + S3_BUCKET + '/' + S3_KEY
    DOWNLOAD_QUERY = "SELECT * FROM funzies"
    AWS_CONNECTION_CREDENTIALS = 'faux_aws_credentials'

    EXPECTED_PARALLEL_UNLOAD = "UNLOAD ('SELECT * FROM funzies') TO 's3://useful-things-bucket/klaatu/barada/nikto/test_s3_upload_file.csv' CREDENTIALS 'faux_aws_credentials' MANIFEST DELIMITER '|';"
    EXPECTED_SINGLE_UNLOAD = "UNLOAD ('SELECT * FROM funzies') TO 's3://useful-things-bucket/klaatu/barada/nikto/test_s3_upload_file.csv' CREDENTIALS 'faux_aws_credentials' PARALLEL OFF DELIMITER '|';"
    EXPECTED_SINGLE_UNLOAD_WITH_OVERWRITE = "UNLOAD ('SELECT * FROM funzies') TO 's3://useful-things-bucket/klaatu/barada/nikto/test_s3_upload_file.csv' CREDENTIALS 'faux_aws_credentials' PARALLEL OFF ALLOWOVERWRITE DELIMITER '|';"
    EXPECTED_SINGLE_UNLOAD_WITH_ADDQUOTES = "UNLOAD ('SELECT * FROM funzies') TO 's3://useful-things-bucket/klaatu/barada/nikto/test_s3_upload_file.csv' CREDENTIALS 'faux_aws_credentials' PARALLEL OFF DELIMITER '|' ADDQUOTES;"
    EXPECTED_SINGLE_UNLOAD_WITH_ESCAPE = "UNLOAD ('SELECT * FROM funzies') TO 's3://useful-things-bucket/klaatu/barada/nikto/test_s3_upload_file.csv' CREDENTIALS 'faux_aws_credentials' PARALLEL OFF DELIMITER '|' ESCAPE;"

    @patch.object(RedshiftDatabase, 'execute')
    @patch('aws_etl_tools.redshift_database.AWS')
    def test_unload_with_parallel_on(self, mock_aws, db_execution):
        db_execution.return_value = 'alex'
        mock_aws.return_value.connection_string.return_value = self.AWS_CONNECTION_CREDENTIALS

        self.REDSHIFT_DATABASE.unload(self.DOWNLOAD_QUERY, self.S3_PATH, is_parallel_unload=True)

        self.REDSHIFT_DATABASE.execute.assert_called_once_with(self.EXPECTED_PARALLEL_UNLOAD)

    @patch.object(RedshiftDatabase, 'execute')
    @patch('aws_etl_tools.redshift_database.AWS')
    def test_unload_with_parallel_off(self, mock_aws, _):
        mock_aws.return_value.connection_string.return_value = self.AWS_CONNECTION_CREDENTIALS

        self.REDSHIFT_DATABASE.unload(self.DOWNLOAD_QUERY, self.S3_PATH)

        self.REDSHIFT_DATABASE.execute.assert_called_once_with(self.EXPECTED_SINGLE_UNLOAD)

    @patch.object(RedshiftDatabase, 'execute')
    @patch('aws_etl_tools.redshift_database.AWS')
    def test_unload_with_overwrite_on(self, mock_aws, _):
        mock_aws.return_value.connection_string.return_value = self.AWS_CONNECTION_CREDENTIALS

        self.REDSHIFT_DATABASE.unload(self.DOWNLOAD_QUERY, self.S3_PATH, allow_overwrite=True)

        self.REDSHIFT_DATABASE.execute.assert_called_once_with(self.EXPECTED_SINGLE_UNLOAD_WITH_OVERWRITE)

    @patch.object(RedshiftDatabase, 'execute')
    @patch('aws_etl_tools.redshift_database.AWS')
    def test_unload_with_quotes_on(self, mock_aws, _):
        mock_aws.return_value.connection_string.return_value = self.AWS_CONNECTION_CREDENTIALS

        self.REDSHIFT_DATABASE.unload(self.DOWNLOAD_QUERY, self.S3_PATH, add_quotes=True)

        self.REDSHIFT_DATABASE.execute.assert_called_once_with(self.EXPECTED_SINGLE_UNLOAD_WITH_ADDQUOTES)

    @patch.object(RedshiftDatabase, 'execute')
    @patch('aws_etl_tools.redshift_database.AWS')
    def test_unload_with_escape_on(self, mock_aws, _):
        mock_aws.return_value.connection_string.return_value = self.AWS_CONNECTION_CREDENTIALS

        self.REDSHIFT_DATABASE.unload(self.DOWNLOAD_QUERY, self.S3_PATH, escape=True)

        self.REDSHIFT_DATABASE.execute.assert_called_once_with(self.EXPECTED_SINGLE_UNLOAD_WITH_ESCAPE)
