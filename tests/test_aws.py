import unittest
import boto
from unittest.mock import Mock, PropertyMock, patch

from aws_etl_tools.aws import AWS
from aws_etl_tools import config
from tests import test_helper


class TestAWSConnectionString(unittest.TestCase):

    LOCAL_CONNECTION_STRING = 'aws_access_key_id=aws_mock_key;aws_secret_access_key=aws_mock_secret'
    EC2_CONNECTION_STRING = 'aws_access_key_id=aws_mock_key;aws_secret_access_key=aws_mock_secret;token=aws_mock_token'

    @patch.object(boto, 'connect_s3')
    def test_from_local_authentication(self, mock_boto_connection_request):
        mock_aws_connection = Mock()
        mock_aws_connection.get_bucket.return_value = None
        key_property = PropertyMock(return_value='aws_mock_key')
        secret_property = PropertyMock(return_value='aws_mock_secret')
        type(mock_aws_connection).aws_access_key_id = key_property
        type(mock_aws_connection).aws_secret_access_key = secret_property
        mock_boto_connection_request.return_value = mock_aws_connection

        connection_string = AWS().connection_string()

        self.assertEqual(connection_string, self.LOCAL_CONNECTION_STRING)

    @patch.object(AWS, '_request_temporary_credentials')
    @patch.object(boto, 'connect_s3')
    def test_from_ec2_iam_authentication(self, mock_boto_connection_request, mock_aws_request):
        mock_aws_connection = Mock()
        mock_aws_connection.get_bucket.side_effect = boto.exception.S3ResponseError(status=403, reason='forbidden')
        mock_boto_connection_request.return_value = mock_aws_connection

        mock_aws_request.return_value = {
            'AccessKeyId': 'aws_mock_key',
            'SecretAccessKey': 'aws_mock_secret',
            'Token': 'aws_mock_token'}

        connection_string = AWS().connection_string()

        self.assertEqual(connection_string, self.EC2_CONNECTION_STRING)


class TestInitWithoutS3BasePath(unittest.TestCase):
    def setUp(self):
        self.initial_s3_base_path = config.S3_BASE_PATH

    def tearDown(self):
        config.S3_BASE_PATH = self.initial_s3_base_path

    @patch.object(boto, 'connect_s3')
    def test_can_connect_without_s3_base_path(self, *_):
        config.S3_BASE_PATH = None
        try:
            aws_object = AWS().s3_connection()
        except Exception:
            self.fail("AWS() unexpectedly raised an exception related to S3_BASE_PATH")
