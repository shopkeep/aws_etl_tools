import unittest
from unittest.mock import Mock, PropertyMock, patch, call

import boto3
import botocore

from aws_etl_tools.aws import AWS
from aws_etl_tools import config
from tests import test_helper


class TestAWSConnectionString(unittest.TestCase):

    LOCAL_CONNECTION_STRING = 'aws_access_key_id=aws_mock_key;aws_secret_access_key=aws_mock_secret;token=aws_mock_token'
    EC2_CONNECTION_STRING = 'aws_access_key_id=aws_mock_key;aws_secret_access_key=aws_mock_secret;token=aws_mock_token'

    @patch.object(boto3, 'resource')
    @patch.object(boto3, 'Session')
    def test_from_local_authentication(self, mock_boto_session, mock_boto_connection_request):
        mock_aws_credentials = Mock()
        key_property = PropertyMock(return_value='aws_mock_key')
        secret_property = PropertyMock(return_value='aws_mock_secret')
        token_property = PropertyMock(return_value='aws_mock_token')
        type(mock_aws_credentials).access_key = key_property
        type(mock_aws_credentials).secret_key = secret_property
        type(mock_aws_credentials).token = token_property
        mock_boto_session.return_value.get_credentials.return_value = mock_aws_credentials

        connection_string = AWS().connection_string()

        self.assertEqual(connection_string, self.LOCAL_CONNECTION_STRING)

    @patch.object(AWS, '_request_temporary_credentials')
    @patch.object(boto3, 'Session')
    @patch.object(boto3, 'resource')
    def test_from_ec2_iam_authentication(self, mock_boto_resource, mock_boto_session, mock_aws_request):
        mock_resource = Mock()
        client_mock = Mock()
        meta_mock = Mock()
        client_mock.head_bucket.side_effect = \
            botocore.exceptions.ClientError(error_response={'Error': {'Code': '403', 'Message': 'NotFound'}}, operation_name='HeadBucket')
        meta_property = PropertyMock(return_value=meta_mock)
        type(mock_resource).meta = meta_property
        client_property = PropertyMock(return_value=client_mock)
        type(meta_mock).client = client_property
        mock_boto_resource.return_value = mock_resource
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

    @patch.object(boto3, 'resource')
    @patch.object(boto3, 'Session')
    def test_can_connect_without_s3_base_path(self, *_):
        config.S3_BASE_PATH = None
        try:
            aws_object = AWS().s3_connection()
        except Exception as e:
            self.fail("AWS() unexpectedly raised an exception related to S3_BASE_PATH: `{}`".format(e))


class TestAWSConnection(unittest.TestCase):

    @patch.object(boto3, 'resource')
    @patch.object(boto3, 'Session')
    def test_initializes_and_returns_s3_connection(self, mock_boto_session, mock_boto_resource):
        mock_aws_credentials = Mock()
        key_property = PropertyMock(return_value='aws_mock_key')
        secret_property = PropertyMock(return_value='aws_mock_secret')
        token_property = PropertyMock(return_value='aws_mock_token')
        type(mock_aws_credentials).access_key = key_property
        type(mock_aws_credentials).secret_key = secret_property
        type(mock_aws_credentials).token = token_property
        mock_boto_session.return_value.get_credentials.return_value = mock_aws_credentials

        mock_s3_connection = Mock()
        mock_boto_resource.return_value = mock_s3_connection

        expected_calls = [
            call('s3', aws_secret_access_key='aws_mock_secret', aws_access_key_id='aws_mock_key', aws_session_token='aws_mock_token'),
            call('s3', aws_secret_access_key='aws_mock_secret', aws_access_key_id='aws_mock_key', aws_session_token='aws_mock_token')
        ]

        s3_connection = AWS().s3_connection()

        self.assertEqual(s3_connection, mock_s3_connection)
        mock_boto_resource.assert_has_calls(expected_calls, any_order=True)

    @patch.object(boto3, 'resource')
    @patch.object(boto3, 'client')
    @patch.object(boto3, 'Session')
    def test_initializes_and_returns_comprehend_connection(self, mock_boto_session, mock_boto_client, _):
        mock_aws_credentials = Mock()
        key_property = PropertyMock(return_value='aws_mock_key')
        secret_property = PropertyMock(return_value='aws_mock_secret')
        token_property = PropertyMock(return_value='aws_mock_token')
        region_property = PropertyMock(return_value='aws_mock_region_name')
        type(mock_aws_credentials).access_key = key_property
        type(mock_aws_credentials).secret_key = secret_property
        type(mock_aws_credentials).token = token_property

        mock_local_session = Mock()
        type(mock_local_session).region_name = region_property
        mock_boto_session.return_value = mock_local_session
        mock_local_session.get_credentials.return_value = mock_aws_credentials

        mock_comprehend_connection = Mock()
        mock_boto_client.return_value = mock_comprehend_connection

        expected_call = [
            call('comprehend', aws_secret_access_key='aws_mock_secret', aws_access_key_id='aws_mock_key', aws_session_token='aws_mock_token', region_name='aws_mock_region_name')
        ]

        comprehend_connection = AWS().comprehend_connection()

        self.assertEqual(comprehend_connection, mock_comprehend_connection)
        mock_boto_client.assert_has_calls(expected_call, any_order=True)


    @patch.object(boto3, 'resource')
    @patch.object(boto3, 'client')
    @patch.object(boto3, 'Session')
    def test_initializes_and_returns_athena_connection(self, mock_boto_session, mock_boto_client, _):
        mock_aws_credentials = Mock()
        key_property = PropertyMock(return_value='aws_mock_key')
        secret_property = PropertyMock(return_value='aws_mock_secret')
        token_property = PropertyMock(return_value='aws_mock_token')
        type(mock_aws_credentials).access_key = key_property
        type(mock_aws_credentials).secret_key = secret_property
        type(mock_aws_credentials).token = token_property
        mock_boto_session.return_value.get_credentials.return_value = mock_aws_credentials

        mock_athena_connection = Mock()
        mock_boto_client.return_value = mock_athena_connection

        expected_call = [
            call('athena', aws_secret_access_key='aws_mock_secret', aws_access_key_id='aws_mock_key', aws_session_token='aws_mock_token')
        ]

        athena_connection = AWS().athena_connection()

        self.assertEqual(athena_connection, mock_athena_connection)
        mock_boto_client.assert_has_calls(expected_call, any_order=True)
