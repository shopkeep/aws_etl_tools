import json
from urllib.request import urlopen

import boto3
from botocore.exceptions import ClientError
from botocore.utils import METADATA_SECURITY_CREDENTIALS_URL

from aws_etl_tools import config


class AWS:
    '''this class is wrapping the boto3 connection object with
        some extra attempts to make connecting as easy as possible.
        to override those, instantiate it like this:
        >> AWS(aws_access_key_id='brownie_recipe', secret_access_key='peanut_butter')
        aside from that, this class will look in the default boto config locations,
        and attempt a connection to s3 using the bucket defined as the config's base path.
        if that connection fails for any reason, then this class will hit amazon to
        request temporary credentials with a token based on the instance's IAM role.
        Either way, after initialization, this class is usable to connect to s3 and to
        build up a connection_string which is used primarily in Redshift commands like
        `COPY` from s3 and `UNLOAD` to s3.
    '''
    PUBLICLY_LISTABLE_S3_BUCKET = 'example-publicly-accessible'

    def __init__(self, **kwargs):
        try:
            self._connect_with_permanent_credentials(**kwargs)
            if config.S3_BASE_PATH:
                testable_bucket_name = config.S3_BASE_PATH.replace('s3://', '').split('/')[0]
            else:
                testable_bucket_name = self.PUBLICLY_LISTABLE_S3_BUCKET
            self.s3_connection().meta.client.head_bucket(Bucket=testable_bucket_name)
            self.comprehend_connection().detect_dominant_language(Text='test-string')
        except ClientError:
            self._connect_with_temporary_credentials()

    def connection_string(self):
        aws_credential_string = 'aws_access_key_id=%s;aws_secret_access_key=%s' % (
            self.key, self.secret)
        if self.token is not None:
            aws_credential_string += ';token=%s' % self.token
        return aws_credential_string

    def s3_connection(self):
        return boto3.resource('s3', aws_access_key_id=self.key,
                                    aws_secret_access_key=self.secret,
                                    aws_session_token=self.token)

    def comprehend_connection(self):
        return boto3.client('comprehend', aws_access_key_id=self.key,
                                    aws_secret_access_key=self.secret,
                                    aws_session_token=self.token,
                                    region_name=self.region_name)

    def _connect_with_permanent_credentials(self, **kwargs):
        '''creates an aws session through boto using a set key and secret
            that is either passed in explicitly, set through environment
            variables in config, or set in a boto configuration file in the
            default location'''
        possibly_valid_key = kwargs.get('aws_access_key_id', config.AWS_ACCESS_KEY_ID)
        possibly_valid_secret = kwargs.get('aws_secret_access_key', config.AWS_SECRET_ACCESS_KEY)
        possibly_valid_token = kwargs.get('aws_session_token', config.AWS_SESSION_TOKEN)
        possibly_valid_region = kwargs.get('region_name', config.AWS_DEFAULT_REGION)
        local_aws_session = boto3.Session(aws_access_key_id=possibly_valid_key,
                                          aws_secret_access_key=possibly_valid_secret,
                                          aws_session_token=possibly_valid_token,
                                          region_name=possibly_valid_region,
                                          **kwargs)
        local_aws_credentials = local_aws_session.get_credentials()
        self.key = local_aws_credentials.access_key
        self.secret = local_aws_credentials.secret_key
        self.token = local_aws_credentials.token
        self.region_name = local_aws_session.region_name

    def _connect_with_temporary_credentials(self):
        credentials_from_iam_role = self._request_temporary_credentials()
        self.key = credentials_from_iam_role['AccessKeyId']
        self.secret = credentials_from_iam_role['SecretAccessKey']
        self.token = credentials_from_iam_role['Token']
        self.region_name = config.AWS_DEFAULT_REGION

    def _request_temporary_credentials(self):
        aws_iam_base_url = METADATA_SECURITY_CREDENTIALS_URL
        iam_role_name = urlopen(aws_iam_base_url).read().decode()
        aws_iam_creds_url = aws_iam_base_url + iam_role_name
        creds_dictionary = json.loads(
            urlopen(aws_iam_creds_url).read().decode())
        return creds_dictionary
