import os


# required configuration to do almost anything with this package
# so at the top of your code, something like:
# > os.environ['AWS_ETL_TOOLS_S3_BASE_PATH']='s3://ye-bucket/this/isnt/optional'
S3_BASE_PATH_ENV_VAR_KEY = 'AWS_ETL_TOOLS_S3_BASE_PATH'
S3_BASE_PATH = os.getenv(S3_BASE_PATH_ENV_VAR_KEY)


# optional configuration. by default, ingestion will not be audited, but it's
# very easy to turn on and configure.
REDSHIFT_INGEST_AUDIT_TABLE = os.getenv('AWS_ETL_TOOLS_REDSHIFT_INGEST_AUDIT_TABLE', 'public.v1_ingest_audit')
LOCAL_TEMP_DIRECTORY = os.path.join(os.path.dirname(__file__), 'tmp')


# These default to None so the aws connection hierarchy will attempt
# to look for a boto configuration file if they're not set.
# If that's also not present, we'll request temporary creds based on
# the IAM role of the instance.
AWS_ACCESS_KEY_ID = os.getenv('AWS_ETL_TOOLS_AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_ETL_TOOLS_AWS_SECRET_ACCESS_KEY')


try:
    from local_config import *
except ImportError:
    pass
