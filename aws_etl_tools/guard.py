from functools import wraps
from importlib import reload

from aws_etl_tools.exceptions import NoS3BasePathError


def requires_s3_base_path(original_function):
    @wraps(original_function)
    def raises_without_s3_base_path(*args, **kwargs):
        from aws_etl_tools import config
        if not config.S3_BASE_PATH:
            raise NoS3BasePathError("You must set an S3_BASE_PATH to access this functionality. " \
                                    "Check the docs around configuration of environment vaiables.")
        return original_function(*args, **kwargs)
    return raises_without_s3_base_path
