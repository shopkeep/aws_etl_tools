class BaseAwsEtlToolsError(RuntimeError):
    def __init__(self, message):
        super().__init__(message)


class NoDataFoundError(BaseAwsEtlToolsError):
    def __init__(self, message):
        super().__init__(message)


class NoS3BasePathError(BaseAwsEtlToolsError):
    def __init__(self, message):
        super().__init__(message)
