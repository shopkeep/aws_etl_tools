from aws_etl_tools.redshift_database import RedshiftDatabase
from examples.subclass_ingestor import CustomIngestor
from aws_etl_tools.redshift_ingest.ingestors import AuditedUpsertToPostgres


class MyDatabase(RedshiftDatabase):
    '''Extends RedshiftDatabase with an environment and a custom ingestor
    '''

    def __init__(self, environment):
        self.environment = environment
        self.credentials = Config(environment).for_key('my_database')
        self.ingestion_class = CustomIngestor if self.is_test_database else AuditedUpsertToPostgres

    @property
    def is_test_database(self):
        return self.environment in ['test', 'development']
