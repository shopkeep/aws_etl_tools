from datetime import datetime


class RedshiftTable:

    def __init__(self, database, target_table, upsert_uniqueness_key):
        self.database = database
        self.target_table = target_table
        self.table_schema, self.table_name = self.target_table.split('.')
        self.upsert_uniqueness_key = upsert_uniqueness_key
        self.instantiation_timestamp = datetime.utcnow()

    @property
    def unique_identifier(self):
        return '%(table_name)s_%(timestamp)s' % {
            'table_name': self.table_name,
            'timestamp': self.instantiation_timestamp.strftime('%Y_%m_%d_%H_%M_%S')
        }
