from aws_etl_tools.aws import AWS
from aws_etl_tools.postgres_database import PostgresDatabase
from aws_etl_tools.redshift_ingest.ingestors import BasicUpsert


class RedshiftDatabase(PostgresDatabase):
    ingestion_class = BasicUpsert

    def unload(self, query, s3_path, delimiter='|', is_parallel_unload=False, allow_overwrite=False, add_quotes=False, escape=False):
        '''Unloads a query on this database to an s3_path.
            `is_parallel_unload` is good for unloading to multiple files with a
            manifest when you know you will be loading this data back into redshift
            like another table or a different cluster. if you are planning to pull
            this data into something else, the default behavior is to unload to a
            single CSV file.

            example usage:
            >> query = 'select * from events where timestamp < \'2014-01-01\''
            >> destination_s3_path = 's3://ye-bucket/data_dumps/2013-events'
            >> RedshiftDatabase(credentials_dict).unload(query, destination_s3_path)'''

        options = {
            'is_parallel_unload': is_parallel_unload,
            'allow_overwrite': allow_overwrite,
            'delimiter': delimiter,
            'add_quotes': add_quotes,
            'escape': escape
        }

        unload_query = self._compose_unload_query(query, s3_path, options)
        self.execute(unload_query)

    def _compose_unload_query(self, query, s3_path, options):
        query_commands = ['MANIFEST'] if options.get('is_parallel_unload', False) else ['PARALLEL OFF']
        query_commands.append('ALLOWOVERWRITE') if options.get('allow_overwrite', False) else None
        query_commands.append('DELIMITER \'%s\'' % options.get('delimiter'))
        query_commands.append('ADDQUOTES') if options.get('add_quotes', False) else None
        query_commands.append('ESCAPE') if options.get('escape', False) else None

        unload_query_options = ' '.join(query_commands).strip()

        unload_query = """UNLOAD ('{query}') TO '{s3_path}'
                          CREDENTIALS '{aws_connection_string}'
                          {unload_query_options};""".format(s3_path=s3_path,
                                                   aws_connection_string=AWS().connection_string(),
                                                   query=query,
                                                   unload_query_options=unload_query_options)
        unload_query = unload_query.replace("\n", "").replace("                          ", " ")
        return unload_query
