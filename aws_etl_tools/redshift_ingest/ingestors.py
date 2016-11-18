from collections import OrderedDict
from datetime import datetime
import json
import os
from uuid import uuid4 as uuid

from psycopg2 import DatabaseError

from aws_etl_tools.aws import AWS
from aws_etl_tools.s3_file import S3File
from aws_etl_tools import config


class BasicUpsert:
    def __init__(self, file_path, destination, with_manifest=False, jsonpaths=None, gzip=None):
        self.file_path = file_path
        self.database = destination.database
        self.with_manifest = with_manifest
        self.jsonpaths = jsonpaths
        self.gzip = gzip
        self.target_table = destination.target_table
        self.schema_name, self.table_name = self.target_table.split('.')
        self.staging_table = destination.unique_identifier
        self.upsert_keys = destination.upsert_uniqueness_key

    def __call__(self):
        self.before_ingest()
        self.ingest()
        self.after_ingest()
        self.final_cleanup()

    def before_ingest(self):
        pass

    def after_ingest(self):
        pass

    def final_cleanup(self):
        pass

    @property
    def connection_string(self):
        return AWS().connection_string()

    def ingest(self):
        self.database.execute(self._ingest_query())

    def _ingest_query(self):
        return """
            BEGIN TRANSACTION;

            CREATE TEMP TABLE {staging_table} (LIKE {target_table});
            {copy_statement};
            DELETE FROM {target_table} USING {staging_table} WHERE ({upsert_match_statement});
            {insert_statement};
            DROP TABLE {staging_table};

            END TRANSACTION;
        """.format(
                target_table=self.target_table,
                staging_table=self.staging_table,
                copy_statement=self._copy_statement(),
                insert_statement=self._insert_statement(),
                upsert_match_statement=self._upsert_match_statement()
            )

    @property
    def copy_parameters(self):
        copy_parameters = ["EMPTYASNULL", "BLANKSASNULL", "TIMEFORMAT AS 'auto'", "STATUPDATE ON"]
        copy_parameters.append('MANIFEST') if self.manifest else None
        copy_parameters.append('GZIP') if self.gzip else None

        if self.jsonpaths:
            copy_parameters.append("JSON '{}'".format(self.jsonpaths))
        else:
            copy_parameters.extend(['CSV', 'IGNOREBLANKLINES'])

        return copy_parameters

    def _copy_statement(self):
        return """
            COPY {staging_table} FROM '{s3_path}'
            WITH CREDENTIALS AS '{connection_string}'
            {copy_commands}
        """.format(
            staging_table=self.staging_table,
            s3_path=self.file_path,
            connection_string=self.connection_string,
            copy_commands="\n".join(self.copy_parameters)
        )

    def _insert_statement(self):
        return "INSERT INTO {target_table} SELECT * FROM {staging_table}".format(
            target_table=self.target_table,
            staging_table=self.staging_table
        )

    def _upsert_match_statement(self):
        '''Validate that data in specified `upsert_uniqueness_key` columns matches between
        staging_table and target_table. A string is returned, which is then inserted into the
        SQL query.'''
        return ' and '.join(["%s.%s = %s.%s" % (
            self.target_table, key_element, self.staging_table, key_element) for key_element in self.upsert_keys])


class AuditedUpsert(BasicUpsert):

    def __init__(self, file_path, destination, **kwargs):
        super().__init__(file_path, destination, **kwargs)
        self.uuid = None
        self.load_start_time = None
        self.audit_table = config.REDSHIFT_INGEST_AUDIT_TABLE

    def before_ingest(self):
        self.uuid = str(uuid()).upper()
        self.load_start_time = datetime.utcnow()
        self.database.execute("""
            INSERT INTO {audit_table} (uuid, loaded_at, schema_name, table_name)
            VALUES (%(uuid)s, %(load_start_time)s, %(schema_name)s, %(table_name)s);
            """.format(audit_table=self.audit_table), {'uuid': self.uuid,
                  'load_start_time': self.load_start_time,
                  'schema_name': self.schema_name,
                  'table_name': self.table_name}
        )

    def after_ingest(self):
        self.database.execute("""
            UPDATE {audit_table}
            SET detail=%(ingest_results)s
            WHERE uuid=%(uuid)s
            """.format(audit_table=self.audit_table), {
                'ingest_results': self._fetch_ingest_results(),
                'uuid': self.uuid
            }
        )

    def cleanup(self):
        try:
            self.database.execute("""VACUUM {target_table};""".format(target_table=self.target_table))
        except DatabaseError:
            # this is a result of multiple vacuums running simultaneously
            # it's not a big deal to swallow this exception, because the
            # table will be vacuumed later
            pass


    def ingest(self):
        self._upsert_query_id = self.database.fetch(self._ingest_query())[0][0]
        self.ingest_results = self._fetch_ingest_results()

    def _ingest_query(self):
        basic_upsert_command = super()._ingest_query()
        return basic_upsert_command + "\nSELECT PG_LAST_COPY_ID();"

    def _fetch_ingest_results(self):
        ingest_results = self.database.fetch("""
            SELECT COALESCE(load_errors.query, load_commits.query) AS query_id
            , BTRIM(COALESCE(load_errors.filename, load_commits.filename)) AS filename
            , load_commits.lines_scanned
            , load_errors.colname as column_name
            , load_errors.type as column_type
            , load_errors.col_length as column_length
            , load_errors.position as error_row_number
            , load_errors.raw_field_value
            , load_errors.err_code as error_code
            , load_errors.err_reason
            FROM STL_LOAD_ERRORS load_errors
            FULL OUTER JOIN STL_LOAD_COMMITS load_commits
            ON load_errors.query = load_commits.query
            WHERE COALESCE(load_errors.query, load_commits.query) = %(query_id)s
            """, {'query_id': self._upsert_query_id})
        ingest_results_values = ingest_results[0] if ingest_results else []

        ingest_results_keys = [
            'query_id',
            'filename',
            'lines_scanned',
            'column_name',
            'column_type',
            'column_length',
            'error_row_number',
            'raw_field_value',
            'error_code',
            'err_reason'
        ]
        # OrderedDict is used to preserve the column order when dumped to json
        return json.dumps(OrderedDict(zip(ingest_results_keys, ingest_results_values)))


class AuditedUpsertToPostgres(AuditedUpsert):
    # For testing and development, it can be useful to have a local postgres
    # that behaves very similarly to the hosted Redshift.
    # The differences are:
    # 1) download the file from s3 so the ingest can be local
    # 2) remove all the remote and redshifty things from the COPY command
    # 3) the ingest result tables are redshift-specific. so we'll just stub that out.

    def __init__(self, file_path, destination):
        local_file_path = S3File(file_path).download_to_temp()
        super().__init__(local_file_path, destination)

    def ingest(self):
        with open(self.file_path) as local_file:
            cursor = self.database.make_new_cursor()
            cursor.copy_expert(self._ingest_query(), local_file)

    def _copy_statement(self):
        return """
            COPY {staging_table} FROM STDIN CSV;
        """.format(
            staging_table=self.staging_table
        )

    def _fetch_ingest_results(self):
        return '{}'
