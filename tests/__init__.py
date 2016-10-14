import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from .settings import *


def _connection_to_base_postgres():
    connection = psycopg2.connect(database=EXISTING_POSTGRES_CREDENTIALS['database_name'],
                                  user=EXISTING_POSTGRES_CREDENTIALS['username'],
                                  password=EXISTING_POSTGRES_CREDENTIALS['password'],
                                  host=EXISTING_POSTGRES_CREDENTIALS['host'],
                                  port=EXISTING_POSTGRES_CREDENTIALS['port'])
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return connection


def _create_test_databases():
    base_postgres_connection = _connection_to_base_postgres()
    with base_postgres_connection.cursor() as cursor:
        cursor.execute('CREATE DATABASE {new_db};'.format(new_db=TEST_REDSHIFT_DATABASE))
        cursor.execute('CREATE DATABASE {new_db};'.format(new_db=TEST_POSTGRES_DATABASE))
    base_postgres_connection.close()


def _configure_test_redshift_database():
    redshift_connection = psycopg2.connect(database=REDSHIFT_TEST_CREDENTIALS['database_name'],
                                   user=REDSHIFT_TEST_CREDENTIALS['username'],
                                   password=REDSHIFT_TEST_CREDENTIALS['password'],
                                   host=REDSHIFT_TEST_CREDENTIALS['host'],
                                   port=REDSHIFT_TEST_CREDENTIALS['port'])
    redshift_connection.autocommit = True
    with redshift_connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE {audit_table} (
                uuid VARCHAR(36) NOT NULL,
                loaded_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                schema_name VARCHAR(32) NOT NULL,
                table_name VARCHAR(50) NOT NULL,
                detail VARCHAR(65535),
                PRIMARY KEY (uuid)
            );
        """.format(audit_table=INGEST_AUDIT_TABLE_NAME))
        cursor.execute("""
            CREATE FUNCTION PG_LAST_COPY_ID() RETURNS integer AS $$
                SELECT 0;
            $$ LANGUAGE SQL;
        """)
    redshift_connection.close()


def _drop_test_databases():
    base_postgres_connection = _connection_to_base_postgres()
    for new_database in [TEST_REDSHIFT_DATABASE, TEST_POSTGRES_DATABASE]:
        with base_postgres_connection.cursor() as cursor:
            try:
                cursor.execute('DROP DATABASE {new_db};'.format(new_db=new_database))
            except psycopg2.ProgrammingError:
                # Could not drop the database because it probably doesn't exist
                cursor.execute("ROLLBACK")
    base_postgres_connection.close()


def setup_package():
    # setup tests by creating two local postgres databases, one of which is pretending to be redshift
    # the db that's pretending needs an ingest audit table and PG_LAST_COPY_ID(), a function
    # which is used for auditing that exists on hosted redshift but not vanilla postgres.
    _drop_test_databases()
    _create_test_databases()
    _configure_test_redshift_database()


def teardown_package():
    # after the tests have finished, drop these databases
    _drop_test_databases()
