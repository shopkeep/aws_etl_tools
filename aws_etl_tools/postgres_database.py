import os

import psycopg2 as ps
from sqlalchemy import create_engine


class PostgresDatabase:

    def __init__(self, credentials):
        '''Takes `credentials`: a dict with database_name, username, password, host, and port.'''
        self.credentials = credentials

    def make_new_cursor(self):
        db_connection = ps.connect(database=self.credentials["database_name"],
                                   user=self.credentials["username"],
                                   password=self.credentials["password"],
                                   host=self.credentials["host"],
                                   port=self.credentials["port"])
        db_connection.autocommit = True
        return db_connection.cursor()

    def execute(self, query, params=None):
        cursor = self.make_new_cursor()
        cursor.execute(query, params)
        cursor.connection.close()

    def executemany(self, query, params=None):
        cursor = self.make_new_cursor()
        cursor.executemany(query, params)
        cursor.connection.close()

    def fetch(self, query, params=None):
        cursor = self.make_new_cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

    def table_count(self, table_name):
        return int(self.fetch("""SELECT COUNT(1) FROM %s""" % table_name)[0][0])

    def table_value_max(self, table, column):
        return self.fetch("""SELECT max(%s) FROM %s""" % (column, table))[0][0]

    def table_value_min(self, table, column):
        return self.fetch("""SELECT min(%s) FROM %s""" % (column, table))[0][0]

    def create_database_engine(self):
        return create_engine('postgres://%(username)s:%(password)s@%(host)s:%(port)s/%(database_name)s' % self.credentials)

