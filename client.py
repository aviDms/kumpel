# -*- coding: utf-8 -*-
from psycopg2.extras import RealDictConnection

__author__ = "avram dames"
__doc__ = """ ETL tool """


class Source(object):
    """ List of connections to Postgresql Cluster. """
    def __init__(self):
        self.connections = dict()

    def add_connection(self, name, dsn):
        assert isinstance(name, str)
        assert isinstance(dsn, str)
        self.connections[name] = dsn

    def __repr__(self):
        return 'Source: %s.' % ', '.join(self.connections.keys())

    def tables(self, table_schema):
        get_tables_stmt = "SELECT table_name FROM information_schema.tables WHERE table_schema='%s'" % table_schema
        for name, dsn in self.connections.items():
            with Connection(name, dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(get_tables_stmt)
                    for row in cursor:
                        yield row['table_name']


class Connection(RealDictConnection):
    """ Psycopg2 connection handle. """
    def __init__(self, name, dsn):
        self.name = name
        self.app_name = 'Eazy-Etl'
        self.db_url = '{conn_string}?application_name={app_name}'
        super().__init__(self.db_url.format(conn_string=dsn,
                                            app_name=self.app_name))

    def __repr__(self):
        s = 'Application: {app} \nConnection: {name} (status:{status})'
        return s.format(app=self.name, name=self.name, status=self.status)


class Query(object):
    """ Query object - The E in ETL """
    def __init__(self, db, sql=None, script_path=None):
        """
        :param db: Source object with one or more connections
        :param sql: SQL string
        :param script_path: os path to SQL script
        """
        self.connections = db.connections
        self.sql = sql
        self.script_path = script_path

    def run(self):
        """
        """
        for name, dsn in self.connections.items():
            with Connection(name, dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(self.sql)
                    for row in cursor:
                        row['__conn_name'] = connection.name
                        yield row
