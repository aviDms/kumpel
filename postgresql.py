import psycopg2
from psycopg2.extras import RealDictConnection
from .helpers import read_sql


class Table(object):
    """ Database table - The Load step in ETL

        Abstraction of a database table, used to:
            * CREATE or recreate from SQL
            * INSERT, UPDATE, UPSERT data from a generator of dicts
            * DROP TABLE IF EXISTS
            * TRUNCATE TABLE
            * run data profile
            * check if table exists
            * add column to existing table and load its data only

        Current support: PostgreSQL

        TODO:
    """

    def __init__(self, name, schema, uri=None):
        """ """
        self.name = name
        self.schema = schema
        self.uri = uri

    def exists(self):
        """ Returns True if table exists false otherwise. """
        stmt = "SELECT COUNT(1)=1 " \
               "FROM information_schema.tables " \
               "WHERE table_schema='%s' " \
               "AND table_name='%s';" % (self.schema, self.name)
        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt)
                resp = cursor.fetchone()
        assert isinstance(resp[0], bool)
        return resp[0]

    def column_exists(self, col_name):
        """ Returns True if column exists false otherwise. """
        stmt = "SELECT COUNT(1)=1 " \
               "FROM information_schema.columns " \
               "WHERE table_schema='{schema}' " \
               "AND table_name='{table}' " \
               "AND column_name='{column}';"
        stmt = stmt.format(schema=self.schema, table=self.name,
                           column=col_name)
        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt)
                resp = cursor.fetchone()
        assert isinstance(resp[0], bool)
        return resp[0]

    def create(self, sql=None, script_path=None, drop_if_exists=False):
        """
        TODO: add **kargs to create stmt
        """
        drop_stmt = "DROP TABLE IF EXISTS %s.%s;" % (self.schema, self.name)
        if sql:
            create_stmt = sql
        elif script_path:
            create_stmt = read_sql(path=script_path, schema=self.schema,
                                   table=self.name)
        else:
            exit('Create SQL statement is missing.')

        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                if not self.exists():
                    cursor.execute(create_stmt)
                    connection.commit()
                elif self.exists() and drop_if_exists:
                    cursor.execute(drop_stmt)
                    cursor.execute(create_stmt)
                    connection.commit()
                else:
                    pass

    def add_column(self, col_name, col_type):
        stmt = "ALTER TABLE {schema}.{table} ADD COLUMN {column} {type};"
        stmt = stmt.format(schema=self.schema, table=self.name, column=col_name,
                           type=col_type)
        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt)
            connection.commit()

    def truncate(self):
        truncate_stmt = "TRUNCATE %s.%s;" % (self.schema, self.name)
        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(truncate_stmt)
            connection.commit()

    def vacuum(self, full=True, analyze=True):
        """ Need to close other open connections first """
        raise NotImplementedError
        # truncate_stmt = "VACUUM FULL ANALYZE %s.%s;" % (self.schema, self.name)
        # with psycopg2.connect(self.uri) as connection:
        #     with connection.cursor() as cursor:
        #         cursor.execute(truncate_stmt)
        #         connection.commit()

    def insert(self, rows, conflict_on=None):
        """ INSERT rows into existing PostgreSQL table.

        :param rows: python generator
        :param conflict_on: name of the column, string

        The conflict_on column must have unique values. It is used as a rule
        by the INSERT command to determine if the row needs to be appended to
        the table (e.g. INSERT) or if the row already exists and the values
        need to be updated (e.g. UPDATE).
        """
        first_row = next(rows)
        keys = first_row.keys()

        if conflict_on:
            stmt = self.get_upsert_stmt(columns=keys, constraint=conflict_on)
        else:
            stmt = self.get_insert_stmt(columns=keys)

        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt, list(first_row.values()))
                for row in rows:
                    cursor.execute(stmt, list(row.values()))
            connection.commit()

    def get_insert_stmt(self, columns):
        """ Generate INSERT statement using a list of columns.

        :param columns: list of strings

        TODO:
            * created_at, updated_at -- need to figure out if
        it is a good idea to do it here
            * serial id, hash, and so on
        """
        insert_stmt = "INSERT INTO {table} ({columns}) VALUES ({values});"
        return insert_stmt.format(
            table='.'.join([self.schema, self.name]),
            columns=', '.join(columns),
            values=', '.join(['%s' for c in columns])
        )

    def get_upsert_stmt(self, columns, constraint):
        """ Generate INSERT ON CONFLICT statement using a list of columns.

        :param columns: list of strings
        :param constraint: string

        TODO: updated_at
        """
        merge_stmt = "INSERT INTO {table} ({columns}) VALUES ({values}) " \
                     "ON CONFLICT ({constraint}) DO UPDATE " \
                     "SET ({columns}) = ({excluded_values});"
        return merge_stmt.format(
            table='.'.join([self.schema, self.name]),
            columns=', '.join(columns),
            values=', '.join(['%s' for c in columns]),
            constraint=constraint,
            excluded_values=', '.join(['EXCLUDED.%s' % c for c in columns])
        )

    def update(self, rows, conflict_on):
        """ INSERT rows into existing PostgreSQL table.

        :param rows: python generator
        :param conflict_on: name of the column, string

        The conflict_on column must have unique values. It is used as a rule
        by the INSERT command to determine if the row needs to be appended to
        the table (e.g. INSERT) or if the row already exists and the values
        need to be updated (e.g. UPDATE).
        """
        first_row = next(rows)
        constraint = first_row.pop(conflict_on)
        keys = first_row.keys()

        stmt = self.get_update_stmt(columns=keys)
        where = " WHERE %s = %s;" % (conflict_on, constraint)

        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt + where, list(first_row.values()))
                for row in rows:
                    constraint = row.pop(conflict_on)
                    where = " WHERE %s = %s;" % (conflict_on, constraint)
                    cursor.execute(stmt + where, list(row.values()))
            connection.commit()

    def get_update_stmt(self, columns):
        """ Generate UPDATE statement using a list of columns.

        :param constraint:
        :param columns: list of strings

        TODO:
            * updated_at -- need to figure out if
        it is a good idea to do it here
            * serial id, hash, and so on
        """
        insert_stmt = "UPDATE {table} SET ({columns}) = ({values})"
        return insert_stmt.format(
            table='.'.join([self.schema, self.name]),
            columns=', '.join(columns),
            values=', '.join(['%s' for c in columns]),
        )

    def get_constraints(self):
        """ Query db for the list of constraints for this table """
        stmt = "SELECT constraint_name, constraint_type " \
               "FROM information_schema.table_constraints " \
               "WHERE table_name='{name}' and table_schema" \
               "='{schema}'".format(name=self.name, schema=self.schema)

        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt)
                for row in cursor:
                    c_name, c_type = row
                    yield {c_name: c_type}

    def get_row_count(self):
        stmt = "SELECT n_live_tup FROM pg_stat_user_tables " \
               "WHERE relname='{name}' AND schemaname='{schema}'" \
            .format(name=self.name, schema=self.schema)

        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt)
                res = cursor.fetchone()
        return res[0]

    def list_columns(self):
        stmt = "SELECT column_name FROM information_schema.columns " \
               "WHERE table_name='{name}' and table_schema='{schema}'" \
            .format(name=self.name, schema=self.schema)

        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt)
                for row in cursor:
                    yield row[0]

    def data_profile(self):
        res = list()
        for column in self.list_columns():
            row = dict()
            row['name'] = column
            raise NotImplementedError

    def table_profile(self):
        """tables info: row_count, usage, size, indexes, dependencies"""
        raise NotImplementedError

    def set_index(self):
        raise NotImplementedError


class Database(object):
    """ Database object: a list of Table objects.

        Current support: PostgreSQL

        TODO:
    """

    def __init__(self, uri, schema='public'):
        self.uri = uri
        self.schema = schema
        self.tables = self.__list_tables()
        self.__initiate_tables()

    def __repr__(self):
        pass

    def __list_tables(self):
        tables = list()
        stmt = "SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}';"

        with psycopg2.connect(self.uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt.format(schema=self.schema))
                for row in cursor:
                    tables.append(row[0])
        return tables

    def __initiate_tables(self):
        for table in self.tables:
            self.__setattr__(table, Table(table, self.schema, self.uri))