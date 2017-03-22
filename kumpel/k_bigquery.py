import logging
import bigquery
from time import sleep


class BigQuery(object):
    """ Python object for interacting with Google's BiqQuery
     database. The class defined here is just a simplification
     of the BigQuery lib from here:

     https://github.com/tylertreat/BigQuery-Python

     Its only purpose is to offer all the necessary methods
     for running the attribution model.
    """

    def __init__(self, credentials_file, project_id, readonly=True):
        self.client = bigquery.get_client(
            project_id=project_id,
            json_key_file=credentials_file,
            readonly=readonly
        )

    def get_schema(self, dataset, table):
        """
        Extract the schema of the target BQ table. Useful if you want to delete
        a table: save the schema first and then drop and recreate the table
        using this schema. Deletion of records is not supported in BQ.

        :param dataset: BQ target dataset
        :param table: BQ target table for which the schema is extracted
        :return: dictionary with the target table's column names and data types
        """
        return self.client.get_table_schema(dataset=dataset, table=table)

    def list_datasets(self):
        """ List of data sets in this project. Used by other methods as well.

        :return: <list> list of strings
        """
        l = []
        for dataset in self.client.get_datasets():
            l.append(dataset[u'datasetReference'][u'datasetId'])
        return l

    def exists(self, dataset, table=None):
        """
        Check if a BQ dataset or table already exists.

        :param dataset: <string> Name of the dataset
        :param table: <string, None> Name of the table or None in case
        you only want to test if the dataset exists
        :return: True if table exists False otherwise
        """
        if table:
            return self.client.check_table(dataset=dataset, table=table)
        else:
            if dataset in self.list_datasets():
                return True
            else:
                return False

    def create_table(self, dataset, table, schema):
        """
        Create table using your own schema or the schema saved using
        self.get_schema().

        :param dataset: BQ dataset where to create the new table
        :param table: name of the table to be created
        :param schema: dictionary with the target table's column names and data types
        :return: True if table was created False if table already exists
        """
        if self.exists(dataset, table):
            logging.info('BQ: Table %s.%s already exists.' % (dataset, table))
            return False
        else:
            try:
                self.client.create_table(dataset, table, schema)
            except Exception as e:
                logging.error('BQ: Error while creating table %s. %s\n%s' % (dataset, table, e))
            else:
                logging.info('BQ: Table %s.%s was created!' % (dataset, table))
                return True

    def delete_table(self, dataset, table):
        """
        Deleting records is not supported in BG. Hence, you'll need to
        drop and then recreate the table.

        :param dataset: Target dataset
        :param table: Table to be dropped
        :return: True if table was deleted False if table does not exits
        """
        if self.exists(dataset, table):
            self.client.delete_table(dataset, table)
            return True
        else:
            logging.info('BQ: %s.%s does no exists.' % (dataset, table))
            return False

    def create_dataset(self, dataset):
        """ Create a new empty dataset in current BQ project.

        :param dataset: <string>
        :return: True if dataset was created False if dataset already exists
        """
        if self.exists(dataset):
            logging.info('BQ: Dataset %s already exists!' % dataset)
            return False
        else:
            try:
                self.client.create_dataset(dataset_id=dataset)
            except Exception as e:
                logging.error('Error while creating dataset %s\n%s' % (dataset, e))
                raise e
            else:
                logging.info('Dataset %s was created!' % dataset)
                return True

    def delete_dataset(self, dataset, delete_contents=False):
        """ Deletes a dataset if exists.

        :param dataset: <string> Name of the dataset
        :param delete_contents: <bool> If True, forces the deletion
               of the dataset even when the dataset contains data
        :return: True if the dataset was deleted or False if the dataset did not exist.
        """
        if not self.exists(dataset=dataset):
            logging.info('BQ: Dataset %s does not exists!' % dataset)
            return False
        try:
            self.client.delete_dataset(dataset, delete_contents)
        except Exception as e:
            logging.error('BQ: Error while trying to delete dataset %s\n%s' % (dataset, e))
            raise e
        else:
            logging.info('Dataset %s has been deleted!' % dataset)
            return True

    def export_to_storage(self, destination_uris, dataset, table, **kwargs):
        """
        IN PROGRESS. NOT REALLY NEEDED unless there is a very large amount of data.

        :param destination_uris:
        :param dataset:
        :param table:
        :return:
        """
        job = kwargs.pop('job', None)
        compression = kwargs.pop('job', None)
        destination_format = kwargs.pop('job', 'CSV')
        print_header = kwargs.pop('job', True)
        field_delimiter = kwargs.pop('job', ';')

        job = self.client.export_data_to_uris(
            destination_uris, dataset, table, job, compression,
            destination_format, print_header, field_delimiter
        )
        try:
            _ = self.client.wait_for_job(job, timeout=300)
        except Exception as e:
            logging.error('Error while exporting BQ dataset to storage:\n%s\n\n' % e)
            logging.info('Hint: maybe timeout parameter in wait_for_job needs to be increased.')
            return False
        logging.info('Export successful.')
        return True

    def read_query(self, query, delay=5, batch_read=1000):
        """
        Run a query on BQ and return the result as a list of dicts.
        After the request is sent to BQ, script halts for 60 sec
        to give BQ time to process the request.

        After that, the reading begins only if the query has been
        completed, otherwise an error msg will be returned.

        :param batch_read:
        :param delay:
        :param query: sql query to be ran on the bq
        :return: generator of records
        """
        index = 0
        batch = batch_read if batch_read < 1000 else 1000

        job_id, _results = self.client.query(query)
        logging.info('BQ: running job %s ...' % job_id)
        logging.info(_results)

        complete = False
        while not complete:
            logging.info('Query still running, sleep for %i seconds ...' % delay)
            sleep(delay)
            complete, row_count = self.client.check_job(job_id)

        if complete:
            while index < row_count:
                results = self.client.get_query_rows(job_id, offset=index, limit=batch)
                for row in results:
                    yield row
                index += batch
        else:
            logging.error('BQ: Error while running query.')
            logging.info('Tip: please try again or increase the sleep time.')
            raise Exception('Query did not run successfully')

    def read_data(self, dataset, table, batch=None):
        """
        Read all records from a specific table in BQ.

        !!! Batch read needs to be implemented for tables that
        are to large to be queried in on go.

        :param dataset: BQ dataset name
        :param table:  BA table name to be fetched
        :param batch: integer
        :return: generator of records in a dict form
        """
        if batch:
            raise NotImplementedError
        else:
            return self.read_query('SELECT * FROM [%s.%s]' % (dataset, table))

    def write_to_table(self, dataset, table, data, primary_key=None,
                       batch_write=10000):
        """
        The bigquery.insertAll() function currently can not handle more then
        50 000 row at a time. Hence, we need to split the table into batches.

        Define the size of batch in init_bigquery.py - BATCH_WRITE_SIZE

        :param batch_write:
        :param dataset: target dataset in bigquery
        :param table: target table in bigquery
        :param data: generator of dicts, data to be uploaded to bigquery
        :param primary_key: field name of the primary key, in case there is one
        """
        i = 0
        batch = []

        for row in data:
            i += 1
            batch.append(row)
            if i % batch_write == 0:
                print(batch)
                self.client.push_rows(dataset=dataset,
                                      table=table,
                                      rows=batch,
                                      insert_id_key=primary_key)
                batch = []
        # commit the last rows before exit
        self.client.push_rows(dataset=dataset,
                              table=table,
                              rows=batch,
                              insert_id_key=primary_key)

    def write_query_to_table(self, query, dataset, table, write_disposition=None):
        """
        Run a query in BQ and save the output in a bigquery table

        :param query: BigQuery SQL query to be ran
        :param dataset: Target dataset where to save the table
        :param table: Target table where to save the output of the query
        :param write_disposition: What to do if target table already exits:
            WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        """
        res = self.client.write_to_table(query=query,
                                         dataset=dataset,
                                         table=table,
                                         write_disposition=write_disposition,
                                         allow_large_results=True)

        job_id = res['jobReference']['jobId']
        logging.info('BQ: write query to table ...')

        rows = None
        job_complete = False

        while not job_complete:
            job_complete, rows = self.client.check_job(job_id)
            logging.info('still writing ...')
            sleep(5)

        return rows
