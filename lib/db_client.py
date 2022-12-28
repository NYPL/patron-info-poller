import os
import psycopg2
import redshift_connector

from botocore.exceptions import ClientError
from enum import Enum
from helpers.log_helper import create_log


class DbMode(Enum):
    SIERRA = 1
    REDSHIFT = 2

    def __str__(self):
        return self.name.lower()


class DbClient:
    """
    Client for managing connections to either the Sierra PostgreSQL database or
    the NYPL Redshift database depending on the mode.

    The `mode` argument should be one of the enums above.
    """

    def __init__(self, mode):
        if mode not in DbMode:
            raise DbClientError(
                'DbClient initialized in unknown mode: {}'.format(mode))

        self.logger = create_log('{}_db_client'.format(mode))
        self.logger.debug('Connecting to {} database'.format(mode))
        self.mode = mode
        self.conn = None
        try:
            if mode == DbMode.SIERRA:
                self.conn = psycopg2.connect(
                    host=os.environ['SIERRA_DB_HOST'],
                    port=os.environ['SIERRA_DB_PORT'],
                    dbname=os.environ['SIERRA_DB_NAME'],
                    user=os.environ['SIERRA_DB_USER'],
                    password=os.environ['SIERRA_DB_PASSWORD'])
            elif mode == DbMode.REDSHIFT:
                self.conn = redshift_connector.connect(
                    iam=True,
                    cluster_identifier=os.environ['REDSHIFT_CLUSTER'],
                    database=os.environ['REDSHIFT_DB_NAME'],
                    db_user=os.environ['REDSHIFT_DB_USER'],
                    region=os.environ['AWS_REGION'],
                    access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                    secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
        except (psycopg2.OperationalError, ClientError) as e:
            self.conn = None
            self.logger.error(
                'Error connecting to {mode} database: {error}'.format(
                    mode=mode, error=e))
            raise DbClientError(
                'Error connecting to {mode} database: {error}'.format(
                    mode=mode, error=e)) from None

    def execute_query(self, query):
        """
        Executes an arbitrary query against the chosen database.

        Returns a sequence of tuples representing the rows returned by the
        query.
        """
        if self.conn is None:
            return []

        self.logger.info('Querying {} database'.format(self.mode))
        self.logger.debug('Executing query {}'.format(query))
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(('Error executing {mode} query \'{query}\': '
                               '{error}').format(mode=self.mode, query=query,
                                                 error=e))
            raise DbClientError(('Error executing {mode} query \'{query}\': '
                                 '{error}').format(mode=self.mode, query=query,
                                                   error=e)) from None
        finally:
            cursor.close()

    def close_connection(self):
        self.logger.debug('Closing {} database connection'.format(self.mode))
        self.conn.close()


class DbClientError(Exception):
    def __init__(self, message=None):
        self.message = message
