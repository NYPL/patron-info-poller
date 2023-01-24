import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import datetime
import os
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from helpers.log_helper import create_log
from helpers.obfuscation_helper import obfuscate
from helpers.query_helper import (build_deleted_patrons_query,
                                  build_new_patrons_query,
                                  build_redshift_address_query,
                                  build_redshift_patron_query,
                                  build_updated_patrons_query)
from lib import (AvroEncoder, DbClient, DbMode, GeocoderApiClient,
                 KinesisClient, S3Client)


class PipelineMode(Enum):
    NEW_PATRONS = 1
    UPDATED_PATRONS = 2
    DELETED_PATRONS = 3

    def __str__(self):
        return self.name.lower().rstrip('_patrons')


_EST_TIMEZONE = datetime.timezone(datetime.timedelta(days=-1, seconds=68400))
_REDSHIFT_COLUMNS = [
    'patron_id', 'address_hash', 'postal_code', 'geoid', 'creation_date_et',
    'circ_active_date_et', 'ptype_code', 'pcode3', 'patron_home_library_code']
_SIERRA_COLUMNS_MAP = {
    PipelineMode.NEW_PATRONS:
        ['patron_id_plaintext', 'ptype_code', 'pcode3',
         'patron_home_library_code', 'city', 'region', 'postal_code',
         'address', 'circ_active_date_et', 'deletion_date_et',
         'last_updated_date_et', 'creation_timestamp'],
    PipelineMode.UPDATED_PATRONS:
        ['patron_id_plaintext', 'ptype_code', 'pcode3',
         'patron_home_library_code', 'city', 'region', 'postal_code',
         'address', 'circ_active_date_et', 'deletion_date_et',
         'creation_date_et', 'last_updated_timestamp'],
    PipelineMode.DELETED_PATRONS:
        ['patron_id_plaintext', 'deletion_date_et']}


class PipelineController:
    """
    Class for orchestrating different types of pipeline runs. There are three
    modes: 1) checking for newly created patrons, 2) checking for recently
    updated patrons (who may or may not have changed their address), and 3)
    checking for recently deleted patrons
    """

    def __init__(self):
        self.logger = create_log('pipeline_controller')

        self.s3_client = S3Client()
        self.geocoder_client = GeocoderApiClient()
        self.avro_encoder = AvroEncoder()
        self.kinesis_client = KinesisClient()
        self.has_max_batches = 'MAX_BATCHES' in os.environ
        self.poller_state = None
        self.processed_ids = set()

    def run_pipeline(self, mode):
        """Runs the full pipeline in the given PipelineMode mode."""
        if mode not in PipelineMode:
            raise PipelineControllerError(
                'run_pipeline called with bad pipeline mode: {}'.format(mode))

        sierra_client = DbClient(DbMode.SIERRA)
        if mode == PipelineMode.NEW_PATRONS:
            redshift_client = None
        else:
            redshift_client = DbClient(DbMode.REDSHIFT)

        batch_number = 1
        finished = False
        while not finished:
            # Retrieve the query parameters to use for this batch
            self.poller_state = self._get_poller_state(batch_number)

            # Process the data
            self.logger.info(
                'Begin processing {mode} patrons batch {batch} with state '
                '{state}'.format(
                    mode=mode, batch=batch_number, state=self.poller_state))
            if mode == PipelineMode.DELETED_PATRONS:
                last_record = self._run_deleted_patrons_single_iteration(
                    sierra_client, redshift_client)
            else:
                last_record = self._run_active_patrons_single_iteration(
                    mode, sierra_client, redshift_client)
            self.logger.info(
                'Finished processing {mode} patrons batch {batch}'.format(
                    mode=mode, batch=batch_number))

            # Cache the new state in S3
            self._set_poller_state(mode, last_record)

            # Check if processing is complete
            reached_max_batches = self.has_max_batches and batch_number >= int(
                os.environ['MAX_BATCHES'])
            no_more_records = (last_record.name + 1) < int(
                os.environ['SIERRA_BATCH_SIZE'])
            finished = reached_max_batches or no_more_records
            batch_number += 1

        # Close database connections
        self.logger.info((
            'Finished processing {mode} patrons session with {batch} batches, '
            'closing database connections').format(
            mode=mode, batch=batch_number-1))
        sierra_client.close_connection()
        if (mode == PipelineMode.UPDATED_PATRONS):
            redshift_client.close_connection()

    def _run_active_patrons_single_iteration(
            self, mode, sierra_client, redshift_client):
        """
        Runs the full pipeline a single time for either newly created
        patrons or for recently updated patrons
        """
        # Get data from Sierra
        query = (
            build_new_patrons_query(self.poller_state['creation_dt'])
            if mode == PipelineMode.NEW_PATRONS else
            build_updated_patrons_query(self.poller_state['update_dt']))
        sierra_raw_data = sierra_client.execute_query(query)
        unprocessed_sierra_df = pd.DataFrame(
            data=sierra_raw_data, columns=_SIERRA_COLUMNS_MAP[mode],
            dtype='string')

        # Remove records for any patron id that has already been processed and
        # update the total set of processed ids
        unseen_records_mask = ~unprocessed_sierra_df[
            'patron_id_plaintext'].isin(self.processed_ids)
        processed_df = unprocessed_sierra_df[unseen_records_mask]
        self.processed_ids.update(processed_df['patron_id_plaintext'])

        # Reduce the dataframe to only one row per patron_id, keeping the row
        # with the lowest display_order and patron_record_address_type_id.
        distinct_records_mask = ~processed_df.duplicated('patron_id_plaintext',
                                                         keep='first')
        processed_df = processed_df[distinct_records_mask].reset_index(
            drop=True)

        # Obfuscate the patron addresses using bcrypt
        self.logger.info('Concatenating and obfuscating ({}) addresses'.format(
            len(processed_df)))
        processed_df['address_hash_plaintext'] = (
            processed_df['patron_id_plaintext'] + '_' +
            processed_df['address'].fillna('') + '_' +
            processed_df['city'].fillna('') + '_' +
            processed_df['region'].fillna('') + '_' +
            processed_df['postal_code'].fillna(''))
        with ThreadPoolExecutor() as executor:
            processed_df['address_hash'] = list(executor.map(
                obfuscate, processed_df['address_hash_plaintext']))

        # For every (patron id + address) hash found in Redshift, use the geoid
        # and obfuscated patron id found there
        if mode == PipelineMode.UPDATED_PATRONS:
            processed_df = self._find_known_addreses(processed_df,
                                                     redshift_client)
        else:
            processed_df[['patron_id', 'geoid']] = None

        # For every row not already in Redshift, geocode it and obfuscate the
        # patron id
        unknown_patrons_df = processed_df[pd.isnull(processed_df['patron_id'])]
        unknown_patrons_df = self._process_unknown_patrons(unknown_patrons_df)
        processed_df.update(unknown_patrons_df)

        # Modify the data to match what's expected by the PatronInfo Avro
        # schema
        processed_df = processed_df.astype('string')
        processed_df[['ptype_code', 'pcode3']] = processed_df[
            ['ptype_code', 'pcode3']].apply(
                pd.to_numeric, errors='coerce').astype('Int64')
        processed_df['postal_code'] = processed_df['postal_code'].str.slice(
            stop=5)
        if (mode == PipelineMode.NEW_PATRONS):
            processed_df['creation_date_et'] = processed_df[
                'creation_timestamp'].apply(self._convert_dt_to_est_date)\
                .astype('string')

        # Encode the resulting data and send it to Kinesis
        results_df = processed_df[
            ['patron_id', 'address_hash', 'postal_code', 'geoid',
             'creation_date_et', 'deletion_date_et', 'circ_active_date_et',
             'ptype_code', 'pcode3', 'patron_home_library_code']]
        encoded_records = self.avro_encoder.encode_batch(results_df)
        self.kinesis_client.send_records(encoded_records)

        return unprocessed_sierra_df.iloc[-1]

    def _run_deleted_patrons_single_iteration(
            self, sierra_client, redshift_client):
        """
        Runs the full pipeline a single time for recently deleted patrons
        """
        # Get data from Sierra
        query = build_deleted_patrons_query(self.poller_state['deletion_date'])
        sierra_raw_data = sierra_client.execute_query(query)
        unprocessed_sierra_df = pd.DataFrame(
            data=sierra_raw_data, dtype='string',
            columns=_SIERRA_COLUMNS_MAP[PipelineMode.DELETED_PATRONS])

        # Remove records for any patron id that has already been processed
        unseen_records_mask = ~unprocessed_sierra_df[
            'patron_id_plaintext'].isin(self.processed_ids)
        processed_df = unprocessed_sierra_df[unseen_records_mask].reset_index(
            drop=True)

        # Obfuscate the patron ids using bcrypt
        self.logger.info('Obfuscating ({}) patron ids'.format(
            len(processed_df)))
        with ThreadPoolExecutor() as executor:
            processed_df['patron_id'] = list(executor.map(
                obfuscate, processed_df['patron_id_plaintext']))

        # Take the existing data in Redshift for each deleted patron and merge
        # it with the deletion date
        processed_df = self._find_deleted_patrons(processed_df,
                                                  redshift_client)

        # Modify the data to match what's expected by the PatronInfo Avro
        # schema
        processed_df = processed_df.astype('string')
        processed_df[['ptype_code', 'pcode3']] = processed_df[
            ['ptype_code', 'pcode3']].apply(
                pd.to_numeric, errors='coerce').astype('Int64')

        # Encode the resulting data and send it to Kinesis
        results_df = processed_df[
            ['patron_id', 'address_hash', 'postal_code', 'geoid',
             'creation_date_et', 'deletion_date_et', 'circ_active_date_et',
             'ptype_code', 'pcode3', 'patron_home_library_code']]
        encoded_records = self.avro_encoder.encode_batch(results_df)
        self.kinesis_client.send_records(encoded_records)

        return unprocessed_sierra_df.iloc[-1]

    def _find_known_addreses(self, all_patrons_df, redshift_client):
        """
        Checks if any of the (patron id + address) hashes already appear in
        Redshift. If they do, take the geoid and obfuscated patron id from
        Redshift and join it with the original Sierra dataframe.
        """
        address_hashes_str = ','.join(
            str(all_patrons_df['address_hash'].values)[1:-1].split())
        redshift_raw_data = redshift_client.execute_query(
            build_redshift_address_query(address_hashes_str))
        redshift_df = pd.DataFrame(
            data=redshift_raw_data, dtype='string',
            columns=['address_hash', 'patron_id', 'geoid'])

        redshift_df['address_hash'] = redshift_df['address_hash'].str.encode(
            'utf-8')
        redshift_df['patron_id'] = redshift_df['patron_id'].str.encode('utf-8')
        new_all_patrons_df = all_patrons_df.merge(redshift_df, how='left',
                                                  on='address_hash')
        return new_all_patrons_df

    def _find_deleted_patrons(self, deleted_patrons_df, redshift_client):
        """
        Finds the Redshift data for recently deleted patrons and joins it with
        the deletion date from Sierra.
        """
        patron_ids_str = ','.join(
            str(deleted_patrons_df['patron_id'].values)[1:-1].split())
        redshift_raw_data = redshift_client.execute_query(
            build_redshift_patron_query(patron_ids_str))
        redshift_df = pd.DataFrame(
            data=redshift_raw_data, dtype='string', columns=_REDSHIFT_COLUMNS)

        redshift_df['address_hash'] = redshift_df['address_hash'].str.encode(
            'utf-8')
        redshift_df['patron_id'] = redshift_df['patron_id'].str.encode('utf-8')
        full_patrons_df = deleted_patrons_df.merge(redshift_df, how='left',
                                                   on='patron_id')
        return full_patrons_df

    def _process_unknown_patrons(self, unknown_patrons_df):
        """
        Takes a dataframe of patrons whose addresses have not already been
        geocoded, sends them to the geocoder API and obfuscates their patron
        ids
        """
        # Get geoids from geocoder API and join with processed Sierra data
        unknown_patrons_df_copy = unknown_patrons_df.copy()
        geoids = self.geocoder_client.get_geoids(unknown_patrons_df_copy)
        unknown_patrons_df_copy.update(geoids)

        # Obfuscate the patron ids using bcrypt
        self.logger.info('Obfuscating ({}) patron ids'.format(
            len(unknown_patrons_df_copy)))
        with ThreadPoolExecutor() as executor:
            unknown_patrons_df_copy['patron_id'] = list(executor.map(
                obfuscate, unknown_patrons_df_copy['patron_id_plaintext']))
        return unknown_patrons_df_copy

    def _get_poller_state(self, batch_number):
        """
        Retrieves the poller state from the S3 cache, the config, or the local
        memory
        """
        if os.environ.get('IGNORE_CACHE', False) != 'True':
            return self.s3_client.fetch_state()
        elif batch_number == 1:
            return {'creation_dt': os.environ.get('STARTING_CREATION_DT',
                                                  '2020-01-01 00:00:00-05'),
                    'update_dt': os.environ.get('STARTING_UPDATE_DT',
                                                '2020-01-01 00:00:00-05'),
                    'deletion_date': os.environ.get('STARTING_DELETION_DATE',
                                                    '2020-01-01')}
        else:
            return self.poller_state

    def _set_poller_state(self, mode, last_processed_data):
        """
        Sets the poller state locally and in the S3 cache if appropriate
        """
        if (mode == PipelineMode.NEW_PATRONS):
            self.poller_state['creation_dt'] = last_processed_data[
                'creation_timestamp']
        elif (mode == PipelineMode.UPDATED_PATRONS):
            self.poller_state['update_dt'] = last_processed_data[
                'last_updated_timestamp']
        elif (mode == PipelineMode.DELETED_PATRONS):
            self.poller_state['deletion_date'] = last_processed_data[
                'deletion_date_et']

        if os.environ.get('IGNORE_CACHE', False) != 'True':
            self.s3_client.set_state(self.poller_state)

    def _convert_dt_to_est_date(self, string_input):
        """
        Converts a string datetime with a timezone to the corresponding EST
        date. In order to match what postgresql does when "AT TIME ZONE 'EST'"
        is used, we always use EST, even when the date implies the timezone
        should be EDT.
        """
        dt_input = datetime.datetime.strptime(
            string_input, '%Y-%m-%d %H:%M:%S%z')
        return dt_input.astimezone(_EST_TIMEZONE).strftime('%Y-%m-%d')


class PipelineControllerError(Exception):
    def __init__(self, message=None):
        self.message = message
