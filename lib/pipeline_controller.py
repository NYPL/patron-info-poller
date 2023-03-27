import datetime
import json
import os
import pandas as pd
import warnings

from concurrent.futures import ProcessPoolExecutor
from enum import Enum
from helpers.address_helper import reformat_malformed_address
from helpers.query_helper import (build_deleted_patrons_query,
                                  build_new_patrons_query,
                                  build_redshift_address_query,
                                  build_redshift_patron_query,
                                  build_updated_patrons_query)
from lib import CensusGeocoderApiClient, NycGeocoderClient
from nypl_py_utils.classes.avro_encoder import AvroEncoder
from nypl_py_utils.classes.kinesis_client import KinesisClient
from nypl_py_utils.classes.postgresql_client import PostgreSQLClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.classes.s3_client import S3Client
from nypl_py_utils.functions.log_helper import create_log
from nypl_py_utils.functions.obfuscation_helper import obfuscate


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

        self.census_geocoder_client = CensusGeocoderApiClient()
        self.nyc_geocoder_client = NycGeocoderClient()
        self.s3_client = S3Client(
            os.environ['S3_BUCKET'], os.environ['S3_RESOURCE'])
        self.avro_encoder = AvroEncoder(os.environ['PATRON_INFO_SCHEMA_URL'])
        self.kinesis_client = KinesisClient(
            os.environ['KINESIS_STREAM_ARN'],
            int(os.environ['KINESIS_BATCH_SIZE']))
        self.sierra_client = PostgreSQLClient(
            os.environ['SIERRA_DB_HOST'], os.environ['SIERRA_DB_PORT'],
            os.environ['SIERRA_DB_NAME'], os.environ['SIERRA_DB_USER'],
            os.environ['SIERRA_DB_PASSWORD'])
        self.redshift_client = RedshiftClient(
            os.environ['REDSHIFT_DB_HOST'],
            os.environ['REDSHIFT_DB_NAME'],
            os.environ['REDSHIFT_DB_USER'],
            os.environ['REDSHIFT_DB_PASSWORD'])

        self.has_max_batches = 'MAX_BATCHES' in os.environ
        self.poller_state = None
        self.processed_ids = set()

    def run_pipeline(self, mode):
        """Runs the full pipeline in the given PipelineMode mode."""
        if mode not in PipelineMode:
            raise PipelineControllerError(
                'run_pipeline called with bad pipeline mode: {}'.format(mode))

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
                last_record = self._run_deleted_patrons_single_iteration()
            else:
                last_record = self._run_active_patrons_single_iteration(mode)
            self.logger.info(
                'Finished processing {mode} patrons batch {batch}'.format(
                    mode=mode, batch=batch_number))

            # Cache the new state in S3 if necessary and check for more records
            if last_record is not None:
                self._set_poller_state(mode, last_record)
                no_more_records = (last_record.name + 1) < int(
                    os.environ['SIERRA_BATCH_SIZE'])
            else:
                no_more_records = True

            # Check if processing is complete
            reached_max_batches = self.has_max_batches and batch_number >= int(
                os.environ['MAX_BATCHES'])
            finished = reached_max_batches or no_more_records
            batch_number += 1

        self.logger.info((
            'Finished processing {mode} patrons session with {batch} batches, '
            'closing AWS connections').format(mode=mode, batch=batch_number-1))
        self.s3_client.close()
        self.kinesis_client.close()

    def _run_active_patrons_single_iteration(self, mode):
        """
        Runs the full pipeline a single time for either newly created
        patrons or for recently updated patrons
        """
        # Get data from Sierra
        query = (
            build_new_patrons_query(self.poller_state['creation_dt'])
            if mode == PipelineMode.NEW_PATRONS else
            build_updated_patrons_query(self.poller_state['update_dt']))
        self.sierra_client.connect()
        sierra_raw_data = self.sierra_client.execute_query(query)
        self.sierra_client.close_connection()
        unprocessed_sierra_df = pd.DataFrame(
            data=sierra_raw_data, columns=_SIERRA_COLUMNS_MAP[mode],
            dtype='string')

        # Remove records for any patron ids that have already been processed
        unseen_records_mask = ~unprocessed_sierra_df[
            'patron_id_plaintext'].isin(self.processed_ids)
        processed_df = unprocessed_sierra_df[unseen_records_mask].reset_index(
            drop=True)

        # If there are no unprocessed patron ids left, return. Otherwise,
        # update the total set of processed ids, ignoring the FutureWarning
        # caused by the pandas update method, which is not relevant to this
        # code.
        if len(processed_df) == 0:
            return None
        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            self.processed_ids.update(processed_df['patron_id_plaintext'])

        # Reduce the dataframe to only one row per patron_id, keeping the row
        # with the lowest display_order and patron_record_address_type_id
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
        with ProcessPoolExecutor() as executor:
            processed_df['address_hash'] = list(executor.map(
                obfuscate, processed_df['address_hash_plaintext']))

        # For every (patron id + address) hash found in Redshift, use the geoid
        # and obfuscated patron id found there
        if mode == PipelineMode.UPDATED_PATRONS:
            processed_df = self._find_known_addreses(processed_df)
        else:
            processed_df[['patron_id', 'geoid']] = None
        processed_df = processed_df.astype('string')

        # For every row not already in Redshift, obfuscate the patron id and
        # geocode it, ignoring the FutureWarning caused by the pandas update
        # method, which is not relevant to this code
        unknown_patrons_df = processed_df[
            pd.isnull(processed_df['patron_id'])][
            ['address', 'city', 'region', 'postal_code',
             'patron_id_plaintext']]
        geocoded_df = self._process_unknown_patrons(unknown_patrons_df)
        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            processed_df.update(geocoded_df)

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
        encoded_records = self.avro_encoder.encode_batch(
            json.loads(results_df.to_json(orient='records')))
        self.kinesis_client.send_records(encoded_records)

        return unprocessed_sierra_df.iloc[-1]

    def _run_deleted_patrons_single_iteration(self):
        """
        Runs the full pipeline a single time for recently deleted patrons
        """
        # Get data from Sierra
        query = build_deleted_patrons_query(self.poller_state['deletion_date'])
        self.sierra_client.connect()
        sierra_raw_data = self.sierra_client.execute_query(query)
        self.sierra_client.close_connection()
        unprocessed_sierra_df = pd.DataFrame(
            data=sierra_raw_data, dtype='string',
            columns=_SIERRA_COLUMNS_MAP[PipelineMode.DELETED_PATRONS])

        # Remove records for any patron ids that have already been processed
        unseen_records_mask = ~unprocessed_sierra_df[
            'patron_id_plaintext'].isin(self.processed_ids)
        processed_df = unprocessed_sierra_df[unseen_records_mask].reset_index(
            drop=True)

        # If there are no unprocessed patron ids left, return. Otherwise,
        # update the total set of processed ids, ignoring the FutureWarning
        # caused by the pandas update method, which is not relevant to this
        # code.
        if len(processed_df) == 0:
            return None
        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            self.processed_ids.update(processed_df['patron_id_plaintext'])

        # Obfuscate the patron ids using bcrypt
        self.logger.info('Obfuscating ({}) patron ids'.format(
            len(processed_df)))
        with ProcessPoolExecutor() as executor:
            processed_df['patron_id'] = list(executor.map(
                obfuscate, processed_df['patron_id_plaintext']))

        # Take the existing data in Redshift for each deleted patron and merge
        # it with the deletion date
        processed_df = self._find_deleted_patrons(processed_df)

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
        encoded_records = self.avro_encoder.encode_batch(
            json.loads(results_df.to_json(orient='records')))
        self.kinesis_client.send_records(encoded_records)

        return unprocessed_sierra_df.iloc[-1]

    def _find_known_addreses(self, all_patrons_df):
        """
        Checks if any of the (patron id + address) hashes already appear in
        Redshift. If they do, take the geoid and obfuscated patron id from
        Redshift and join it with the original Sierra dataframe.
        """
        address_hashes_str = "','".join(
            all_patrons_df['address_hash'].to_string(index=False).split())
        address_hashes_str = "'" + address_hashes_str + "'"
        self.redshift_client.connect()
        redshift_raw_data = self.redshift_client.execute_query(
            build_redshift_address_query(address_hashes_str))
        self.redshift_client.close_connection()
        redshift_df = pd.DataFrame(
            data=redshift_raw_data, dtype='string',
            columns=['address_hash', 'patron_id', 'geoid'])

        new_all_patrons_df = all_patrons_df.merge(redshift_df, how='left',
                                                  on='address_hash')
        return new_all_patrons_df

    def _find_deleted_patrons(self, deleted_patrons_df):
        """
        Finds the Redshift data for recently deleted patrons and joins it with
        the deletion date from Sierra.
        """
        patron_ids_str = "','".join(
            deleted_patrons_df['patron_id'].to_string(index=False).split())
        patron_ids_str = "'" + patron_ids_str + "'"
        self.redshift_client.connect()
        redshift_raw_data = self.redshift_client.execute_query(
            build_redshift_patron_query(patron_ids_str))
        self.redshift_client.close_connection()
        redshift_df = pd.DataFrame(
            data=redshift_raw_data, dtype='string', columns=_REDSHIFT_COLUMNS)

        full_patrons_df = deleted_patrons_df.merge(redshift_df, how='left',
                                                   on='patron_id')
        return full_patrons_df

    def _process_unknown_patrons(self, unknown_patrons_df):
        """
        Takes a dataframe of patrons whose addresses have not already been
        geocoded, obfuscates their patron ids, sends them to the census
        geocoder API and then, if that's unsuccessful, to the NYC geocoder.
        """
        # Obfuscate the patron ids using bcrypt
        address_df = unknown_patrons_df.copy()
        self.logger.info('Obfuscating ({}) patron ids'.format(
            len(address_df)))
        with ProcessPoolExecutor() as executor:
            address_df['patron_id'] = list(executor.map(
                obfuscate, address_df['patron_id_plaintext']))

        # Get geoids from census geocoder API
        address_df[['address', 'city', 'region', 'postal_code']] = address_df[
            ['address', 'city', 'region', 'postal_code']].replace(
            r'\'|"|\\', '', regex=True).fillna('')
        address_df['full_address'] = (
            address_df['address'] + ' ' + address_df['city'] + ' ' +
            address_df['region'] + ' ' + address_df['postal_code']).str.strip()
        input_df = address_df[address_df['full_address'].str.len() > 0]
        if len(input_df) == 0:
            address_df['geoid'] = None
            return address_df[['patron_id', 'geoid']]
        geoids = self.census_geocoder_client.get_geoids(input_df)

        # For addresses that weren't geocoded, reformat them and try again,
        # ignoring the FutureWarning caused by the pandas update method, which
        # is not relevant to this code. Sending two requests is also
        # recommended by the API because it sometimes erroneously fails to
        # match an address when in batch mode. For more info, see:
        # https://www2.census.gov/geo/pdfs/maps-data/data/Census_Geocoder_FAQ.pdf
        retry_indices = geoids[geoids.isnull()].index
        if len(retry_indices) == 0:
            address_df['geoid'] = geoids
            return address_df[['patron_id', 'geoid']]
        input_df = input_df.loc[retry_indices]
        input_df = input_df.apply(reformat_malformed_address, axis=1)
        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            geoids.update(self.census_geocoder_client.get_geoids(input_df))

        # Send addresses that still aren't geocoded to the NYC geocoder,
        # ignoring the FutureWarning caused by the pandas update method, which
        # is not relevant to this code
        retry_indices = geoids[geoids.isnull()].index
        if len(input_df) == 0:
            address_df['geoid'] = geoids
            return address_df[['patron_id', 'geoid']]
        input_df = input_df.loc[retry_indices]
        input_df = input_df[
            (input_df['house_number'].str.len() > 0) &
            (input_df['street_name'].str.len() > 0) &
            (input_df['postal_code'].str.len() > 0)]
        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            geoids.update(self.nyc_geocoder_client.get_geoids(input_df))

        self.logger.info(
            'Successfully geocoded {success}/{total} non-empty addresses'
            .format(success=len(geoids[geoids.notnull()]), total=len(geoids)))
        address_df['geoid'] = geoids
        return address_df[['patron_id', 'geoid']]

    def _get_poller_state(self, batch_number):
        """
        Retrieves the poller state from the S3 cache, the config, or the local
        memory
        """
        if os.environ.get('IGNORE_CACHE', False) != 'True':
            return self.s3_client.fetch_cache()
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
            self.s3_client.set_cache(self.poller_state)

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
