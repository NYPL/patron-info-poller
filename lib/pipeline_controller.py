import json
import os
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from helpers.address_helper import reformat_malformed_address
from helpers.pipeline_mode import PipelineMode
from helpers.query_helper import (build_active_patrons_query,
                                  build_deleted_patrons_query,
                                  build_redshift_address_query,
                                  build_redshift_iphlc_query,
                                  build_redshift_patron_query)
from lib import CensusGeocoderApiClient, NycGeocoderClient
from nypl_py_utils.classes.avro_encoder import AvroEncoder
from nypl_py_utils.classes.kinesis_client import KinesisClient
from nypl_py_utils.classes.postgresql_client import PostgreSQLClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.classes.s3_client import S3Client
from nypl_py_utils.functions.log_helper import create_log
from nypl_py_utils.functions.obfuscation_helper import obfuscate


_REDSHIFT_COLUMNS = [
    'patron_id', 'address_hash', 'postal_code', 'geoid', 'creation_date_et',
    'circ_active_date_et', 'ptype_code', 'pcode3', 'patron_home_library_code',
    'initial_patron_home_library_code']
_SIERRA_COLUMNS = [
    'patron_id_plaintext', 'ptype_code', 'pcode3', 'patron_home_library_code',
    'city', 'region', 'postal_code', 'address', 'circ_active_date_et',
    'deletion_date_et', 'last_updated_timestamp', 'creation_timestamp']
_DTYPE_MAP = {
    'patron_id': 'string',
    'address_hash': 'string',
    'postal_code': 'string',
    'geoid': 'string',
    'creation_date_et': 'string',
    'deletion_date_et': 'string',
    'circ_active_date_et': 'string',
    'ptype_code': 'Int64',
    'pcode3': 'Int64',
    'patron_home_library_code': 'string',
    'initial_patron_home_library_code': 'string'}


class PipelineController:
    """
    Class for orchestrating different types of pipeline runs. There are three
    modes: 1) checking for newly created patrons, 2) checking for recently
    updated patrons (who may or may not have changed their address), and 3)
    checking for recently deleted patrons
    """

    def __init__(self, now):
        self.logger = create_log('pipeline_controller')
        self.now = now

        self.census_geocoder_client = CensusGeocoderApiClient()
        self.nyc_geocoder_client = NycGeocoderClient()
        self.avro_encoder = AvroEncoder(os.environ['PATRON_INFO_SCHEMA_URL'])
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
        self.ignore_cache = os.environ.get('IGNORE_CACHE', False) == 'True'
        self.ignore_kinesis = os.environ.get('IGNORE_KINESIS', False) == 'True'
        self.poller_state = None
        self.processed_ids = set()

        if not self.ignore_cache:
            self.s3_client = S3Client(
                os.environ['S3_BUCKET'], os.environ['S3_RESOURCE'])
        if not self.ignore_kinesis:
            self.kinesis_client = KinesisClient(
                os.environ['KINESIS_STREAM_ARN'],
                int(os.environ['KINESIS_BATCH_SIZE']))

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
                if mode == PipelineMode.DELETED_PATRONS:
                    no_more_records = (last_record.name + 1) < int(
                        os.environ['DELETED_PATRON_BATCH_SIZE'])
                else:
                    no_more_records = (last_record.name + 1) < int(
                        os.environ['ACTIVE_PATRON_BATCH_SIZE'])
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
        if not self.ignore_cache:
            self.s3_client.close()
        if not self.ignore_kinesis:
            self.kinesis_client.close()

    def _run_active_patrons_single_iteration(self, mode):
        """
        Runs the full pipeline a single time for either newly created
        patrons or for recently updated patrons
        """
        # Get data from Sierra
        query = build_active_patrons_query(mode, self.poller_state, self.now)
        self.sierra_client.connect()
        sierra_raw_data = self.sierra_client.execute_query(query)
        self.sierra_client.close_connection()
        unprocessed_sierra_df = pd.DataFrame(
            data=sierra_raw_data, columns=_SIERRA_COLUMNS)
        unprocessed_sierra_df['patron_id_plaintext'] = unprocessed_sierra_df[
            'patron_id_plaintext'].astype('Int64').astype('string')

        # Check that the number of records with the same timestamp does not
        # exceed the batch size, which would cause the poller to stall.
        if len(unprocessed_sierra_df) == int(
            os.environ['ACTIVE_PATRON_BATCH_SIZE']) and (
            (mode == PipelineMode.NEW_PATRONS and
                min(unprocessed_sierra_df['creation_timestamp']) ==
                max(unprocessed_sierra_df['creation_timestamp'])) or
            (mode == PipelineMode.UPDATED_PATRONS and
                min(unprocessed_sierra_df['last_updated_timestamp']) ==
                max(unprocessed_sierra_df['last_updated_timestamp']))):
            self.logger.error('Too many records found with the same timestamp')
            raise PipelineControllerError(
                'Too many records found with the same timestamp')

        # Remove records for any patron ids that have already been processed
        unseen_records_mask = ~unprocessed_sierra_df[
            'patron_id_plaintext'].isin(self.processed_ids)
        processed_df = unprocessed_sierra_df[unseen_records_mask].reset_index(
            drop=True)

        # If there are no unprocessed patron ids left, return. Otherwise,
        # update the total set of processed ids.
        if len(processed_df) == 0:
            return None
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
        processed_df[
            ['address', 'city', 'region', 'postal_code']] = processed_df[
                ['address', 'city', 'region', 'postal_code']].astype('string')
        processed_df['address_hash_plaintext'] = (
            processed_df['patron_id_plaintext'] + '_' +
            processed_df['address'].fillna('') + '_' +
            processed_df['city'].fillna('') + '_' +
            processed_df['region'].fillna('') + '_' +
            processed_df['postal_code'].fillna('')).astype('string')
        with ThreadPoolExecutor() as executor:
            processed_df['address_hash'] = list(executor.map(
                obfuscate, processed_df['address_hash_plaintext']))

        # For every (patron id + address) hash found in Redshift, use the geoid
        # and obfuscated patron id found there
        if mode == PipelineMode.UPDATED_PATRONS:
            processed_df = self._find_known_addresses(processed_df)
        else:
            processed_df[['patron_id', 'geoid']] = None
            processed_df[['patron_id', 'geoid']] = processed_df[
                ['patron_id', 'geoid']].astype('string')
            processed_df['initial_patron_home_library_code'] = processed_df[
                'patron_home_library_code']

        # For every row not already in Redshift, obfuscate the patron id and
        # geocode it
        unknown_patrons_df = processed_df[
            pd.isnull(processed_df['patron_id'])][
            ['address', 'city', 'region', 'postal_code',
             'patron_id_plaintext']]
        if len(unknown_patrons_df) > 0:
            geocoded_df = self._process_unknown_patrons(unknown_patrons_df)
            processed_df.update(geocoded_df)
            if mode == PipelineMode.UPDATED_PATRONS:
                unknown_iphlc_mask = pd.isnull(
                    processed_df['initial_patron_home_library_code'])
                iphlc_map = self._find_initial_patron_home_library_codes(
                    processed_df.loc[unknown_iphlc_mask, 'patron_id'])
                processed_df.loc[unknown_iphlc_mask,
                                 'initial_patron_home_library_code'] = \
                    processed_df.loc[unknown_iphlc_mask, 'patron_id'].map(
                        iphlc_map)

        # Modify the data to match what's expected by the PatronInfo Avro
        # schema, encode it, and send it to Kinesis
        processed_df['postal_code'] = processed_df['postal_code'].str.slice(
            stop=5)
        processed_df['creation_date_et'] = processed_df[
            'creation_timestamp'].dt.date

        results_df = processed_df[
            ['patron_id', 'address_hash', 'postal_code', 'geoid',
             'creation_date_et', 'deletion_date_et', 'circ_active_date_et',
             'ptype_code', 'pcode3', 'patron_home_library_code',
             'initial_patron_home_library_code']].astype(_DTYPE_MAP)
        encoded_records = self.avro_encoder.encode_batch(
            json.loads(results_df.to_json(orient='records')))
        if not self.ignore_kinesis:
            self.kinesis_client.send_records(encoded_records)

        return unprocessed_sierra_df.iloc[-1]

    def _run_deleted_patrons_single_iteration(self):
        """
        Runs the full pipeline a single time for recently deleted patrons
        """
        # Get data from Sierra
        query = build_deleted_patrons_query(self.poller_state['deletion_date'],
                                            self.now)
        self.sierra_client.connect()
        sierra_raw_data = self.sierra_client.execute_query(query)
        self.sierra_client.close_connection()
        unprocessed_sierra_df = pd.DataFrame(
            data=sierra_raw_data,
            columns=['patron_id_plaintext', 'deletion_date_et'])
        unprocessed_sierra_df['patron_id_plaintext'] = unprocessed_sierra_df[
            'patron_id_plaintext'].astype('Int64').astype('string')

        # Check that the number of records with the same date does not exceed
        # the batch size, which would cause the poller to stall.
        if len(unprocessed_sierra_df) == int(
                os.environ['DELETED_PATRON_BATCH_SIZE']) and (
            min(unprocessed_sierra_df['deletion_date_et']) ==
                max(unprocessed_sierra_df['deletion_date_et'])):
            self.logger.error('Too many records found with the same date')
            raise PipelineControllerError(
                'Too many records found with the same date')

        # Remove records for any patron ids that have already been processed
        unseen_records_mask = ~unprocessed_sierra_df[
            'patron_id_plaintext'].isin(self.processed_ids)
        processed_df = unprocessed_sierra_df[unseen_records_mask].reset_index(
            drop=True)

        # If there are no unprocessed patron ids left, return. Otherwise,
        # update the total set of processed ids
        if len(processed_df) == 0:
            return None
        self.processed_ids.update(processed_df['patron_id_plaintext'])

        # Obfuscate the patron ids using bcrypt
        self.logger.info('Obfuscating ({}) patron ids'.format(
            len(processed_df)))
        with ThreadPoolExecutor() as executor:
            processed_df['patron_id'] = list(executor.map(
                obfuscate, processed_df['patron_id_plaintext']))

        # Take the existing data in Redshift for each deleted patron and merge
        # it with the deletion date
        processed_df = self._find_deleted_patrons(processed_df)

        # Modify the data to match what's expected by the PatronInfo Avro
        # schema, encode it, and send it to Kinesis
        results_df = processed_df[
            ['patron_id', 'address_hash', 'postal_code', 'geoid',
             'creation_date_et', 'deletion_date_et', 'circ_active_date_et',
             'ptype_code', 'pcode3', 'patron_home_library_code',
             'initial_patron_home_library_code']].astype(_DTYPE_MAP)
        encoded_records = self.avro_encoder.encode_batch(
            json.loads(results_df.to_json(orient='records')))
        if not self.ignore_kinesis:
            self.kinesis_client.send_records(encoded_records)

        return unprocessed_sierra_df.iloc[-1]

    def _find_known_addresses(self, all_patrons_df):
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
            columns=['address_hash', 'patron_id', 'geoid',
                     'initial_patron_home_library_code'])

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
            data=redshift_raw_data, columns=_REDSHIFT_COLUMNS)

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
        with ThreadPoolExecutor() as executor:
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

        # For addresses that weren't geocoded, reformat them and try again.
        # Sending two requests is also recommended by the API because it
        # sometimes erroneously fails to match an address when in batch mode.
        # For more info, see:
        # https://www2.census.gov/geo/pdfs/maps-data/data/Census_Geocoder_FAQ.pdf
        retry_indices = geoids[geoids.isnull()].index
        if len(retry_indices) == 0:
            address_df['geoid'] = geoids
            return address_df[['patron_id', 'geoid']]
        input_df = input_df.loc[retry_indices]
        input_df = input_df.apply(reformat_malformed_address, axis=1)
        geoids.update(self.census_geocoder_client.get_geoids(input_df))

        # Send addresses that still aren't geocoded to the NYC geocoder
        retry_indices = geoids[geoids.isnull()].index
        if len(retry_indices) == 0:
            address_df['geoid'] = geoids
            return address_df[['patron_id', 'geoid']]
        input_df = input_df.loc[retry_indices]
        input_df = input_df[
            (input_df['house_number'].str.len() > 0) &
            (input_df['street_name'].str.len() > 0) &
            (input_df['postal_code'].str.len() > 0)]
        if len(input_df) == 0:
            address_df['geoid'] = geoids
            return address_df[['patron_id', 'geoid']]

        geoids.update(self.nyc_geocoder_client.get_geoids(input_df))
        self.logger.info(
            'Successfully geocoded {success}/{total} non-empty addresses'
            .format(success=len(geoids[geoids.notnull()]), total=len(geoids)))
        address_df['geoid'] = geoids
        return address_df[['patron_id', 'geoid']]

    def _find_initial_patron_home_library_codes(self, unknown_iphlc_series):
        """
        Finds the initial patron home library code for existing patrons whose
        addresses could not be found in Redshift
        """
        patron_ids_str = "','".join(
            unknown_iphlc_series.to_string(index=False).split())
        patron_ids_str = "'" + patron_ids_str + "'"
        self.redshift_client.connect()
        redshift_raw_data = self.redshift_client.execute_query(
            build_redshift_iphlc_query(patron_ids_str))
        self.redshift_client.close_connection()

        iphlc_map = {row[0]: row[1] for row in redshift_raw_data}
        missing_patron_ids = set(unknown_iphlc_series).difference(
            set(iphlc_map.keys()))
        if len(missing_patron_ids) > 0:
            self.logger.warning(
                'The following updated patrons could not be found in '
                'Redshift: {}'.format(sorted(list(missing_patron_ids))))
            for patron_id in missing_patron_ids:
                iphlc_map[patron_id] = None
        return iphlc_map

    def _get_poller_state(self, batch_number):
        """
        Retrieves the poller state from the S3 cache, the config, or the local
        memory
        """
        if not self.ignore_cache:
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
                'creation_timestamp'].isoformat()
        elif (mode == PipelineMode.UPDATED_PATRONS):
            self.poller_state['update_dt'] = last_processed_data[
                'last_updated_timestamp'].isoformat()
        elif (mode == PipelineMode.DELETED_PATRONS):
            self.poller_state['deletion_date'] = last_processed_data[
                'deletion_date_et'].isoformat()
        if not self.ignore_cache:
            self.s3_client.set_cache(self.poller_state)


class PipelineControllerError(Exception):
    def __init__(self, message=None):
        self.message = message
