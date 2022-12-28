import datetime
import os
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from helpers.config_helper import load_env_file
from helpers.log_helper import create_log
from helpers.obfuscation_helper import obfuscate
from helpers.query_helper import build_new_patrons_query
from lib import (AvroEncoder, DbClient, DbMode, GeocoderApiClient,
                 KinesisClient, S3Client)

_EST_TIMEZONE = datetime.timezone(datetime.timedelta(days=-1, seconds=68400))


def main():
    load_env_file(os.environ['ENVIRONMENT'], 'config/{}.yaml')
    logger = create_log(__name__)

    # Set up all the clients
    sierra_client = DbClient(DbMode.SIERRA)
    geocoder_client = GeocoderApiClient()
    avro_encoder = AvroEncoder()
    kinesis_client = KinesisClient()
    s3_client = S3Client()

    has_max_batches = 'MAX_BATCHES' in os.environ
    batch_number = 1
    poller_state = None
    finished = False
    while not finished:
        # Retrieve the query parameters to use for this batch
        poller_state = _get_poller_state(s3_client, poller_state, batch_number)

        # Get data from Sierra
        logger.info('Begin processing batch {batch} with state {state}'.format(
            batch=batch_number, state=poller_state))
        sierra_raw_data = sierra_client.execute_query(
            build_new_patrons_query(poller_state['creation_dt']))
        unprocessed_sierra_df = pd.DataFrame(
            data=sierra_raw_data, dtype='string',
            columns=['patron_id_plaintext', 'ptype_code', 'pcode3',
                     'patron_home_library_code', 'city', 'region',
                     'postal_code', 'address', 'circ_active_date_et',
                     'last_updated_date_et', 'deletion_date_et',
                     'creation_timestamp'])

        # Reduce the dataframe to only one row per patron_id, keeping the row
        # with the lowest display_order and patron_record_address_type_id.
        should_keep_mask = ~unprocessed_sierra_df.duplicated(
            'patron_id_plaintext', keep='first')
        processed_df = unprocessed_sierra_df[should_keep_mask].reset_index(
            drop=True)

        # Get geoids from geocoder API and join with Sierra data
        geoids = geocoder_client.get_geoids(processed_df)
        processed_df = processed_df.join(geoids)

        # Modify data to match what's expected by the PatronInfo Avro schema
        processed_df['ptype_code'] = pd.to_numeric(
            processed_df['ptype_code'], errors='coerce').astype('Int64')
        processed_df['pcode3'] = pd.to_numeric(
            processed_df['pcode3'], errors='coerce').astype('Int64')
        processed_df['creation_date_et'] = processed_df[
            'creation_timestamp'].apply(_convert_dt_to_est_date)

        # Obfuscate the patron ids and addresses using bcrypt
        logger.info('Obfuscating ({}) patron ids'.format(len(processed_df)))
        with ThreadPoolExecutor() as executor:
            processed_df['patron_id'] = list(executor.map(
                obfuscate, processed_df['patron_id_plaintext']))

        logger.info('Concatenating and obfuscating ({}) addresses'.format(
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

        # Encode the resulting data and send it to Kinesis
        results_df = processed_df[['patron_id', 'address_hash', 'postal_code',
                                   'geoid', 'creation_date_et',
                                   'deletion_date_et', 'circ_active_date_et',
                                   'ptype_code', 'pcode3',
                                   'patron_home_library_code']]
        encoded_records = avro_encoder.encode_batch(results_df)
        kinesis_client.send_records(encoded_records)

        # Cache the new state in S3
        poller_state['creation_dt'] = unprocessed_sierra_df[
            'creation_timestamp'].iat[-1]
        if os.environ.get('IGNORE_CACHE', False) != 'True':
            s3_client.set_state(poller_state)
        logger.info('Finished processing batch {}'.format(batch_number))

        # Check if processing is complete
        reached_max_batches = has_max_batches and batch_number >= int(
            os.environ['MAX_BATCHES'])
        no_more_records = len(unprocessed_sierra_df) < int(
            os.environ['SIERRA_BATCH_SIZE'])
        finished = reached_max_batches or no_more_records
        batch_number += 1

    # Close database connections
    logger.info(
        ('Finished processing session with {} batches, closing database '
         'connections').format(batch_number-1))
    sierra_client.close_connection()


def _get_poller_state(s3_client, local_state, batch_number):
    """
    Retrieves the poller state from the cache, the config, or the local memory
    """
    if os.environ.get('IGNORE_CACHE', False) != 'True':
        return s3_client.fetch_state()
    elif batch_number == 1:
        return {'creation_dt': os.environ.get('STARTING_CREATION_DT',
                                              '2020-01-01 00:00:00-05'),
                'update_dt': os.environ.get('STARTING_UPDATE_DT',
                                            '2020-01-01 00:00:00-05'),
                'deletion_date': os.environ.get('STARTING_DELETION_DATE',
                                                '2020-01-01')}
    else:
        return local_state


def _convert_dt_to_est_date(string_input):
    """
    Converts a string datetime with a timezone to the corresponding EST date
    """
    dt_input = datetime.datetime.strptime(string_input, '%Y-%m-%d %H:%M:%S%z')
    return dt_input.astimezone(_EST_TIMEZONE).strftime('%Y-%m-%d')


if __name__ == '__main__':
    main()
