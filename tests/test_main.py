import datetime
import os
import pandas as pd
import pytest
import main

from pandas.testing import assert_frame_equal
from tests.test_helpers import TestHelpers

_EST_TIMEZONE = datetime.timezone(datetime.timedelta(days=-1, seconds=68400))

_POLLER_STATE_1 = {'creation_dt': '2021-01-03 03:04:05-05',
                   'update_dt': '2021-01-02 02:03:04-04',
                   'deletion_date': '2021-01-01'}

_SIERRA_RESULTS = [[123, 4, 5, 'home_library1', 'city1', 'region1',
                    'postal_code1', 'address1', datetime.date(2021, 1, 1),
                    datetime.date(2021, 1, 2), datetime.date(2021, 1, 3),
                    datetime.datetime(2020, 12, 31, 23, 59, 59,
                                      tzinfo=_EST_TIMEZONE)],
                   [456, 5, 6, 'home_library2', 'city2', 'region2',
                    'postal_code2', 'address2', datetime.date(2021, 2, 1),
                    datetime.date(2021, 2, 2), datetime.date(2021, 2, 3),
                    datetime.datetime(2020, 12, 30, 23, 59, 59,
                                      tzinfo=_EST_TIMEZONE)],
                   [456, 6, 7, 'home_library3', 'city3', 'region3',
                    'postal_code3', 'address3', datetime.date(2021, 3, 1),
                    datetime.date(2021, 3, 2), datetime.date(2021, 3, 3),
                    datetime.datetime(2022, 12, 29, 23, 59, 59,
                                      tzinfo=_EST_TIMEZONE)],
                   [789, None, None, None, None, None,
                    None, None, None,
                    None, None,
                    datetime.datetime(2022, 1, 1, 1, 1, 1,
                                      tzinfo=_EST_TIMEZONE)]]

_GEOCODER_INPUT = pd.DataFrame(
    data=[['123', '4.0', '5.0', 'home_library1', 'city1', 'region1',
           'postal_code1', 'address1', '2021-01-01', '2021-01-02',
           '2021-01-03', '2020-12-31 23:59:59-05:00'],
          ['456', '5.0', '6.0', 'home_library2', 'city2', 'region2',
           'postal_code2', 'address2', '2021-02-01', '2021-02-02',
           '2021-02-03', '2020-12-30 23:59:59-05:00'],
          ['789', None, None, None, None, None,
           None, None, None, None,
           None, '2022-01-01 01:01:01-05:00']],
    dtype='string',
    columns=['patron_id_plaintext', 'ptype_code', 'pcode3',
             'patron_home_library_code', 'city', 'region', 'postal_code',
             'address', 'circ_active_date_et', 'last_updated_date_et',
             'deletion_date_et', 'creation_timestamp'])

_GEOIDS = pd.Series(['67890', None, '12345'], index=[1, 2, 0], name='geoid')

_AVRO_ENCODER_INPUT = pd.DataFrame(
    data=[['obfuscated_1', 'obfuscated_4', 'postal_code1', '12345',
           '2020-12-31', '2021-01-03', '2021-01-01', 4, 5, 'home_library1'],
          ['obfuscated_2', 'obfuscated_5', 'postal_code2', '67890',
          '2020-12-30', '2021-02-03', '2021-02-01', 5, 6, 'home_library2'],
          ['obfuscated_3', 'obfuscated_6', None, None,
          '2022-01-01', None, None, None, None, None]],
    columns=['patron_id', 'address_hash', 'postal_code', 'geoid',
             'creation_date_et', 'deletion_date_et', 'circ_active_date_et',
             'ptype_code', 'pcode3', 'patron_home_library_code']).astype(
    {'postal_code': 'string',
     'deletion_date_et': 'string',
     'circ_active_date_et': 'string',
     'ptype_code': 'Int64',
     'pcode3': 'Int64',
     'patron_home_library_code': 'string'})

_ENCODED_RECORDS = [b'encoded_1', b'encoded_2', b'encoded_3']


class TestMain:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_helpers(self, mocker):
        mocker.patch('main.load_env_file')
        mocker.patch('main.obfuscate', side_effect=[
                     'obfuscated_{}'.format(n) for n in range(1, 7)])
        mocker.patch('main.build_new_patrons_query', return_value='TEST QUERY')

    @pytest.fixture
    def test_s3_client(self, mocker):
        mock_s3_client = mocker.MagicMock()
        mock_s3_client.fetch_state.return_value = _POLLER_STATE_1
        mocker.patch('main.S3Client', return_value=mock_s3_client)
        return mock_s3_client

    @pytest.fixture
    def test_sierra_client(self, mocker):
        mock_sierra_client = mocker.MagicMock()
        mock_sierra_client.execute_query.return_value = _SIERRA_RESULTS
        mocker.patch('main.DbClient', return_value=mock_sierra_client)
        return mock_sierra_client

    @pytest.fixture
    def test_geocoder_client(self, mocker):
        mock_geocoder_client = mocker.MagicMock()
        mock_geocoder_client.get_geoids.return_value = _GEOIDS
        mocker.patch('main.GeocoderApiClient',
                     return_value=mock_geocoder_client)
        return mock_geocoder_client

    @pytest.fixture
    def test_avro_encoder(self, mocker):
        mock_avro_encoder = mocker.MagicMock()
        mock_avro_encoder.encode_batch.return_value = _ENCODED_RECORDS
        mocker.patch('main.AvroEncoder', return_value=mock_avro_encoder)
        return mock_avro_encoder

    @pytest.fixture
    def test_kinesis_client(self, mocker):
        mock_kinesis_client = mocker.MagicMock()
        mocker.patch('main.KinesisClient', return_value=mock_kinesis_client)
        return mock_kinesis_client

    def test_main(
            self, test_helpers, test_s3_client, test_sierra_client,
            test_geocoder_client, test_avro_encoder, test_kinesis_client):
        os.environ['SIERRA_BATCH_SIZE'] = '3'
        os.environ['MAX_BATCHES'] = '1'

        main.main()

        test_s3_client.fetch_state.assert_called_once()
        test_sierra_client.execute_query.assert_called_once_with('TEST QUERY')

        # The input check implicitly tests that the raw data is loaded into a
        # dataframe properly and that duplicate patron ids have been removed
        test_geocoder_client.get_geoids.assert_called_once()
        assert_frame_equal(
            test_geocoder_client.get_geoids.call_args.args[0], _GEOCODER_INPUT)

        # The input check implicitly tests that the geoids have been joined,
        # the datatypes have been converted, and the id and address have been
        # obfuscated
        test_avro_encoder.encode_batch.assert_called_once()
        assert_frame_equal(
            test_avro_encoder.encode_batch.call_args.args[0],
            _AVRO_ENCODER_INPUT)

        test_kinesis_client.send_records.assert_called_once_with(
            _ENCODED_RECORDS)
        test_s3_client.set_state.assert_called_once()
        test_sierra_client.close_connection.assert_called_once()
