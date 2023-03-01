import copy
import datetime
import os
import pandas as pd
import pytest

from lib.pipeline_controller import PipelineController, PipelineMode
from pandas.testing import assert_frame_equal, assert_series_equal
from tests.test_helpers import TestHelpers


_CREATION_DT = '2021-01-{} 01:01:01-05'
_UPDATE_DT = '2021-02-{} 02:02:02-05'
_DELETION_DATE = '2021-03-{}'
_EST_TIMEZONE = datetime.timezone(datetime.timedelta(days=-1, seconds=68400))

_NEW_SIERRA_RESULTS = [
    [123, 4, 5, 'home_library1', 'city1', 'region1', 'postal_code1',
     'address1', datetime.date(2021, 1, 1), datetime.date(2021, 1, 2),
     datetime.date(2021, 1, 3), datetime.datetime(2020, 12, 31, 23, 59, 59,
                                                  tzinfo=_EST_TIMEZONE)],
    [456, 5, 6, 'home_library2', 'city2', 'region2', 'postal_code2',
     'address2', datetime.date(2021, 2, 1), datetime.date(2021, 2, 2),
     datetime.date(2021, 2, 3), datetime.datetime(2020, 12, 30, 23, 59, 59,
                                                  tzinfo=_EST_TIMEZONE)],
    [456, 6, 7, 'home_library3', 'city3', 'region3', 'postal_code3',
     'address3', datetime.date(2021, 3, 1), datetime.date(2021, 3, 2),
     datetime.date(2021, 3, 3), datetime.datetime(2020, 12, 29, 23, 59, 59,
                                                  tzinfo=_EST_TIMEZONE)],
    [789, None, None, None, None, None, None, None, None, None, None,
     datetime.datetime(2020, 12, 28, 23, 59, 59, tzinfo=_EST_TIMEZONE)]]

_UPDATED_SIERRA_RESULTS = [
    [999, 9, 9, 'home_library9', 'city9', 'region9', 'postal_code9',
     'address9', datetime.date(2021, 9, 1), datetime.date(2021, 9, 2),
     datetime.date(2021, 9, 3), datetime.datetime(2020, 12, 1, 23, 59, 59,
                                                  tzinfo=_EST_TIMEZONE)],
    [888, 8, 8, 'home_library8', 'city8', 'region8', 'postal_code8',
     'address8', datetime.date(2021, 8, 1), datetime.date(2021, 8, 2),
     datetime.date(2021, 8, 3), datetime.datetime(2020, 12, 2, 23, 59, 59,
                                                  tzinfo=_EST_TIMEZONE)],
    [777, None, None, None, None, None, None, None, None, None, None,
     datetime.datetime(2022, 7, 7, 7, 7, 7, tzinfo=_EST_TIMEZONE)]]

_DELETED_SIERRA_RESULTS = [
    [111, datetime.date(2022, 1, 1)],
    [222, datetime.date(2022, 2, 2)],
    [333, datetime.date(2022, 3, 3)]]

_BASE_LAST_SIERRA_ROW = pd.Series(
    {'patron_id_plaintext': None, 'ptype_code': None, 'pcode3': None,
     'patron_home_library_code': None, 'city': None, 'region': None,
     'postal_code': None, 'address': None, 'circ_active_date_et': None,
     'deletion_date_et': None},
    dtype='string')

_LAST_NEW_SIERRA_ROW = pd.concat([
    _BASE_LAST_SIERRA_ROW,
    pd.Series({'last_updated_date_et': None,
               'creation_timestamp': '2020-12-28 23:59:59-05:00'},
              dtype='string')])\
    .rename(3)
_LAST_NEW_SIERRA_ROW.loc['patron_id_plaintext'] = '789'

_LAST_UPDATED_SIERRA_ROW = pd.concat([
    _BASE_LAST_SIERRA_ROW,
    pd.Series({'creation_date_et': None,
               'last_updated_timestamp': '2022-07-07 07:07:07-05:00'},
              dtype='string')])\
    .rename(6)
_LAST_UPDATED_SIERRA_ROW.loc['patron_id_plaintext'] = '777'

_LAST_DELETED_SIERRA_ROW = pd.Series(
    {'patron_id_plaintext': '333', 'deletion_date_et': '2022-03-03'},
    dtype='string',
    name=2)

_REDSHIFT_ADDRESS_RESULTS = [
    ['addr_hash_9', 'obfuscated_patron_9', '99999999999'],
    ['addr_hash_8', 'obfuscated_patron_8', '88888888888']]

_REDSHIFT_PATRON_RESULTS = [
    ['obfuscated_patron_1', 'addr_hash_1', '11111', '11111111111',
     datetime.date(2021, 1, 1), datetime.date(2021, 6, 1), 1, 2, 'aa'],
    ['obfuscated_patron_3', 'addr_hash_3', '33333', '33333333333',
     datetime.date(2021, 3, 3), datetime.date(2021, 6, 3), 3, 4, 'cc'],]

_NEW_GEOCODER_INPUT_COLUMNS = [
    'patron_id_plaintext', 'ptype_code', 'pcode3', 'patron_home_library_code',
    'city', 'region', 'postal_code', 'address', 'circ_active_date_et',
    'deletion_date_et', 'last_updated_date_et', 'creation_timestamp',
    'address_hash_plaintext', 'address_hash', 'patron_id', 'geoid']

_NEW_GEOCODER_INPUT = pd.DataFrame(
    data=[['123', '4.0', '5.0', 'home_library1', 'city1', 'region1',
           'postal_code1', 'address1', '2021-01-01', '2021-01-02',
           '2021-01-03', '2020-12-31 23:59:59-05:00',
           '123_address1_city1_region1_postal_code1', 'obfuscated_1', None,
           None],
          ['456', '5.0', '6.0', 'home_library2', 'city2', 'region2',
           'postal_code2', 'address2', '2021-02-01', '2021-02-02',
           '2021-02-03', '2020-12-30 23:59:59-05:00',
           '456_address2_city2_region2_postal_code2', 'obfuscated_2', None,
           None],
          ['789', None, None, None, None, None, None, None, None, None, None,
           '2020-12-28 23:59:59-05:00', '789____', 'obfuscated_3', None,
           None]],
    columns=_NEW_GEOCODER_INPUT_COLUMNS,
    dtype='string')

_UPDATED_GEOCODER_INPUT = _NEW_GEOCODER_INPUT.rename(
    columns={'last_updated_date_et': 'creation_date_et',
             'creation_timestamp': 'last_updated_timestamp'})

_GEOIDS = pd.Series(['67890', None, '12345'], index=[1, 2, 0], name='geoid')

_NEW_AVRO_ENCODER_INPUT = [
    {'patron_id': 'obfuscated_4', 'address_hash': 'obfuscated_1',
     'postal_code': 'posta', 'geoid': '12345',
     'creation_date_et': '2020-12-31', 'deletion_date_et': '2021-01-02',
     'circ_active_date_et': '2021-01-01', 'ptype_code': 4, 'pcode3': 5,
     'patron_home_library_code': 'home_library1'},
    {'patron_id': 'obfuscated_5', 'address_hash': 'obfuscated_2',
     'postal_code': 'posta', 'geoid': '67890',
     'creation_date_et': '2020-12-30', 'deletion_date_et': '2021-02-02',
     'circ_active_date_et': '2021-02-01', 'ptype_code': 5, 'pcode3': 6,
     'patron_home_library_code': 'home_library2'},
    {'patron_id': 'obfuscated_6', 'address_hash': 'obfuscated_3',
     'postal_code': None, 'geoid': None, 'creation_date_et': '2020-12-28',
     'deletion_date_et': None, 'circ_active_date_et': None, 'ptype_code': None,
     'pcode3': None, 'patron_home_library_code': None}]

_UPDATED_AVRO_ENCODER_INPUT = copy.deepcopy(_NEW_AVRO_ENCODER_INPUT)
_UPDATED_AVRO_ENCODER_INPUT[0]['creation_date_et'] = '2021-01-03'
_UPDATED_AVRO_ENCODER_INPUT[1]['creation_date_et'] = '2021-02-03'
_UPDATED_AVRO_ENCODER_INPUT[2]['creation_date_et'] = None
_UPDATED_AVRO_ENCODER_INPUT += [
    {'patron_id': 'obfuscated_patron_9', 'address_hash': 'addr_hash_9',
     'postal_code': 'posta', 'geoid': '99999999999',
     'creation_date_et': '2021-09-03', 'deletion_date_et': '2021-09-02',
     'circ_active_date_et': '2021-09-01', 'ptype_code': 9, 'pcode3': 9,
     'patron_home_library_code': 'home_library9'},
    {'patron_id': 'obfuscated_patron_8', 'address_hash': 'addr_hash_8',
     'postal_code': 'posta', 'geoid': '88888888888',
     'creation_date_et': '2021-08-03', 'deletion_date_et': '2021-08-02',
     'circ_active_date_et': '2021-08-01', 'ptype_code': 8, 'pcode3': 8,
     'patron_home_library_code': 'home_library8'}]

_DELETED_AVRO_ENCODER_INPUT = [
    {'patron_id': 'obfuscated_patron_1', 'address_hash': 'addr_hash_1',
     'postal_code': '11111', 'geoid': '11111111111',
     'creation_date_et': '2021-01-01', 'deletion_date_et': '2022-01-01',
     'circ_active_date_et': '2021-06-01',  'ptype_code': 1, 'pcode3': 2,
     'patron_home_library_code': 'aa'},
    {'patron_id': 'obfuscated_patron_2', 'address_hash': None,
     'postal_code': None, 'geoid': None, 'creation_date_et': None,
     'deletion_date_et': '2022-02-02', 'circ_active_date_et': None,
     'ptype_code': None, 'pcode3': None, 'patron_home_library_code': None},
    {'patron_id': 'obfuscated_patron_3', 'address_hash': 'addr_hash_3',
     'postal_code': '33333', 'geoid': '33333333333',
     'creation_date_et': '2021-03-03', 'deletion_date_et': '2022-03-03',
     'circ_active_date_et': '2021-06-03', 'ptype_code': 3, 'pcode3': 4,
     'patron_home_library_code': 'cc'}]

_ENCODED_RECORDS = [b'encoded_1', b'encoded_2',  b'encoded_3', b'encoded_4',
                    b'encoded_5']


class TestMain:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        def mock_multiprocess_mapper(method, input_df):
            return [method(row) for row in input_df]

        mocker.patch('lib.pipeline_controller.S3Client')
        mocker.patch('lib.pipeline_controller.PostgreSQLClient')
        mocker.patch('lib.pipeline_controller.RedshiftClient')
        mocker.patch('lib.pipeline_controller.GeocoderApiClient')
        mocker.patch('lib.pipeline_controller.KinesisClient')
        mocker.patch('lib.pipeline_controller.AvroEncoder')

        mock_pool = mocker.MagicMock()
        mock_pool.map.side_effect = mock_multiprocess_mapper
        mocker.patch('lib.pipeline_controller.ProcessPoolExecutor.__enter__',
                     return_value=mock_pool)
        return PipelineController()

    def test_run_new_patrons_pipeline(self, test_instance, mocker):
        os.environ['MAX_BATCHES'] = '3'
        test_instance.has_max_batches = True

        mocker.patch(
            'lib.pipeline_controller.PipelineController._run_active_patrons_single_iteration',  # noqa: E501
            side_effect=[pd.Series(
                {'creation_timestamp': _CREATION_DT.format(i)}, name=2)
                for i in range(2, 5)])

        test_instance.s3_client.fetch_cache.side_effect = [
            {'creation_dt': _CREATION_DT.format(i),
             'update_dt': _UPDATE_DT.format(1),
             'deletion_date': _DELETION_DATE.format(1)} for i in range(1, 4)]

        test_instance.run_pipeline(PipelineMode.NEW_PATRONS)

        assert test_instance.s3_client.fetch_cache.call_count == 3
        assert test_instance._run_active_patrons_single_iteration.call_count \
            == 3
        test_instance._run_active_patrons_single_iteration.assert_called_with(
            PipelineMode.NEW_PATRONS)
        test_instance.s3_client.set_cache.assert_has_calls([mocker.call(
            {'creation_dt': _CREATION_DT.format(i),
             'update_dt': _UPDATE_DT.format(1),
             'deletion_date': _DELETION_DATE.format(1)}) for i in range(2, 5)]
        )
        test_instance.s3_client.close.assert_called_once()
        test_instance.kinesis_client.close.assert_called_once()
        test_instance.sierra_client.close_connection.assert_called_once()
        del os.environ['MAX_BATCHES']

    def test_run_updated_patrons_pipeline(self, test_instance, mocker):
        mocker.patch(
            'lib.pipeline_controller.PipelineController._run_active_patrons_single_iteration',  # noqa: E501
            side_effect=[
                pd.Series(
                    {'last_updated_timestamp': _UPDATE_DT.format(2)}, name=2),
                pd.Series(
                    {'last_updated_timestamp': _UPDATE_DT.format(3)}, name=2),
                pd.Series(
                    {'last_updated_timestamp': _UPDATE_DT.format(4)}, name=1)])

        test_instance.s3_client.fetch_cache.side_effect = [
            {'creation_dt': _CREATION_DT.format(1),
             'update_dt': _UPDATE_DT.format(i),
             'deletion_date': _DELETION_DATE.format(1)} for i in range(1, 4)]

        test_instance.run_pipeline(PipelineMode.UPDATED_PATRONS)

        assert test_instance.s3_client.fetch_cache.call_count == 3
        assert test_instance._run_active_patrons_single_iteration.call_count \
            == 3
        test_instance._run_active_patrons_single_iteration.assert_called_with(
            PipelineMode.UPDATED_PATRONS)
        test_instance.s3_client.set_cache.assert_has_calls([mocker.call(
            {'creation_dt': _CREATION_DT.format(1),
             'update_dt': _UPDATE_DT.format(i),
             'deletion_date': _DELETION_DATE.format(1)}) for i in range(2, 5)]
        )
        test_instance.s3_client.close.assert_called_once()
        test_instance.kinesis_client.close.assert_called_once()
        test_instance.sierra_client.close_connection.assert_called_once()
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_deleted_patrons_pipeline(self, test_instance, mocker):
        os.environ['MAX_BATCHES'] = '4'
        test_instance.has_max_batches = True

        mocker.patch(
            'lib.pipeline_controller.PipelineController._run_deleted_patrons_single_iteration',  # noqa: E501
            side_effect=[
                pd.Series(
                    {'deletion_date_et': _DELETION_DATE.format(2)}, name=2),
                pd.Series(
                    {'deletion_date_et': _DELETION_DATE.format(3)}, name=2),
                pd.Series(
                    {'deletion_date_et': _DELETION_DATE.format(4)}, name=1)])

        test_instance.s3_client.fetch_cache.side_effect = [
            {'creation_dt': _CREATION_DT.format(1),
             'update_dt': _UPDATE_DT.format(1),
             'deletion_date': _DELETION_DATE.format(i)} for i in range(1, 4)]

        test_instance.run_pipeline(PipelineMode.DELETED_PATRONS)

        assert test_instance.s3_client.fetch_cache.call_count == 3
        assert test_instance._run_deleted_patrons_single_iteration.call_count \
            == 3
        test_instance.s3_client.set_cache.assert_has_calls([mocker.call(
            {'creation_dt': _CREATION_DT.format(1),
             'update_dt': _UPDATE_DT.format(1),
             'deletion_date': _DELETION_DATE.format(i)}) for i in range(2, 5)]
        )
        test_instance.s3_client.close.assert_called_once()
        test_instance.kinesis_client.close.assert_called_once()
        test_instance.sierra_client.close_connection.assert_called_once()
        test_instance.redshift_client.close_connection.assert_called_once()
        del os.environ['MAX_BATCHES']

    def test_run_active_pipeline_no_results(self, test_instance, mocker):
        test_instance.s3_client.fetch_cache.return_value = {
            'creation_dt': _CREATION_DT.format(1),
            'update_dt': _UPDATE_DT.format(1),
            'deletion_date': _DELETION_DATE.format(1)}
        test_instance.sierra_client.execute_query.return_value = []

        mocker.patch('lib.pipeline_controller.build_new_patrons_query',
                     return_value='NEW PATRONS QUERY')

        test_instance.run_pipeline(PipelineMode.NEW_PATRONS)

        assert test_instance.s3_client.fetch_cache.call_count == 1
        assert test_instance.s3_client.set_cache.call_count == 0

    def test_run_deleted_pipeline_no_results(self, test_instance, mocker):
        test_instance.s3_client.fetch_cache.return_value = {
            'creation_dt': _CREATION_DT.format(1),
            'update_dt': _UPDATE_DT.format(1),
            'deletion_date': _DELETION_DATE.format(1)}
        test_instance.sierra_client.execute_query.return_value = []

        mocker.patch('lib.pipeline_controller.build_deleted_patrons_query',
                     return_value='DELETED PATRONS QUERY')

        test_instance.run_pipeline(PipelineMode.DELETED_PATRONS)

        assert test_instance.s3_client.fetch_cache.call_count == 1
        assert test_instance.s3_client.set_cache.call_count == 0

    def test_run_new_patrons_single_iteration(self, test_instance, mocker):
        # This input check implicitly tests that the raw data is loaded into a
        # dataframe properly and that duplicate patron ids have been removed.
        # It must be done in a side_effect function because the real argument
        # to the geocoder is updated, preventing usage of call_args. For more
        # information, see:
        # https://docs.python.org/dev/library/unittest.mock-examples.html#coping-with-mutable-arguments
        def test_geocoder_input(input_df):
            assert_frame_equal(input_df, _NEW_GEOCODER_INPUT)
            return _GEOIDS

        test_instance.poller_state = {
            'creation_dt': _CREATION_DT.format(1),
            'update_dt': _UPDATE_DT.format(1),
            'deletion_date': _DELETION_DATE.format(1)}

        test_instance.sierra_client.execute_query.return_value = \
            _NEW_SIERRA_RESULTS

        test_instance.geocoder_client.get_geoids.side_effect = \
            test_geocoder_input
        test_instance.avro_encoder.encode_batch.return_value = \
            _ENCODED_RECORDS[:3]
        mocker.patch('lib.pipeline_controller.build_new_patrons_query',
                     return_value='NEW PATRONS QUERY')
        mocker.patch('lib.pipeline_controller.obfuscate', side_effect=[
            'obfuscated_{}'.format(i) for i in range(1, 7)])

        assert_series_equal(
            test_instance._run_active_patrons_single_iteration(
                PipelineMode.NEW_PATRONS), _LAST_NEW_SIERRA_ROW)

        test_instance.sierra_client.execute_query.assert_called_once_with(
            'NEW PATRONS QUERY')

        # This input check implicitly tests that the geoids have been joined,
        # the datatypes have been converted, and the ids have been obfuscated
        test_instance.avro_encoder.encode_batch.assert_called_once()
        assert test_instance.avro_encoder.encode_batch.call_args.args[
            0] == _NEW_AVRO_ENCODER_INPUT

        test_instance.kinesis_client.send_records.assert_called_once_with(
            _ENCODED_RECORDS[:3])

    def test_run_updated_patrons_single_iteration(self, test_instance, mocker):
        # This input check implicitly tests that the raw data is loaded into a
        # dataframe properly, that duplicate and previously processed patron
        # ids have been removed, and that addresses contained in Redshift are
        # not included. It must be done in a side_effect function because the
        # real argument to the geocoder is updated, preventing usage of
        # call_args. For more information, see:
        # https://docs.python.org/dev/library/unittest.mock-examples.html#coping-with-mutable-arguments
        def test_geocoder_input(input_df):
            assert_frame_equal(
                input_df, _UPDATED_GEOCODER_INPUT, check_like=True)
            return _GEOIDS

        test_instance.processed_ids = {'777'}
        test_instance.poller_state = {
            'creation_dt': _CREATION_DT.format(1),
            'update_dt': _UPDATE_DT.format(1),
            'deletion_date': _DELETION_DATE.format(1)}

        test_instance.sierra_client.execute_query.return_value = \
            _NEW_SIERRA_RESULTS + _UPDATED_SIERRA_RESULTS
        test_instance.redshift_client.execute_query.return_value = \
            _REDSHIFT_ADDRESS_RESULTS

        test_instance.geocoder_client.get_geoids.side_effect = \
            test_geocoder_input
        test_instance.avro_encoder.encode_batch.return_value = \
            _ENCODED_RECORDS
        mocker.patch('lib.pipeline_controller.build_updated_patrons_query',
                     return_value='UPDATED PATRONS QUERY')
        mocker.patch('lib.pipeline_controller.build_redshift_address_query',
                     return_value='REDSHIFT ADDRESS QUERY')
        mocker.patch('lib.pipeline_controller.obfuscate', side_effect=[
            'obfuscated_1', 'obfuscated_2', 'obfuscated_3', 'addr_hash_9',
            'addr_hash_8', 'obfuscated_4', 'obfuscated_5', 'obfuscated_6'])

        assert_series_equal(
            test_instance._run_active_patrons_single_iteration(
                PipelineMode.UPDATED_PATRONS), _LAST_UPDATED_SIERRA_ROW)

        test_instance.sierra_client.execute_query.assert_called_once_with(
            'UPDATED PATRONS QUERY')
        test_instance.redshift_client.execute_query.assert_called_once_with(
            'REDSHIFT ADDRESS QUERY')

        # This input check implicitly tests that the geoids have been joined,
        # the datatypes have been converted, and the ids have been obfuscated
        test_instance.avro_encoder.encode_batch.assert_called_once()
        assert test_instance.avro_encoder.encode_batch.call_args.args[
            0] == _UPDATED_AVRO_ENCODER_INPUT

        test_instance.kinesis_client.send_records.assert_called_once_with(
            _ENCODED_RECORDS)

    def test_run_deleted_patrons_single_iteration(self, test_instance, mocker):
        test_instance.poller_state = {
            'creation_dt': _CREATION_DT.format(1),
            'update_dt': _UPDATE_DT.format(1),
            'deletion_date': _DELETION_DATE.format(1)}

        test_instance.sierra_client.execute_query.return_value = \
            _DELETED_SIERRA_RESULTS
        test_instance.redshift_client.execute_query.return_value = \
            _REDSHIFT_PATRON_RESULTS

        test_instance.avro_encoder.encode_batch.return_value = \
            _ENCODED_RECORDS[:2]
        mocker.patch('lib.pipeline_controller.build_deleted_patrons_query',
                     return_value='DELETED PATRONS QUERY')
        mocker.patch('lib.pipeline_controller.build_redshift_patron_query',
                     return_value='REDSHIFT PATRON QUERY')
        mocker.patch('lib.pipeline_controller.obfuscate', side_effect=[
            'obfuscated_patron_{}'.format(i) for i in range(1, 4)])

        assert_series_equal(
            test_instance._run_deleted_patrons_single_iteration(),
            _LAST_DELETED_SIERRA_ROW)

        test_instance.sierra_client.execute_query.assert_called_once_with(
            'DELETED PATRONS QUERY')
        test_instance.redshift_client.execute_query.assert_called_once_with(
            'REDSHIFT PATRON QUERY')

        # This input check implicitly tests that the Sierra and Redshift
        # dataframes have been joined and the datatypes have been converted
        test_instance.avro_encoder.encode_batch.assert_called_once()
        assert test_instance.avro_encoder.encode_batch.call_args.args[
            0] == _DELETED_AVRO_ENCODER_INPUT

        test_instance.kinesis_client.send_records.assert_called_once_with(
            _ENCODED_RECORDS[:2])
