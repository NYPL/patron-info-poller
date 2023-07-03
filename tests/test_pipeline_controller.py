import copy
import datetime
import numpy as np
import os
import pandas as pd
import pytest

from lib.pipeline_controller import PipelineController, PipelineMode
from pandas.testing import assert_frame_equal, assert_series_equal
from tests.test_helpers import TestHelpers


_CREATION_DT = '2021-01-0{}T01:01:01-05:00'
_UPDATE_DT = '2021-02-0{}T02:02:02-05:00'
_DELETION_DATE = '2021-03-0{}'
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
     'deletion_date_et': None})

_LAST_NEW_SIERRA_ROW = pd.concat([
    _BASE_LAST_SIERRA_ROW,
    pd.Series({
        'last_updated_date_et': None,
        'creation_timestamp': datetime.datetime(
            2020, 12, 28, 23, 59, 59, tzinfo=_EST_TIMEZONE)})]).rename(3)
_LAST_NEW_SIERRA_ROW.loc['patron_id_plaintext'] = '789'

_LAST_UPDATED_SIERRA_ROW = pd.concat([
    _BASE_LAST_SIERRA_ROW,
    pd.Series({
        'creation_date_et': None,
        'last_updated_timestamp': datetime.datetime(
            2022, 7, 7, 7, 7, 7, tzinfo=_EST_TIMEZONE)})]).rename(6)
_LAST_UPDATED_SIERRA_ROW.loc['patron_id_plaintext'] = '777'

_LAST_DELETED_SIERRA_ROW = pd.Series(
    {'patron_id_plaintext': '333',
     'deletion_date_et': datetime.date(2022, 3, 3)},
    name=2)

_REDSHIFT_ADDRESS_RESULTS = [
    ['addr_hash_9', 'obfuscated_patron_9', '99999999999'],
    ['addr_hash_8', 'obfuscated_patron_8', '88888888888']]

_REDSHIFT_PATRON_RESULTS = [
    ['obfuscated_patron_1', 'addr_hash_1', '11111', '11111111111',
     datetime.date(2021, 1, 1), datetime.date(2021, 6, 1), 1, 2, 'aa'],
    ['obfuscated_patron_3', 'addr_hash_3', '33333', '33333333333',
     datetime.date(2021, 3, 3), datetime.date(2021, 6, 3), 3, 4, 'cc'],]

_GEOCODER_INPUT = pd.DataFrame(
    data=[['address1', 'city1', 'region1', 'postal_code1', '123'],
          ['address2', 'city2', 'region2', 'postal_code2', '456'],
          [None, None, None, None, '789']],
    columns=['address', 'city', 'region',
             'postal_code', 'patron_id_plaintext'],
    dtype='string')

_GEOID_OUTPUT = pd.DataFrame(
    {'patron_id': ['obfuscated_5', 'obfuscated_6', 'obfuscated_4'],
     'geoid': ['67890', None, '12345']}, index=[1, 2, 0])

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
     'circ_active_date_et': '2021-06-01', 'ptype_code': 1, 'pcode3': 2,
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

_ENCODED_RECORDS = [b'encoded_1', b'encoded_2', b'encoded_3', b'encoded_4',
                    b'encoded_5']

_ORIGINAL_ADDRESS_DF = pd.DataFrame(
    {'address': ['123 address', None, '456 address', '789 address',
                 '012 address', '345 address', '678 address'],
     'city': ['New York', None, 'Brooklyn', 'C"hicag\\o', 'LA', 'Tokyo',
              'Bronx'],
     'region': ['NY', None, 'NY', 'IL', 'CA', None, 'NY'],
     'postal_code': ['11111', None, '22222', '33333-4444', '55555-6666', '',
                     '77777'],
     'patron_id_plaintext': ['patid1', 'patid2', 'patid3', 'patid4', 'patid5',
                             'patid6', 'patid7']
     }, index=[1, 0, 3, 2, 4, 10, 5])

_CENSUS_INPUT_1 = pd.DataFrame(
    {'address': ['123 address', '456 address', '789 address', '012 address',
                 '345 address', '678 address'],
     'city': ['New York', 'Brooklyn', 'Chicago', 'LA', 'Tokyo', 'Bronx'],
     'region': ['NY', 'NY', 'IL', 'CA', '', 'NY'],
     'postal_code': ['11111', '22222', '33333-4444', '55555-6666', '',
                     '77777'],
     'patron_id_plaintext': ['patid1', 'patid3', 'patid4', 'patid5',
                             'patid6', 'patid7'],
     'patron_id': ['obfuscated_1', 'obfuscated_3', 'obfuscated_4',
                   'obfuscated_5', 'obfuscated_6', 'obfuscated_7'],
     'full_address': ['123 address New York NY 11111',
                      '456 address Brooklyn NY 22222',
                      '789 address Chicago IL 33333-4444',
                      '012 address LA CA 55555-6666',
                      '345 address Tokyo',
                      '678 address Bronx NY 77777']
     }, index=[1, 3, 2, 4, 10, 5])

_CENSUS_INPUT_2 = pd.DataFrame(
    {'address': ['456 address', '012 address', '345 address', '678 address'],
     'city': ['Brooklyn', 'LA', 'Tokyo', 'Bronx'],
     'region': ['NY', 'CA', '', 'NY'],
     'postal_code': ['22222', '55555-6666', '', '77777'],
     'patron_id_plaintext': ['patid3', 'patid5', 'patid6', 'patid7'],
     'patron_id': ['obfuscated_3', 'obfuscated_5', 'obfuscated_6',
                   'obfuscated_7'],
     'full_address': ['456 address Brooklyn NY 22222',
                      '012 address LA CA 55555-6666',
                      '345 address Tokyo',
                      '678 address Bronx NY 77777'],
     'house_number': ['456', '012', '345', '678'],
     'street_name': ['address']*4
     }, index=[3, 4, 10, 5])

_NYC_INPUT = pd.DataFrame(
    {'address': ['456 address', '678 address'],
     'city': ['Brooklyn', 'Bronx'],
     'region': ['NY', 'NY'],
     'postal_code': ['22222', '77777'],
     'patron_id_plaintext': ['patid3', 'patid7'],
     'patron_id': ['obfuscated_3', 'obfuscated_7'],
     'full_address': ['456 address Brooklyn NY 22222',
                      '678 address Bronx NY 77777'],
     'house_number': ['456', '678'],
     'street_name': ['address', 'address']
     }, index=[3, 5])

_CENSUS_GEOID_1 = pd.Series(
    ['00111222222', np.nan, '3344455555', np.nan, np.nan, np.nan],
    name='geoid', index=[1, 4, 2, 3, 10, 5])

_CENSUS_GEOID_2 = pd.Series(['66777888888', np.nan, np.nan, np.nan],
                            name='geoid', index=[4, 3, 10, 5])

_NYC_GEOID = pd.Series([np.nan, '99000111111'], name='geoid', index=[5, 3])

_ALL_GEOIDS = pd.DataFrame({
    'patron_id': ['obfuscated_1', 'obfuscated_2', 'obfuscated_3',
                  'obfuscated_4', 'obfuscated_5', 'obfuscated_6',
                  'obfuscated_7'],
    'geoid': ['00111222222', np.nan, '99000111111', '3344455555',
              '66777888888', np.nan, np.nan]},
    index=[1, 0, 3, 2, 4, 10, 5])


class TestMain:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch('lib.pipeline_controller.S3Client')
        mocker.patch('lib.pipeline_controller.PostgreSQLClient')
        mocker.patch('lib.pipeline_controller.RedshiftClient')
        mocker.patch('lib.pipeline_controller.CensusGeocoderApiClient')
        mocker.patch('lib.pipeline_controller.NycGeocoderClient')
        mocker.patch('lib.pipeline_controller.KinesisClient')
        mocker.patch('lib.pipeline_controller.AvroEncoder')
        return PipelineController('2023-01-01 12:34:56+00:00')

    def test_run_new_patrons_pipeline(self, test_instance, mocker):
        os.environ['MAX_BATCHES'] = '3'
        test_instance.has_max_batches = True

        mocker.patch(
            'lib.pipeline_controller.PipelineController._run_active_patrons_single_iteration',  # noqa: E501
            side_effect=[pd.Series({
                'creation_timestamp':
                    pd.Timestamp(_CREATION_DT.format(i), tz='EST')}, name=2)
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
        del os.environ['MAX_BATCHES']

    def test_run_updated_patrons_pipeline(self, test_instance, mocker):
        mocker.patch(
            'lib.pipeline_controller.PipelineController._run_active_patrons_single_iteration',  # noqa: E501
            side_effect=[
                pd.Series({'last_updated_timestamp':
                           pd.Timestamp(_UPDATE_DT.format(2), tz='EST')},
                          name=2),
                pd.Series({'last_updated_timestamp':
                           pd.Timestamp(_UPDATE_DT.format(3), tz='EST')},
                          name=2),
                pd.Series({'last_updated_timestamp':
                           pd.Timestamp(_UPDATE_DT.format(4), tz='EST')},
                          name=1)])

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

    def test_run_deleted_patrons_pipeline(self, test_instance, mocker):
        os.environ['MAX_BATCHES'] = '4'
        test_instance.has_max_batches = True

        mocker.patch(
            'lib.pipeline_controller.PipelineController._run_deleted_patrons_single_iteration',  # noqa: E501
            side_effect=[
                pd.Series({'deletion_date_et':
                           datetime.datetime.strptime(_DELETION_DATE.format(2),
                                                      '%Y-%m-%d').date()},
                          name=2),
                pd.Series({'deletion_date_et':
                           datetime.datetime.strptime(_DELETION_DATE.format(3),
                                                      '%Y-%m-%d').date()},
                          name=2),
                pd.Series({'deletion_date_et':
                           datetime.datetime.strptime(_DELETION_DATE.format(4),
                                                      '%Y-%m-%d').date()},
                          name=1)])

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

        test_instance.s3_client.fetch_cache.assert_called_once()
        test_instance.s3_client.set_cache.assert_not_called()
        test_instance.sierra_client.connect.assert_called_once()
        test_instance.sierra_client.close_connection.assert_called_once()

    def test_run_deleted_pipeline_no_results(self, test_instance, mocker):
        test_instance.s3_client.fetch_cache.return_value = {
            'creation_dt': _CREATION_DT.format(1),
            'update_dt': _UPDATE_DT.format(1),
            'deletion_date': _DELETION_DATE.format(1)}
        test_instance.sierra_client.execute_query.return_value = []

        mocker.patch('lib.pipeline_controller.build_deleted_patrons_query',
                     return_value='DELETED PATRONS QUERY')

        test_instance.run_pipeline(PipelineMode.DELETED_PATRONS)

        test_instance.s3_client.fetch_cache.assert_called_once()
        test_instance.s3_client.set_cache.assert_not_called()
        test_instance.sierra_client.connect.assert_called_once()
        test_instance.sierra_client.close_connection.assert_called_once()

    def test_run_new_patrons_single_iteration(self, test_instance, mocker):
        test_instance.poller_state = {
            'creation_dt': _CREATION_DT.format(1),
            'update_dt': _UPDATE_DT.format(1),
            'deletion_date': _DELETION_DATE.format(1)}

        test_instance.sierra_client.execute_query.return_value = \
            _NEW_SIERRA_RESULTS

        test_instance.avro_encoder.encode_batch.return_value = \
            _ENCODED_RECORDS[:3]
        mocked_unknown_patrons_method = mocker.patch(
            'lib.pipeline_controller.PipelineController._process_unknown_patrons',  # noqa: E501
            return_value=_GEOID_OUTPUT)
        mocker.patch('lib.pipeline_controller.build_new_patrons_query',
                     return_value='NEW PATRONS QUERY')
        mocker.patch('lib.pipeline_controller.obfuscate', side_effect=[
            'obfuscated_{}'.format(i) for i in range(1, 7)])

        assert_series_equal(
            test_instance._run_active_patrons_single_iteration(
                PipelineMode.NEW_PATRONS), _LAST_NEW_SIERRA_ROW)

        test_instance.sierra_client.connect.assert_called_once()
        test_instance.sierra_client.execute_query.assert_called_once_with(
            'NEW PATRONS QUERY')
        test_instance.sierra_client.close_connection.assert_called_once()

        mocked_unknown_patrons_method.assert_called_once()
        assert_frame_equal(mocked_unknown_patrons_method.call_args.args[0],
                           _GEOCODER_INPUT)

        # This input check implicitly tests that the geoids have been joined,
        # the datatypes have been converted, and the ids have been obfuscated
        test_instance.avro_encoder.encode_batch.assert_called_once()
        assert test_instance.avro_encoder.encode_batch.call_args.args[
            0] == _NEW_AVRO_ENCODER_INPUT

        test_instance.kinesis_client.send_records.assert_called_once_with(
            _ENCODED_RECORDS[:3])

    def test_run_updated_patrons_single_iteration(self, test_instance, mocker):
        test_instance.processed_ids = {'777'}
        test_instance.poller_state = {
            'creation_dt': _CREATION_DT.format(1),
            'update_dt': _UPDATE_DT.format(1),
            'deletion_date': _DELETION_DATE.format(1)}

        test_instance.sierra_client.execute_query.return_value = \
            _NEW_SIERRA_RESULTS + _UPDATED_SIERRA_RESULTS
        test_instance.redshift_client.execute_query.return_value = \
            _REDSHIFT_ADDRESS_RESULTS

        test_instance.avro_encoder.encode_batch.return_value = \
            _ENCODED_RECORDS
        mocked_unknown_patrons_method = mocker.patch(
            'lib.pipeline_controller.PipelineController._process_unknown_patrons',  # noqa: E501
            return_value=_GEOID_OUTPUT)
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

        test_instance.sierra_client.connect.assert_called_once()
        test_instance.sierra_client.execute_query.assert_called_once_with(
            'UPDATED PATRONS QUERY')
        test_instance.sierra_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        test_instance.redshift_client.execute_query.assert_called_once_with(
            'REDSHIFT ADDRESS QUERY')
        test_instance.redshift_client.close_connection.assert_called_once()

        mocked_unknown_patrons_method.assert_called_once()
        assert_frame_equal(mocked_unknown_patrons_method.call_args.args[0],
                           _GEOCODER_INPUT)

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

        test_instance.sierra_client.connect.assert_called_once()
        test_instance.sierra_client.execute_query.assert_called_once_with(
            'DELETED PATRONS QUERY')
        test_instance.sierra_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        test_instance.redshift_client.execute_query.assert_called_once_with(
            'REDSHIFT PATRON QUERY')
        test_instance.redshift_client.close_connection.assert_called_once()

        # This input check implicitly tests that the Sierra and Redshift
        # dataframes have been joined and the datatypes have been converted
        test_instance.avro_encoder.encode_batch.assert_called_once()
        assert test_instance.avro_encoder.encode_batch.call_args.args[
            0] == _DELETED_AVRO_ENCODER_INPUT

        test_instance.kinesis_client.send_records.assert_called_once_with(
            _ENCODED_RECORDS[:2])

    def test_process_unknown_patrons(self, test_instance, mocker):
        def mock_reformat_malformed_address(address_row):
            address_row['house_number'] = address_row['address'][:3]
            address_row['street_name'] = 'address'
            return address_row

        mocker.patch('lib.pipeline_controller.obfuscate', side_effect=[
            'obfuscated_{}'.format(i) for i in range(1, 8)])
        mocker.patch('lib.pipeline_controller.reformat_malformed_address',
                     new=mock_reformat_malformed_address)

        test_instance.census_geocoder_client.get_geoids.side_effect = [
            _CENSUS_GEOID_1, _CENSUS_GEOID_2]
        test_instance.nyc_geocoder_client.get_geoids.return_value = _NYC_GEOID

        assert_frame_equal(test_instance._process_unknown_patrons(
            _ORIGINAL_ADDRESS_DF), _ALL_GEOIDS, check_like=True)
        assert_frame_equal(
            test_instance.census_geocoder_client.get_geoids.call_args_list[0][0][0],  # noqa: E501
            _CENSUS_INPUT_1, check_like=True)
        assert_frame_equal(
            test_instance.census_geocoder_client.get_geoids.call_args_list[1][0][0],  # noqa: E501
            _CENSUS_INPUT_2, check_like=True)
        assert_frame_equal(
            test_instance.nyc_geocoder_client.get_geoids.call_args[0][0],
            _NYC_INPUT, check_like=True)
