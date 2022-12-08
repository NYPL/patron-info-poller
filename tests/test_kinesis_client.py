import pytest

from freezegun import freeze_time
from lib import KinesisClient
from tests.test_helpers import TestHelpers

_DATETIME_KEY = '1640995200000000000'
_MOCK_KINESIS_RECORDS = [
    {'Data': b'a', 'PartitionKey': _DATETIME_KEY},
    {'Data': b'b', 'PartitionKey': _DATETIME_KEY},
    {'Data': b'c', 'PartitionKey': _DATETIME_KEY},
    {'Data': b'd', 'PartitionKey': _DATETIME_KEY},
    {'Data': b'e', 'PartitionKey': _DATETIME_KEY}
]


class TestKinesisClient:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch('boto3.client')
        return KinesisClient()

    @freeze_time('2022-01-01')
    def test_send_records(self, test_instance, mocker):
        MOCK_AVRO_RECORDS = [b'a', b'b', b'c', b'd', b'e']
        mocked_send_method = mocker.patch(
            'lib.kinesis_client.KinesisClient._send_kinesis_format_records')

        test_instance.send_records(MOCK_AVRO_RECORDS)
        mocked_send_method.assert_has_calls([
            mocker.call([_MOCK_KINESIS_RECORDS[0], _MOCK_KINESIS_RECORDS[1]]),
            mocker.call([_MOCK_KINESIS_RECORDS[2], _MOCK_KINESIS_RECORDS[3]]),
            mocker.call([_MOCK_KINESIS_RECORDS[4]])])

    def test_send_kinesis_format_records(self, test_instance):
        test_instance.kinesis_client.put_records.return_value = {
            'FailedRecordCount': 0}

        test_instance._send_kinesis_format_records(_MOCK_KINESIS_RECORDS)
        test_instance.kinesis_client.put_records.assert_called_once_with(
            Records=_MOCK_KINESIS_RECORDS, StreamName='test_kinesis_stream')

    def test_send_kinesis_format_records_with_failures(
            self, test_instance, mocker):
        test_instance.kinesis_client.put_records.side_effect = [
            {'FailedRecordCount': 2, 'Records': [
                'record1', {'ErrorCode': 2},
                'record3', {'ErrorCode': 4},
                'record5']},
            {'FailedRecordCount': 0}]
        test_instance._send_kinesis_format_records(_MOCK_KINESIS_RECORDS)
        test_instance.kinesis_client.put_records.assert_has_calls([
            mocker.call(Records=_MOCK_KINESIS_RECORDS,
                        StreamName='test_kinesis_stream'),
            mocker.call(Records=[_MOCK_KINESIS_RECORDS[1],
                                 _MOCK_KINESIS_RECORDS[3]],
                        StreamName='test_kinesis_stream')])
