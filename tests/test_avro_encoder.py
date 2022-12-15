import json
import pandas as pd
import pytest

from lib import AvroEncoder, AvroEncoderError
from tests.test_helpers import TestHelpers


_MOCK_SCHEMA = {'data': {'schema': json.dumps({
    'name': 'MockPatronInfo',
    'type': 'record',
    'fields': [
        {
            'name': 'patron_id',
            'type': 'int'
        },
        {
            'name': 'patron_name',
            'type': ['null', 'string']
        }
    ]
})}}


class TestAvroEncoder:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, requests_mock):
        requests_mock.get(
            'https://test_schema_url', text=json.dumps(_MOCK_SCHEMA))
        return AvroEncoder()

    def test_get_json_schema(self, test_instance):
        assert test_instance.schema == _MOCK_SCHEMA['data']['schema']

    def test_encode_batch_success(self, test_instance):
        TEST_BATCH = pd.DataFrame(
            {'patron_id': [123, 456, 789],
             'patron_name': ['Angus', None, 'Bianca']})
        encoded_records = test_instance.encode_batch(TEST_BATCH)
        assert len(encoded_records) == len(TEST_BATCH)
        for i in range(3):
            assert type(encoded_records[i]) is bytes
            assert test_instance.decode_record(
                encoded_records[i]) == TEST_BATCH.loc[i].to_dict()

    def test_encode_batch_error(self, test_instance):
        BAD_BATCH = pd.DataFrame(
            {'patron_id': [123, 456], 'bad_field': ['bad', 'field']})
        with pytest.raises(AvroEncoderError):
            test_instance.encode_batch(BAD_BATCH)
