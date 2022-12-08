import json
import pytest

from lib import S3Client
from tests.test_helpers import TestHelpers

_MOCK_STATE = {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'}


class TestS3Client:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch('boto3.client')
        return S3Client()

    def test_fetch_state(self, test_instance):
        def mock_download(bucket, resource, stream):
            assert bucket == 'test_s3_bucket'
            assert resource == 'test_s3_resource'
            stream.write(json.dumps(_MOCK_STATE).encode())

        test_instance.s3_client.download_fileobj.side_effect = mock_download
        assert test_instance.fetch_state() == _MOCK_STATE

    def test_set_state(self, test_instance):
        test_instance.set_state(_MOCK_STATE)
        arguments = test_instance.s3_client.upload_fileobj.call_args.args
        assert arguments[0].getvalue() == json.dumps(_MOCK_STATE).encode()
        assert arguments[1] == 'test_s3_bucket'
        assert arguments[2] == 'test_s3_resource'
