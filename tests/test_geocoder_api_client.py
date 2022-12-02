import numpy as np
import pandas as pd
import pytest

from io import BytesIO
from lib import GeocoderApiClient, GeocoderApiClientError
from requests_mock import Adapter
from tests.test_helpers import TestHelpers


_API_RESPONSE_1 = '"0","123 good address, New York, NY, 11111","Match","Exact","123 matched address, New York, NY, 11111-9999","-0.00000001,1.11111110","123456789","R","00","111","222222","3333"\n' + \
    '"1","456 bad address, Brooklyn, NY, 22222","No_Match"\n' + \
    '"2","789 good address, Staten Island, NY, 33333-4444","Match","Non_Exact","789 matched address, Staten Island, NY, 33333-4444","-0.00000001,1.11111110","123456789","R","44","555","666666","7777"\n' + \
    '"3","012 bad address, Bronx, NY, 55555-6666","No_Match"\n' + \
    '"4","345 tie address, Queens, NY, 77777","Tie"'

_API_RESPONSE_2 = '"1","456 bad address, Brooklyn, NY, 22222","No_Match"\n' + \
    '"3","012 bad address, Bronx, NY, 55555-6666","No_Match"\n' + \
    '"4","345 tie address, Queens, NY, 77777","Tie","Non_Exact","345 matched address, Queens, NY, 77777","-0.00000001,1.11111110","123456789","R","88","999","000000","1111"'

_ADDRESS_DF = pd.DataFrame({'index': [4, 3, 2, 1, 0],
                            'address': ['123 good address', '456 bad address', '789 good address', '012 bad address', '345 tie address'],
                            'city': ['New York', 'Brooklyn', 'Staten Island', 'Bronx', 'Queens'],
                            'region': ['NY', 'NY', 'NY', 'NY', 'NY'],
                            'postal_code': ['11111', '22222', '33333-4444', '55555-6666', '77777'],
                            'random_column': ['a', 'b', 'c', 'd', 'e']})


class TestGeocoderApiClient:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self):
        return GeocoderApiClient()

    def test_send_request(self, requests_mock, test_instance):
        requests_mock.post(
            'https://test_geocoder_url?benchmark=test_geocoder_benchmark&vintage=test_geocoder_vintage',
            text=_API_RESPONSE_1)

        test_stream = BytesIO(b'"0","123 good address","New York","NY","11111"\n' +
                              b'"1","456 bad address","Brooklyn","NY","22222"\n' +
                              b'"2","789 good address","Staten Island","NY","33333-4444"\n' +
                              b'"3","012 bad address","Bronx","NY","55555-6666"\n' +
                              b'"4","345 tie address","Queens","NY","77777"')
        assert test_instance._send_request(
            test_stream) == bytes(_API_RESPONSE_1, 'utf-8')

    def test_get_geoids_with_single_request(self, requests_mock, test_instance):
        requests_mock.post(
            'https://test_geocoder_url?benchmark=test_geocoder_benchmark&vintage=test_geocoder_vintage',
            text=_API_RESPONSE_1)

        assert list(test_instance._get_geoids_with_single_request(_ADDRESS_DF)) == [
            '00111222222', np.nan, '44555666666', np.nan, np.nan]

    def test_get_geoids(self, requests_mock, test_instance):
        requests_mock.post(
            'https://test_geocoder_url?benchmark=test_geocoder_benchmark&vintage=test_geocoder_vintage',
            [{'text': _API_RESPONSE_1, 'status_code': 200}, {'text': _API_RESPONSE_2, 'status_code': 200}])

        assert list(test_instance.get_geoids(_ADDRESS_DF)) == [
            '00111222222', np.nan, '44555666666', np.nan, '88999000000']
