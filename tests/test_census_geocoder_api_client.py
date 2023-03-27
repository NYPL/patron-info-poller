import numpy as np
import pandas as pd
import pytest

from lib import CensusGeocoderApiClient
from pandas.testing import assert_series_equal
from requests.exceptions import ConnectionError
from tests.test_helpers import TestHelpers


_API_RESPONSE = ('"0","123 good address, New York, NY, 11111","Match","Exact","123 matched address, New York, NY, 11111-9999","-0.00000001,1.11111110","123456789","R","00","111","222222","3333"\n'  # noqa: E501
                 '"1","456 bad address, Brooklyn, NY, 22222","No_Match"\n'
                 '"2","789 good address, Staten Island, NY, 33333-4444","Match","Non_Exact","789 matched address, Staten Island, NY, 33333-4444","-0.00000001,1.11111110","123456789","R","44","555","666666","7777"\n'  # noqa: E501
                 '"3","012 bad address, Bronx, NY, 55555-6666","No_Match"\n'
                 '"4","345 tie address, Queens, NY, 77777","Tie"\n')

_ADDRESS_DF = pd.DataFrame({'index': [4, 3, 2, 1, 0],
                            'address': ['123 good address', '456 bad address',
                                        '789 good address', '012 bad address',
                                        '345 tie address'],
                            'city': ['New York', 'Brooklyn', 'Staten Island',
                                     'Bronx', 'Queens'],
                            'region': ['NY', 'NY', 'NY', 'NY', 'NY'],
                            'postal_code': ['11111', '22222', '33333-4444',
                                            '55555-6666', '77777'],
                            'random_column': ['a', 'b', 'c', 'd', 'e']}
                           ).set_index('index')


class TestCensusGeocoderApiClient:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self):
        return CensusGeocoderApiClient()

    def test_send_request(self, requests_mock, test_instance):
        requests_mock.post(
            'https://test_geocoder_url?benchmark=test_geocoder_benchmark&vintage=test_geocoder_vintage',  # noqa: E501
            text=_API_RESPONSE)

        assert test_instance._send_request(
            _ADDRESS_DF) == bytes(_API_RESPONSE, 'utf-8')

    def test_send_request_with_retries(self, requests_mock, test_instance):
        _BIG_ADDRESS_DF = _ADDRESS_DF.reindex(list(range(2000)))
        _FIRST_RESPONSE = '\n'.join(_API_RESPONSE.split('\n')[:2]) + '\n'
        _SECOND_RESPONSE = '\n'.join(_API_RESPONSE.split('\n')[2:])

        requests_mock.post(
            'https://test_geocoder_url?benchmark=test_geocoder_benchmark&vintage=test_geocoder_vintage',  # noqa: E501
            [{'exc': ConnectionError},
             {'text': _FIRST_RESPONSE, 'status_code': 200},
             {'text': _SECOND_RESPONSE, 'status_code': 200}])

        assert test_instance._send_request(
            _BIG_ADDRESS_DF) == bytes(_API_RESPONSE, 'utf-8')

    def test_get_geoids(self, requests_mock, test_instance):
        _GEOIDS = pd.Series(
            ['00111222222', np.nan, '44555666666', np.nan, np.nan],
            name='geoid', index=[0, 1, 2, 3, 4])
        _GEOIDS.index.name = 'index'
        requests_mock.post(
            'https://test_geocoder_url?benchmark=test_geocoder_benchmark&vintage=test_geocoder_vintage',  # noqa: E501
            text=_API_RESPONSE)

        assert_series_equal(test_instance.get_geoids(_ADDRESS_DF), _GEOIDS)
