import os
import pandas as pd
import requests
import warnings

from helpers.address_helper import reformat_malformed_addresses
from io import BytesIO, TextIOWrapper
from nypl_py_utils.functions.log_helper import create_log
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import RequestException


class GeocoderApiClient:
    """Client for managing requests to the Census Geocoder API"""

    def __init__(self):
        self.logger = create_log('geocoder_api_client')

        retry_policy = Retry(total=2, backoff_factor=4,
                             status_forcelist=[500, 502, 503, 504],
                             allowed_methods=frozenset(['GET', 'POST']))
        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(max_retries=retry_policy))

    def get_geoids(self, address_df):
        """
        Geocodes the addresses in address_df by sending two (successful)
        requests to the API: one with all of the input addresses and another
        with the addresses that failed to be geocoded in the first request.
        This is recommended by the API because it sometimes erroneously fails
        to match an address when it's used in batch mode. For more info, see
        https://www2.census.gov/geo/pdfs/maps-data/data/Census_Geocoder_FAQ.pdf

        Returns a series containing the geoids (or NaN) indexed to match
        address_df.
        """
        self.logger.info(
            'Sending ({}) addresses to geocoder API'.format(len(address_df)))
        input_address_df = address_df[
            ['address', 'city', 'region', 'postal_code']].replace(
                r'\'|"|\\', '', regex=True)
        geoids = self._get_geoids_with_single_request(input_address_df)
        retry_indices = geoids[geoids.isnull()].index
        retry_address_df = input_address_df.loc[retry_indices]
        retry_address_df = retry_address_df.apply(
            reformat_malformed_addresses, axis=1)

        # Ignore the FutureWarning caused by the pandas update method, which is
        # not relevant to this code.
        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            geoids.update(
                self._get_geoids_with_single_request(retry_address_df))

        return geoids

    def _get_geoids_with_single_request(self, address_df):
        """
        Sends the addresses in address_df to the geocoder a single time, which
        may require multiple requests if the geocoder is overloaded.

        Returns a series containing the geoids (or NaN) indexed to match
        address_df.
        """
        raw_response = self._send_request(address_df)
        response_df = pd.read_csv(BytesIO(raw_response), header=None,
                                  dtype=str, index_col=0, engine='python',
                                  names=['index', 'input_address', 'match',
                                         'match_type', 'matched_address',
                                         'coordinates', 'tigerline_id',
                                         'tigerline_side', 'state_id',
                                         'county_id', 'tract_id', 'block_id'])
        geoids = (response_df['state_id'] + response_df['county_id'] +
                  response_df['tract_id']).rename('geoid')
        return geoids

    def _send_request(self, address_df):
        """
        Send a request to the API. Recursively calls itself with a smaller
        batch size if the initial request fails.

        Returns a csv string where each line contains information about a
        single geocoded address.
        """
        try:
            with BytesIO() as address_stream:
                address_df.to_csv(
                    address_stream, header=False,
                    columns=['address', 'city', 'region', 'postal_code'])
                self.logger.debug(
                    'Sending {}-address batch to geocoder API'.format(
                        len(address_df)))
                address_stream.seek(0)
                response = self.session.post(
                    os.environ['GEOCODER_API_BASE_URL'],
                    files={'addressFile': NamedTextIOWrapper(
                        address_stream, name='input_addresses.csv',
                        encoding='utf-8')},
                    params={
                        'benchmark': os.environ['GEOCODER_API_BENCHMARK'],
                        'vintage': os.environ['GEOCODER_API_VINTAGE'],
                        'key': os.environ['GEOCODER_API_KEY']
                    },
                    timeout=300)
                return response.content
        except RequestException as e:
            new_df_size = len(address_df) // 2
            if new_df_size >= 1000:
                self.logger.info(
                    ('Initial geocoding request failed -- sending two new '
                     'requests with {} addresses each').format(new_df_size))
                results_1 = self._send_request(address_df.iloc[:new_df_size])
                results_2 = self._send_request(address_df.iloc[new_df_size:])
                return results_1 + results_2
            else:
                self.logger.error(
                    ('Failed to retrieve geocoded addresses from API: {}')
                    .format(e))
                raise GeocoderApiClientError(
                    ('Failed to retrieve geocoded addresses from API: {}')
                    .format(e)) from None


class NamedTextIOWrapper(TextIOWrapper):
    """
    Wrapper class around TextIOWrapper that makes the 'name' attribute
    settable. The geocoder API apparently requires a named file to function.
    """

    def __init__(self, buffer, name=None, **kwargs):
        vars(self)['name'] = name
        super().__init__(buffer, **kwargs)

    def __getattribute__(self, name):
        if name == 'name':
            return vars(self)['name']
        return super().__getattribute__(name)


class GeocoderApiClientError(Exception):
    def __init__(self, message=None):
        self.message = message
