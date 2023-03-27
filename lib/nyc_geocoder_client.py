import geosupport
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from nypl_py_utils.functions.log_helper import create_log


_BOROUGH_MAP = {
    'BRONX': '36005',
    'BROOKLYN': '36047',
    'MANHATTAN': '36061',
    'QUEENS': '36081',
    'STATEN IS': '36085'
}


class NycGeocoderClient:
    """Client for managing calls to the NYC Geocoder"""

    def __init__(self):
        self.logger = create_log('nyc_geocoder_client')
        self.geosupport = geosupport.Geosupport()

    def get_geoids(self, address_df):
        """
        Geocodes the addresses in address_df and returns a series containing
        the geoids (or None) indexed to match address_df
        """
        self.logger.info(
            'Sending ({}) addresses to NYC geocoder'.format(len(address_df)))
        with ThreadPoolExecutor() as executor:
            geoids_list = list(executor.map(
                self._geocode_address, address_df.iterrows()))

        # Transforms a list of (index, value) tuples into [(indices), (values)]
        # so that it can be used to construct a pandas Series
        transposed_geoids_list = list(zip(*geoids_list))
        return pd.Series(transposed_geoids_list[1],
                         index=transposed_geoids_list[0], name='geoid')

    def _geocode_address(self, address_row_tuple):
        """
        Processes a single row of the address dataframe by parsing the given
        address and sending it to the NYC geocoder.

        Returns the row index and the address's geoid as a string or None if it
        can't be geocoded.
        """
        index = address_row_tuple[0]
        address_row = address_row_tuple[1]
        try:
            # Setting street_name_normalization='C' prevents the geocoder from
            # padding the results for sorting/display purposes
            result = self.geosupport.address(
                house_number=address_row['house_number'],
                street_name=address_row['street_name'],
                zip_code=address_row['postal_code'],
                street_name_normalization='C')
            county_id = _BOROUGH_MAP.get(result.get('First Borough Name'))
            tract_id = (result.get('2020 Census Tract') or
                        result.get('2010 Census Tract') or
                        result.get('2000 Census Tract') or
                        result.get('1990 Census Tract'))
            if county_id is None or tract_id is None:
                return index, None
            else:
                return index, county_id + tract_id
        except geosupport.error.GeosupportError:
            return index, None
