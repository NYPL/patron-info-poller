import pandas as pd

from helpers.address_helper import reformat_malformed_addresses
from pandas.testing import assert_series_equal


class TestAddressHelper:

    def test_empty_address(self):
        input_row = pd.Series({
            'address': None,
            'city': '',
            'region': '   ',
            'postal_code': None})
        output_row = pd.Series({
            'address': '',
            'city': '',
            'region': '',
            'postal_code': ''})
        assert_series_equal(
            reformat_malformed_addresses(input_row), output_row)

    def test_good_address(self):
        input_row = pd.Series({
            'address': '123 REAL AVE',
            'city': 'NEW YORK',
            'region': 'NY',
            'postal_code': '11111-2222'})
        assert_series_equal(
            reformat_malformed_addresses(input_row), input_row)

    def test_one_line_address(self):
        input_row = pd.Series({
            'address': '123 REAL AVE NEW YORK NY 11111-2222',
            'city': None,
            'region': None,
            'postal_code': None})
        output_row = pd.Series({
            'address': '123 REAL AVE',
            'city': 'NEW YORK',
            'region': 'NY',
            'postal_code': '11111-2222'})
        assert_series_equal(
            reformat_malformed_addresses(input_row), output_row)

    def test_found_postal_code(self):
        input_df = pd.DataFrame([
            ['123 REAL AVE 11111', 'NEW YORK', 'NY', None],
            ['123 REAL AVE', 'NEW YORK 11111', 'NY', None],
            ['123 REAL AVE', 'NEW YORK', 'NY 11111', None],
            ['123 REAL AVE 11111', 'NEW YORK 11111', 'NY 11111', 'bad']],
            columns=['address', 'city', 'region', 'postal_code'])
        output_row = pd.Series({
            'address': '123 REAL AVE',
            'city': 'NEW YORK',
            'region': 'NY',
            'postal_code': '11111'})

        for _, input_row in input_df.iterrows():
            assert_series_equal(
                reformat_malformed_addresses(input_row), output_row,
                check_names=False)

    def test_found_borough(self):
        input_df = pd.DataFrame([
            ['123 REAL AVE BRONX', None, 'NY', '11111'],
            ['123 REAL AVE', None, 'BROOKLYN', '11111'],
            ['123 REAL AVE', None, 'NY', 'STATEN ISLAND'],
            ['123 REAL AVE QUEENS', None, 'NY QUEENS', 'QUEENS'],
            ['123 REAL AVE', 'MANHATTAN', 'bad', '11111'],
            ['123 REAL AVE NEW YORK NY', None, None, '11111'],
            ['123 REAL AVE NEW YORK', None, None, '11111']],
            columns=['address', 'city', 'region', 'postal_code'])
        output_df = pd.DataFrame([
            ['123 REAL AVE', 'BRONX', 'NY', '11111'],
            ['123 REAL AVE', 'BROOKLYN', 'NY', '11111'],
            ['123 REAL AVE', 'STATEN ISLAND', 'NY', ''],
            ['123 REAL AVE', 'QUEENS', 'NY', ''],
            ['123 REAL AVE', 'NEW YORK', 'NY', '11111'],
            ['123 REAL AVE', 'NEW YORK', 'NY', '11111'],
            ['123 REAL AVE', '', 'NY', '11111']],
            columns=['address', 'city', 'region', 'postal_code'])

        for index, input_row in input_df.iterrows():
            assert_series_equal(
                reformat_malformed_addresses(input_row), output_df.loc[index],
                check_names=False)

    def test_found_ny_state(self):
        input_df = pd.DataFrame([
            ['123 REAL AVE NEW YORK', 'REAL CITY', None, '11111'],
            ['123 REAL AVE', 'REAL CITY NY', None, '11111'],
            ['123 REAL AVE', 'REAL CITY', None, 'NYC 11111'],
            ['123 REAL AVE NY', 'REAL CITY NY', None, '11111 NY']],
            columns=['address', 'city', 'region', 'postal_code'])
        output_row = pd.Series({
            'address': '123 REAL AVE',
            'city': 'REAL CITY',
            'region': 'NY',
            'postal_code': '11111'})

        for _, input_row in input_df.iterrows():
            assert_series_equal(
                reformat_malformed_addresses(input_row), output_row,
                check_names=False)

    def test_character_replacement(self):
        input_row = pd.Series({
            'address': '1\\2"3\' $R%E{A[L∆ AVE',
            'city': 'N1E2W3 Y.O,R#K',
            'region': '1N&Y.',
            'postal_code': 'abc11111-2.2,2+2d'})
        output_row = pd.Series({
            'address': '123 REAL AVE',
            'city': 'NEW YORK',
            'region': 'NY',
            'postal_code': '11111-2222'})
        assert_series_equal(
            reformat_malformed_addresses(input_row), output_row)
