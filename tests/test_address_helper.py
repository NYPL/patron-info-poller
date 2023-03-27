import pandas as pd

from collections import OrderedDict
from helpers.address_helper import reformat_malformed_address
from pandas.testing import assert_series_equal
from usaddress import RepeatedLabelError


class TestAddressHelper:

    def test_good_address(self, mocker):
        input_row = pd.Series({
            'address': '123 REAL AVE APT 1',
            'city': 'NEW YORK',
            'region': 'NY',
            'postal_code': '11111-2222',
            'full_address': '123 REAL AVE APT 1 NEW YORK NY 11111-2222'})
        mocker.patch('usaddress.tag', return_value=(OrderedDict([
            ('AddressNumber', '123'),
            ('street', 'REAL AVE'),
            ('line2', 'APT 1'),
            ('PlaceName', 'NEW YORK'),
            ('StateName', 'NY'),
            ('ZipCode', '11111-2222')]),
            'StreetAddress'))
        output_row = input_row.copy()
        output_row['house_number'] = '123'
        output_row['street_name'] = 'REAL AVE'

        assert_series_equal(
            reformat_malformed_address(input_row), output_row)

    def test_misordered_address(self, mocker):
        input_row = pd.Series({
            'address': '123',
            'city': 'REAL AVE APT 1',
            'region': 'NEW YORK NY',
            'postal_code': '11111-2222',
            'full_address': '123 REAL AVE APT 1 NEW YORK NY 11111-2222'})
        mocker.patch('usaddress.tag', return_value=(OrderedDict([
            ('AddressNumber', '123'),
            ('street', 'REAL AVE'),
            ('line2', 'APT 1'),
            ('PlaceName', 'NEW YORK'),
            ('StateName', 'NY'),
            ('ZipCode', '11111-2222')]),
            'StreetAddress'))
        output_row = pd.Series({
            'address': '123 REAL AVE APT 1',
            'city': 'NEW YORK',
            'region': 'NY',
            'postal_code': '11111-2222',
            'full_address': '123 REAL AVE APT 1 NEW YORK NY 11111-2222',
            'house_number': '123',
            'street_name': 'REAL AVE'})

        assert_series_equal(
            reformat_malformed_address(input_row), output_row)

    def test_character_replacement(self, mocker):
        input_row = pd.Series({
            'address': '123 $R%E{A[L∆ AVE',
            'city': 'N1E2W3 Y.O,R#K',
            'region': '1N&Y.',
            'postal_code': 'abc11111-2.2,2+2d',
            'full_address': ('123 $R%E{A[L∆ AVE N1E2W3 Y.O,R#K 1N&Y. '
                             'abc11111-2.2,2+2d')})
        mocker.patch('usaddress.tag', return_value=(OrderedDict([
            ('AddressNumber', '123'),
            ('street', '$R%E{A[L∆ AVE'),
            ('PlaceName', 'N1E2W3 Y.O,R#K'),
            ('StateName', '1N&Y.'),
            ('ZipCode', 'abc11111-2.2,2+2d')]),
            'StreetAddress'))
        output_row = pd.Series({
            'address': '123 REAL AVE',
            'city': 'NEW YORK',
            'region': 'NY',
            'postal_code': '11111-2222',
            'full_address': ('123 $R%E{A[L∆ AVE N1E2W3 Y.O,R#K 1N&Y. '
                             'abc11111-2.2,2+2d'),
            'house_number': '123',
            'street_name': 'REAL AVE'})

        assert_series_equal(
            reformat_malformed_address(input_row), output_row)

    def test_repeated_labels_error(self, mocker):
        input_row = pd.Series({
            'address': '123 REAL AVE APT 1',
            'city': 'NEW YORK',
            'region': 'NY',
            'postal_code': '11111-2222',
            'full_address': '123 REAL AVE APT 1 NEW YORK NY 11111-2222'})
        mocker.patch('usaddress.tag', side_effect=RepeatedLabelError(
            input_row['full_address'],
            [('123', 'AddressNumber'),
             ('REAL', 'StreetName'),
             ('AVE', 'StreetName'),
             ('APT', 'OccupancyType'),
             ('1', 'OccupancyIdentifier'),
             ('NEW', 'PlaceName'),
             ('YORK NY', 'PlaceName'),
             ('11111', 'ZipCode')],
            'StreetAddress'))
        output_row = pd.Series({
            'address': '123 REAL AVE APT 1',
            'city': 'NEW YORK NY',
            'region': 'NY',
            'postal_code': '11111',
            'full_address': '123 REAL AVE APT 1 NEW YORK NY 11111-2222',
            'house_number': '123',
            'street_name': 'REAL AVE'})

        assert_series_equal(
            reformat_malformed_address(input_row), output_row)

    def test_repeated_address(self, mocker):
        input_row = pd.Series({
            'address': '123 REAL AVE',
            'city': '123 REAL AVE',
            'region': 'NEW YORK NY',
            'postal_code': '11111-2222',
            'full_address': ('123 REAL AVE 123 REAL AVE NEW YORK NY '
                             '11111-2222')})
        mocker.patch('usaddress.tag', side_effect=RepeatedLabelError(
            input_row['full_address'],
            [('123', 'AddressNumber'),
             ('123', 'AddressNumber'),
             ('REAL AVE', 'StreetName'),
             ('REAL AVE', 'StreetName'),
             ('NEW YORK', 'PlaceName'),
             ('NY', 'StateName'),
             ('11111-2222', 'ZipCode')],
            'StreetAddress'))
        output_row = pd.Series({
            'address': '123 REAL AVE',
            'city': 'NEW YORK',
            'region': 'NY',
            'postal_code': '11111-2222',
            'full_address': '123 REAL AVE 123 REAL AVE NEW YORK NY 11111-2222',
            'house_number': '123',
            'street_name': 'REAL AVE'})

        assert_series_equal(
            reformat_malformed_address(input_row), output_row)
