import pandas as pd
import pytest

from geosupport.error import GeosupportError
from lib import NycGeocoderClient
from pandas.testing import assert_series_equal


_ADDRESS_DF = pd.DataFrame({
    'address': ['123 ave', '456 st', '789 blvd', '01-23 ct', '4 pl', '5 rd'],
    'house_number': ['123', '456', '789', '01-23', '4', '5'],
    'street_name': ['ave', 'st', 'blvd', 'ct', 'pl', 'rd'],
    'postal_code': ['11111', '22222', '33333-4444', '55555', '66666', '77777'],
    'random_column': ['a', 'b', 'c', 'd', 'e', 'f']},
    index=[5, 4, 3, 2, 1, 0])


class TestNycGeocoderClient:

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch('geosupport.Geosupport')
        return NycGeocoderClient()

    def test_geocode_address(self, test_instance):
        test_instance.geosupport.address.return_value = {
            'First Borough Name': 'BRONX',
            '2020 Census Tract': '123456'}

        assert test_instance._geocode_address(
            (5, _ADDRESS_DF.loc[5])) == (5, '36005123456')
        test_instance.geosupport.address.assert_called_once_with(
            house_number='123', street_name='ave', zip_code='11111',
            street_name_normalization='C')

    def test_geocode_address_no_tract(self, test_instance):
        test_instance.geosupport.address.return_value = {
            'First Borough Name': 'NOT A BOROUGH',
            '2020 Census Tract': '123456'}

        assert test_instance._geocode_address(
            (5, _ADDRESS_DF.loc[5])) == (5, None)

    def test_geocode_address_error(self, test_instance):
        test_instance.geosupport.address.side_effect = GeosupportError('error')

        assert test_instance._geocode_address(
            (5, _ADDRESS_DF.loc[5])) == (5, None)

    def test_get_geoids(self, test_instance, mocker):
        test_instance.geosupport.address.side_effect = [
            {'First Borough Name': 'BRONX', '2020 Census Tract': '123456'},
            {'First Borough Name': 'BROOKLYN', '2010 Census Tract': '789012'},
            {'First Borough Name': 'MANHATTAN', '2000 Census Tract': '345678'},
            {'First Borough Name': 'QUEENS', '1990 Census Tract': '901234'},
            {'First Borough Name': 'STATEN IS',
             '2020 Census Tract': '567890', '2010 Census Tract': '999999'},
            {'First Borough Name': 'BRONX'}
        ]

        assert_series_equal(test_instance.get_geoids(_ADDRESS_DF), pd.Series(
            ['36005123456', '36047789012', '36061345678', '36081901234',
             '36085567890', None], index=[5, 4, 3, 2, 1, 0], name='geoid'))
        test_instance.geosupport.address.assert_has_calls([
            mocker.call(house_number='123', street_name='ave',
                        zip_code='11111', street_name_normalization='C'),
            mocker.call(house_number='456', street_name='st',
                        zip_code='22222', street_name_normalization='C'),
            mocker.call(house_number='789', street_name='blvd',
                        zip_code='33333-4444', street_name_normalization='C'),
            mocker.call(house_number='01-23', street_name='ct',
                        zip_code='55555', street_name_normalization='C'),
            mocker.call(house_number='4', street_name='pl',
                        zip_code='66666', street_name_normalization='C'),
            mocker.call(house_number='5', street_name='rd',
                        zip_code='77777', street_name_normalization='C'),
        ], any_order=True)
