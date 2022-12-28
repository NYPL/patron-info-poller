import pytest

from lib import DbClient, DbClientError, DbMode
from tests.test_helpers import TestHelpers


class TestDbClient:
    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_pg_conn(self, mocker):
        return mocker.patch('psycopg2.connect')

    @pytest.fixture
    def test_redshift_conn(self, mocker):
        return mocker.patch('redshift_connector.connect')

    @pytest.fixture
    def test_sierra_instance(self):
        return DbClient(DbMode.SIERRA)

    @pytest.fixture
    def test_redshift_instance(self):
        return DbClient(DbMode.REDSHIFT)

    def test_initializer_sierra(self, test_pg_conn, test_sierra_instance):
        assert test_sierra_instance.mode == DbMode.SIERRA
        test_pg_conn.assert_called_once_with(host='test_sierra_host',
                                             port='test_sierra_port',
                                             dbname='test_sierra_name',
                                             user='test_sierra_user',
                                             password='test_sierra_password')

    def test_initializer_redshift(self, test_redshift_conn,
                                  test_redshift_instance):
        assert test_redshift_instance.mode == DbMode.REDSHIFT
        test_redshift_conn.assert_called_once_with(
            iam=True,
            cluster_identifier='test_redshift_cluster',
            database='test_redshift_name',
            db_user='test_redshift_user',
            region='test_aws_region',
            access_key_id='test_aws_key_id',
            secret_access_key='test_aws_secret_key')

    def test_execute_query_sierra(self, test_pg_conn, test_sierra_instance,
                                  mocker):
        mock_cursor = mocker.MagicMock()
        mock_cursor.fetchall.return_value = [[1, 2, 3], ['a', 'b', 'c']]
        test_sierra_instance.conn.cursor.return_value = mock_cursor

        assert test_sierra_instance.execute_query(
            'test query') == [[1, 2, 3], ['a', 'b', 'c']]
        mock_cursor.execute.assert_called_once_with('test query')
        mock_cursor.close.assert_called_once()

    def test_execute_query_redshift(self, test_redshift_conn,
                                    test_redshift_instance, mocker):
        mock_cursor = mocker.MagicMock()
        mock_cursor.fetchall.return_value = [[1, 2, 3], ['a', 'b', 'c']]
        test_redshift_instance.conn.cursor.return_value = mock_cursor

        assert test_redshift_instance.execute_query(
            'test query') == [[1, 2, 3], ['a', 'b', 'c']]
        mock_cursor.execute.assert_called_once_with('test query')
        mock_cursor.close.assert_called_once()

    def test_execute_query_with_exception_sierra(
            self, test_pg_conn, test_sierra_instance, mocker):
        mock_cursor = mocker.MagicMock()
        mock_cursor.execute.side_effect = Exception()
        test_sierra_instance.conn.cursor.return_value = mock_cursor

        with pytest.raises(DbClientError):
            test_sierra_instance.execute_query('test query')

        test_sierra_instance.conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_execute_query_with_exception_redshift(
            self, test_redshift_conn, test_redshift_instance, mocker):
        mock_cursor = mocker.MagicMock()
        mock_cursor.execute.side_effect = Exception()
        test_redshift_instance.conn.cursor.return_value = mock_cursor

        with pytest.raises(DbClientError):
            test_redshift_instance.execute_query('test query')

        test_redshift_instance.conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_close_connection_sierra(self, test_pg_conn, test_sierra_instance):
        test_sierra_instance.close_connection()
        test_sierra_instance.conn.close.assert_called_once()

    def test_close_connection_redshift(self, test_redshift_conn,
                                       test_redshift_instance):
        test_redshift_instance.close_connection()
        test_redshift_instance.conn.close.assert_called_once()
