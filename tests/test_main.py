import pytest
import main

from freezegun import freeze_time
from helpers.pipeline_mode import PipelineMode
from tests.test_helpers import TestHelpers


@freeze_time('2023-01-01 01:23:45+00:00')
class TestMain:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch('main.load_env_file')
        mocker.patch('main.create_log')

    def test_main(self, test_instance, mocker):
        mock_pipeline_controller = mocker.MagicMock()
        mock_controller_initializer = mocker.patch(
            'main.PipelineController', return_value=mock_pipeline_controller)
        main.main()
        mock_controller_initializer.assert_called_once_with(
            '2023-01-01T01:23:45+00:00')
        mock_pipeline_controller.run_pipeline.assert_has_calls([
            mocker.call(PipelineMode.NEW_PATRONS),
            mocker.call(PipelineMode.UPDATED_PATRONS),
            mocker.call(PipelineMode.DELETED_PATRONS)])
