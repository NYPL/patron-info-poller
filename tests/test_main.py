import os
import pytest
import main

from lib.pipeline_controller import PipelineMode
from tests.test_helpers import TestHelpers


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
    
    def test_main_without_backfill(self, test_instance, mocker):
        os.environ['BACKFILL'] = 'False'

        mock_pipeline_controller = mocker.MagicMock()
        mocker.patch('main.PipelineController', return_value=mock_pipeline_controller)
        main.main()
        mock_pipeline_controller.run_pipeline.assert_has_calls([
            mocker.call(PipelineMode.NEW_PATRONS),
            mocker.call(PipelineMode.UPDATED_PATRONS),
            mocker.call(PipelineMode.DELETED_PATRONS)])

        del os.environ['BACKFILL']
        
    def test_main_with_backfill(self, test_instance, mocker):
        os.environ['BACKFILL'] = 'True'

        mock_pipeline_controller = mocker.MagicMock()
        mocker.patch('main.PipelineController', return_value=mock_pipeline_controller)
        main.main()
        mock_pipeline_controller.run_pipeline.assert_called_once_with(PipelineMode.NEW_PATRONS)

        del os.environ['BACKFILL']
