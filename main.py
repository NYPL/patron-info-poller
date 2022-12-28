import os

from helpers.config_helper import load_env_file
from helpers.log_helper import create_log
from lib.pipeline_controller import PipelineController, PipelineMode


def main():
    load_env_file(os.environ['ENVIRONMENT'], 'config/{}.yaml')
    logger = create_log(__name__)
    controller = PipelineController()

    logger.info('Starting new patrons pipeline run')
    controller.run_pipeline(PipelineMode.NEW_PATRONS)

    if os.environ.get('BACKFILL', False) != 'True':
        logger.info('Starting updated patrons pipeline run')
        controller.run_pipeline(PipelineMode.UPDATED_PATRONS)

        logger.info('Starting deleted patrons pipeline run')
        controller.run_pipeline(PipelineMode.DELETED_PATRONS)


if __name__ == '__main__':
    main()
