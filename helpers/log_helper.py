import logging
import os
import sys

levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


def create_log(module):
    logger = logging.getLogger(module)
    if not logger.hasHandlers():
        console_log = logging.StreamHandler(stream=sys.stdout)

        log_level = os.environ.get('LOG_LEVEL', 'warning').lower()

        logger.setLevel(levels[log_level])
        console_log.setLevel(levels[log_level])

        formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s: %(message)s')  # noqa: E501
        console_log.setFormatter(formatter)

        logger.addHandler(console_log)

    return logger
