import logging
from logging.handlers import RotatingFileHandler
import sys
from datetime import datetime
import os


def setup_logger():
    # Create a log directory if it doesn't exist
    log_dir = 'log'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create logger
    logger = logging.getLogger('migration_logger')
    logger.setLevel(logging.DEBUG)

    # Create log file name with timestamp
    log_file = os.path.join(log_dir, f"""migration_log_{
                            datetime.now().strftime('%Y%m%d_%H%M%S')}.log""")

    # Create file handler which logs even debug messages
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setLevel(logging.DEBUG)

    # Create console handler with a higher log level
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Create and configure logger
logger = setup_logger()
