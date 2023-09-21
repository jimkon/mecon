import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
import pathlib

import pandas as pd

LOGS_DIRECTORY = pathlib.Path("logs")
LOG_FILENAME = "logs_raw.csv"

"""Use log rotation Implement log rotation to prevent excessive file growth and disk space usage in your log files. 
Old log files are automatically archived, compressed, and deleted using this approach. You can either use a 
third-party tool like logrotate or the Python language's built-in RotatingFileHandler to construct your own solution.
"""


# %Y-%m-%d %H:%M:%S
# https://realpython.com/python-logging/
# logging.basicConfig(format='%(name)s,%(levelname)s,%(asctime)s,%(msecs)d,%(module)s,%(funcName)s,"%(message)s"')


def setup_logging(logger=None):
    # Create a logger
    if logger is None:
        logger = logging.getLogger()

    # Set the logging level (e.g., INFO, DEBUG)
    logger.setLevel(logging.INFO)

    # Create a file handler (logs to a file)
    file_handler = TimedRotatingFileHandler(
        filename=LOGS_DIRECTORY / LOG_FILENAME,
        when='midnight',
        interval=1,
        backupCount=7,
        encoding='utf-8',  # Optional, specify encoding if needed
        # suffix='%Y-%m-%d'  # Include the date as a suffix in the log file name
    )
    # Create a console handler (logs to the console)
    console_handler = logging.StreamHandler(sys.stdout)

    # Define a format for log messages
    formatter = logging.Formatter('%(asctime)s,%(msecs)d,%(name)s,%(levelname)s,%(module)s,%(funcName)s,"%(message)s"')

    # Set the format for the file handler
    file_handler.setFormatter(formatter)

    # Set the format for the console handler
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info('Test log message.')


def print_logs_info():
    logging.info(f"Logs are stored in {LOGS_DIRECTORY} (filenames {LOG_FILENAME})")


def read_logs_as_df(historic_logs=False):
    col_names = ['datetime', 'msecs', 'logger', 'level', 'module', 'funcName', 'message']

    df_logs = pd.read_csv(LOGS_DIRECTORY / LOG_FILENAME,
                          names=col_names,
                          index_col=False)

    if historic_logs:
        for log_file in LOGS_DIRECTORY.glob('*.csv'):
            df_hist_logs = pd.read_csv(log_file, names=col_names, index_col=False)
            df_logs = pd.concat([df_logs, df_hist_logs])

        df_logs = df_logs.sort_values('msecs')

    return df_logs

setup_logging()
