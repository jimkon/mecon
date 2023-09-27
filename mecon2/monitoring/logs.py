import io
import logging
import pathlib
import sys
from functools import wraps
from logging.handlers import TimedRotatingFileHandler

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
    file_formatter = logging.Formatter('%(asctime)s,%(name)s,%(levelname)s,%(module)s,%(funcName)s,#"%(message)s"#')
    console_formatter = logging.Formatter('[%(levelname)s]\t%(asctime)s - %(name)s:"%(message)s"')

    # Set the format for the file handler
    file_handler.setFormatter(file_formatter)

    # Set the format for the console handler
    console_handler.setFormatter(console_formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    print_logs_info()


def print_logs_info():
    logging.info(f"Logs are stored in {LOGS_DIRECTORY} (filenames {LOG_FILENAME})")


def read_logs_string_as_df(logs_string: str):
    col_names = ['datetime', 'msecs', 'logger', 'level', 'module', 'funcName', 'message']
    logs_string_io = io.StringIO(logs_string)
    df_logs = pd.read_csv(logs_string_io, sep=",", header=None, names=col_names, index_col=False)
    return df_logs


def read_logs_as_df(historic_logs=False):
    log_file = LOGS_DIRECTORY / LOG_FILENAME
    df_logs = read_logs_string_as_df(log_file.read_text())

    if historic_logs:
        for log_file in LOGS_DIRECTORY.glob('*.csv'):
            df_hist_logs = read_logs_string_as_df(log_file.read_text())
            df_logs = pd.concat([df_logs, df_hist_logs])

        df_logs = df_logs.sort_values('datetime')

    return df_logs


def codeflow_log_wrapper(tags=''):
    def decorator(_func):
        # https://flask.palletsprojects.com/en/2.3.x/patterns/viewdecorators/
        @wraps(_func)
        def wrapper(*args, **kwargs):
            _funct_name = f"{_func.__module__}.{_func.__qualname__}"
            logging.info(f"{_funct_name} started... #codeflow#start{tags}")
            try:
                res = _func(*args, **kwargs)
            except Exception as e:
                logging.info(f"{_funct_name} raised {e}! #codeflow#error{tags}")
                raise
            else:
                logging.info(f"{_funct_name} finished... #codeflow#end{tags}")
                return res

        return wrapper

    return decorator
