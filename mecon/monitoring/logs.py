import io
import logging
import pathlib
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import List

import pandas as pd

from mecon import config
from mecon.monitoring import log_data

"""Use log rotation Implement log rotation to prevent excessive file growth and disk space usage in your log files. 
Old log files are automatically archived, compressed, and deleted using this approach. You can either use a 
third-party tool like logrotate or the Python language's built-in RotatingFileHandler to construct your own solution.
"""

"""
Level,              Value,  What it means / When to use it
-                   -       -
logging.NOTSET,     0,      When set on a logger, indicates that ancestor loggers are to be consulted to determine the effective level. If that still resolves to NOTSET, then all events are logged. When set on a handler, all events are handled.
logging.DEBUG,      10,     Detailed information, typically only of interest to a developer trying to diagnose a problem.
logging.INFO,       20,     Confirmation that things are working as expected.
logging.WARNING,    30,     An indication that something unexpected happened, or that a problem might occur in the near future (e.g. ‘disk space low’). The software is still working as expected.
logging.ERROR,      40,     Due to a more serious problem, the software has not been able to perform some function.
logging.CRITICAL,   50,     A serious error, indicating that the program itself may be unable to continue running.
"""


class TimedRotatingFileHandlerV1(TimedRotatingFileHandler):
    def __init__(self):
        super().__init__(
            filename=config.LOGS_DIRECTORY_PATH / config.CURRENT_LOG_FILENAME,
            when='midnight',
            interval=1,
            backupCount=7,
            encoding='utf-8',
            # suffix='%Y-%m-%d'  # Include the date as a suffix in the log file name
            delay=True,  # to resolve the PermissionError
        )

    def doRollover(self) -> None:
        df_logs = read_logs_as_df([pathlib.Path(self.baseFilename)])
        super().doRollover()
        current_log_data = log_data.LogData.from_raw_logs(df_logs)
        perf_data = HistoricalPerformanceData.load_historical_data()
        perf_data.append_log_data(current_log_data)
        perf_data.store()


def setup_logging(logger=None):
    # Create a logger
    if logger is None:
        logger = logging.getLogger()

    # Set the logging level (e.g., INFO, DEBUG)
    logger.setLevel(logging.DEBUG)

    # Create a file handler (logs to a file)
    file_handler = TimedRotatingFileHandlerV1()
    # Create a console handler (logs to the console)
    console_handler = logging.StreamHandler(sys.stdout)

    # Define a format for log messages
    file_formatter = logging.Formatter('%(asctime)s,%(name)s,%(levelname)s,%(module)s,%(funcName)s,~%(message)s~')
    console_formatter = logging.Formatter('[%(levelname)s]\t%(asctime)s - %(name)s:"%(message)s"')

    # Set the format for the file handler
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    # Set the format for the console handler
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    print_logs_info()


def print_logs_info():
    logging.info(
        f"Logs are stored in {config.LOGS_DIRECTORY_PATH} (filename: {config.CURRENT_LOG_FILENAME}, historic: {get_log_files(True)})")


def read_logs_string_as_df(logs_string: str):
    logs_string_io = io.StringIO(logs_string)

    col_names = ['datetime', 'msecs', 'logger', 'level', 'module', 'funcName', 'message']
    df_logs = pd.read_csv(logs_string_io, sep=",", header=None, names=col_names, index_col=False, quotechar='~')
    df_logs.dropna(inplace=True)
    df_logs['msecs'] = df_logs['msecs'].astype('int64')
    df_logs.reset_index(drop=True, inplace=True)
    return df_logs


def get_log_files(historic_logs=False):
    if historic_logs:
        log_files = list(config.LOGS_DIRECTORY_PATH.glob('*'))
    else:
        log_files = [config.LOGS_DIRECTORY_PATH / config.CURRENT_LOG_FILENAME]
    return log_files


def read_logs_as_df(log_files: List[pathlib.Path]):
    df_logs = None
    for log_file in log_files:
        log_text = log_file.read_text(encoding='utf8')
        if df_logs is None:
            df_logs = read_logs_string_as_df(log_text)
        else:
            df_logs_temp = read_logs_string_as_df(log_text)
            df_logs = pd.concat([df_logs, df_logs_temp])

    df_logs = df_logs.sort_values(['datetime', 'msecs']).reset_index(drop=True)

    return df_logs


class HistoricalPerformanceData(log_data.PerformanceData):
    _filename = config.HISTORIC_PERFORMANCE_DATA_DIRECTORY_PATH / config.HISTORIC_PERFORMANCE_DATA_FILENAME

    def append_log_data(self, new_log_data: log_data.LogData):
        perf_data = log_data.PerformanceData.from_log_data(new_log_data)
        self._df = self.merge(perf_data).dataframe()

    def store(self):
        self._df.to_csv(self._filename, index=False, header=False)

    @classmethod
    def load_historical_data(cls):
        df = pd.read_csv(cls._filename, index_col=None, header=None, names=['datetime', 'execution_time', 'tags'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['execution_time'] = df['execution_time'].astype(float)
        return cls(df)



