# TODO:v3 work with relative files
import pathlib

DEFAULT_DATASETS_DIR_PATH = r"C:\Users\dimitris\PycharmProjects\mecon\datasets"

CURRENCY_LOOKUP_RATES_JSON_PATH = r"C:\Users\dimitris\PycharmProjects\mecon\datasets\currency_rates.json"

CONDITION_JSON_MAX_SIZE = 2000
TAG_MONITORING = True

DATE_STRING_FORMAT = "%Y-%m-%d"
TIME_STRING_FORMAT = "%H:%M:%S"
DATETIME_STRING_FORMAT = f"{DATE_STRING_FORMAT} {TIME_STRING_FORMAT}"

DEFAULT_DATASET_NAME = 'v2'

CREDS_DIRECTORY_PATH = pathlib.Path(r"C:\Users\dimitris\PycharmProjects\mecon\creds")

LOGS_DIRECTORY_PATH = pathlib.Path("logs")
CURRENT_LOG_FILENAME = "logs_raw.csv"
