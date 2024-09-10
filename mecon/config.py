# TODO:v3 work with relative files
import pathlib

DEFAULT_DATASETS_DIR_PATH = pathlib.Path(r"C:\Users\dimitris\PycharmProjects\mecon\datasets")
SETTINGS_JSON_FILENAME = r"settings.json"
SETTINGS_JSON_FILEPATH = pathlib.Path(DEFAULT_DATASETS_DIR_PATH, SETTINGS_JSON_FILENAME)

CURRENCY_LOOKUP_RATES_JSON_PATH = r"C:\Users\dimitris\PycharmProjects\mecon\datasets\currency_rates.json"

CONDITION_JSON_MAX_SIZE = 2000
TAG_MONITORING = True

DATE_STRING_FORMAT = "%Y-%m-%d"
TIME_STRING_FORMAT = "%H:%M:%S"
DATETIME_STRING_FORMAT = f"{DATE_STRING_FORMAT} {TIME_STRING_FORMAT}"

# DEFAULT_DATASET_NAME = 'v2'

CREDS_DIRECTORY_PATH = pathlib.Path(r"C:\Users\dimitris\PycharmProjects\mecon\creds")

LOGS_DIRECTORY_PATH = pathlib.Path(r"C:\Users\dimitris\PycharmProjects\mecon\logs")
LOGS_DIRECTORY_PATH.mkdir(exist_ok=True)

CURRENT_LOG_FILENAME = "logs_raw.csv"

HISTORIC_PERFORMANCE_DATA_DIRECTORY_PATH = LOGS_DIRECTORY_PATH / 'historic_perf_data'
HISTORIC_PERFORMANCE_DATA_DIRECTORY_PATH.mkdir(exist_ok=True)

HISTORIC_PERFORMANCE_DATA_FILENAME = "performance_data.csv"

TRANSACTIONS_CHUNK_SIZE = 250

EXPECTED_MONZO_COLUMNS_IN_RAW_STATEMENT = {'Transaction',
                                           "ID",
                                           "Date", "Time", "Type", "Name", "Emoji",
                                           "Category", "Amount", "Currency", "Local amount",
                                           "Local currency", "Notes and  # tags", "Address", "Receipt", "Description",
                                           "Category split",
                                           "Money Out", "Money In"}
