# TODO:v3 work with relative files
import logging
from os import getenv
from pathlib import Path

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

MECON_ROOT_DIRPATH = Path(getenv("MECON_ROOT_DIRPATH")) if getenv("MECON_ROOT_DIRPATH") else Path(__file__).parent.parent
logging.info(f"{MECON_ROOT_DIRPATH=}")

DEFAULT_DATASETS_DIR_PATH = MECON_ROOT_DIRPATH / "datasets"
SETTINGS_JSON_FILENAME = r"settings.json"
SETTINGS_JSON_FILEPATH = Path(DEFAULT_DATASETS_DIR_PATH, SETTINGS_JSON_FILENAME)

CURRENCY_LOOKUP_RATES_JSON_PATH = DEFAULT_DATASETS_DIR_PATH / "currency_rates.json"

CONDITION_JSON_MAX_SIZE = 2000
TAG_MONITORING = True

DATE_STRING_FORMAT = "%Y-%m-%d"
TIME_STRING_FORMAT = "%H:%M:%S"
DATETIME_STRING_FORMAT = f"{DATE_STRING_FORMAT} {TIME_STRING_FORMAT}"

# DEFAULT_DATASET_NAME = 'v2'

CREDS_DIRECTORY_PATH = MECON_ROOT_DIRPATH / "creds"

LOGS_DIRECTORY_PATH = MECON_ROOT_DIRPATH / "logs"
LOGS_DIRECTORY_PATH.mkdir(exist_ok=True)

CURRENT_LOG_FILENAME = "logs_raw.csv"
DB_FILENAME = 'sqlite3'

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
