import os

PATH_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATASETS_DIRECTORY = os.path.join(PATH_ROOT, 'datasets')
DATA_DIRECTORY = os.path.join(DATASETS_DIRECTORY, 'mydata')
STATEMENTS_DIRECTORY = os.path.join(DATA_DIRECTORY, 'statements')
TRANSACTIONS_DIRECTORY = os.path.join(DATA_DIRECTORY, 'transactions')
TAGS_DIRECTORY = os.path.join(DATA_DIRECTORY, 'tags')

STATEMENT_FILES_EXTENSIONS = ['.csv']

COMBINED_STATEMENT_CSV_PATH = os.path.join(STATEMENTS_DIRECTORY, 'combined_statement.csv')
TRANSACTIONS_CSV_PATH = os.path.join(TRANSACTIONS_DIRECTORY, 'transactions.csv')
FULLY_TAGGED_DATASET_CSV = os.path.join(TRANSACTIONS_DIRECTORY, 'fully_tagged_transactions.csv')

LINEAR_EXECUTION = False
