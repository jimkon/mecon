import os

PATH_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATASET_DIRECTORY = os.path.join(PATH_ROOT, 'statements')
FULLY_TAGGED_DATASET_CSV = os.path.join(DATASET_DIRECTORY, 'fully_tagged_statement.csv')

LINEAR_EXECUTION = False
