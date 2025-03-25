import datetime
import pathlib

import pandas as pd

from mecon.data import graphs
from mecon.etl.dataset import DatasetDir, Dataset

if __name__ == '__main__':
    from mecon import config
    from mecon.app.current_data import WorkingDataManager, WorkingDatasetDir, WorkingDatasetDirInfo

    datasets_dir = pathlib.Path("/Users/wimpole/Library/CloudStorage/GoogleDrive-jimitsos41@gmail.com/Other computers/My Laptop/datasets")#config.DEFAULT_DATASETS_DIR_PATH
    if not datasets_dir.exists():
        raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

    # datasets_obj = WorkingDatasetDir()
    # datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
    # dataset = datasets_obj.working_dataset
    data_manager = WorkingDataManager()

    dataset_dir_info = WorkingDatasetDirInfo()
    dataset_dir_info._current_dataset = Dataset.from_dirpath(datasets_dir / 'shared')
    prev_statements = [path for path, *_ in dataset_dir_info.statement_files_info()['Monzo']]
    df_prev = pd.concat([pd.read_csv(file, index_col=None) for file in prev_statements])
    pass
