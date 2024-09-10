import logging

from mecon import config
from mecon.etl.file_system import DatasetDir, Dataset


class WorkingDatasetDir(DatasetDir):
    def __init__(self, path=None):
        if path is None:
            path = config.DEFAULT_DATASETS_DIR_PATH
        super().__init__(path)

        first_dataset_name = self.datasets()[0].name if len(self.datasets()) > 0 else None
        curr_dataset_name = self.settings.get('CURRENT_DATASET', first_dataset_name)
        self.set_working_dataset(curr_dataset_name)

    @property
    def working_dataset(self) -> Dataset:
        return self._working_dataset

    def set_working_dataset(self, dataset_name) -> Dataset:
        self._working_dataset = self.get_dataset(dataset_name)
        logging.info(f"Setting new current working dataset to {dataset_name}")
        return self.working_dataset
