from mecon import config
from mecon.etl.file_system import DatasetDir, Dataset
from mecon.settings import GlobalSettings
from mecon.utils.instance_management import Singleton


class WorkingDatasetDir(DatasetDir, Singleton):
    def __init__(self, path=None):
        if path is None:
            path = config.DEFAULT_DATASETS_DIR_PATH
        super().__init__(path)

        curr_dataset_name = GlobalSettings()['CURRENT_DATASET']
        self.set_working_dataset(curr_dataset_name)

    @property
    def working_dataset(self) -> Dataset:
        return self._working_dataset

    @classmethod
    def set_working_directory(cls, new_path):
        super().set_instance(WorkingDatasetDir(path=new_path))

    def set_working_dataset(self, dataset_name) -> Dataset:
        self._working_dataset = self.get_dataset(dataset_name)
        return self.working_dataset

