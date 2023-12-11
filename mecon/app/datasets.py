from mecon.import_data.file_system import DatasetDir, Dataset
from mecon.utils.instance_management import Singleton
from mecon import config


class WorkingDatasetDir(DatasetDir, Singleton):
    def __init__(self, path=None):
        if path is None:
            path = config.DEFAULT_DATASETS_DIR_PATH
        super().__init__(path)

        self._working_dataset = self.datasets()[0]

    @property
    def working_dataset(self) -> Dataset:
        return self._working_dataset

    @classmethod
    def set_working_directory(cls, new_path):
        super().set_instance(WorkingDatasetDir(path=new_path))

    def set_working_dataset(self, dataset_name):
        self._working_dataset = self.get_dataset(dataset_name)

