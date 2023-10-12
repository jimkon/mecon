from mecon2.data.file_system import DatasetDir
from mecon2.utils.instance_management import Singleton
from mecon2 import config


class WorkingDatasetDir(DatasetDir, Singleton):
    def __init__(self, path=None):
        if path is None:
            path = config.DEFAULT_DATASETS_DIR_PATH
        super().__init__(path)

    @classmethod
    def set_working_directory(cls, new_path):
        super().set_instance(WorkingDatasetDir(path=new_path))
