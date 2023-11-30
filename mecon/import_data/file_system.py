import logging
import pathlib
from typing import Dict


def _subfolder_csvs(path):
    result = {}
    for subfolder in path.iterdir():
        if subfolder.is_dir():
            csv_files = [p.name for p in subfolder.glob("*.csv")]
            result[subfolder.name] = csv_files

    return result


class Dataset:
    def __init__(self, path: str | pathlib.Path):
        self._path = pathlib.Path(path)
        logging.info(f"New dataset in path {self._path} #info#filesystem")

        self._data = self._path / 'data'

        self._sqlite = self._data / 'db'
        self._sqlite.mkdir(parents=True, exist_ok=True)

        self._statements = self._data / 'statements'
        self._statements.mkdir(parents=True, exist_ok=True)

    @property
    def db(self):
        return self._sqlite / 'sqlite3'

    @property
    def statements(self):
        return self._statements

    def statement_files(self) -> Dict:
        return _subfolder_csvs(self.statements)

    def add_statement(self, bank_name: str, statement_path: str | pathlib.Path):
        statement_path = pathlib.Path(statement_path)
        filename = statement_path.name
        new_statement_path = self.statements / bank_name / filename
        new_statement_path.parent.mkdir(parents=True, exist_ok=True)
        new_statement_path.write_bytes(statement_path.read_bytes())


class DatasetDir:
    def __init__(self, path: str | pathlib.Path):
        self._path = pathlib.Path(path)
        logging.info(f"New datasets directory in path {self._path} #info#filesystem")
        self._path.mkdir(parents=True, exist_ok=True)

        self._datasets = []
        for subpath in self._path.iterdir():
            if subpath.is_dir():
                self._datasets.append(Dataset(subpath))

    @property
    def path(self):
        return self._path

    def datasets(self):
        return self._datasets

    def get_dataset(self, dataset_name) -> Dataset:
        dataset_path = self.path / dataset_name
        return Dataset(dataset_path) if dataset_path.exists() else None
