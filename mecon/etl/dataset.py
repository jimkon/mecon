import logging
from pathlib import Path
from typing import Dict

import pandas as pd

from mecon import config
from mecon import settings


def _subfolder_csvs(path):
    result = {}
    for subfolder in path.iterdir():
        if subfolder.is_dir():
            csv_files = [p.name for p in subfolder.glob("*.csv")]
            result[subfolder.name] = csv_files

    return result


class Dataset:
    def __init__(self,
                 name: str,
                 db_path: Path,
                 statements_path: Path,
                 settings_path: Path):
        self._name = name
        self._sqlite = db_path
        self._statements = statements_path
        self._settings = settings.Settings(settings_path)

    @classmethod
    def from_dirpath(self, dir_path: Path):
        data_data = dir_path / 'data'

        db_path = data_data / 'db'
        db_path.mkdir(parents=True, exist_ok=True)

        statements_path = data_data / 'statements'
        statements_path.mkdir(parents=True, exist_ok=True)

        settings_path = dir_path / config.SETTINGS_JSON_FILENAME

        return Dataset(name=dir_path.name,
                       db_path=db_path,
                       statements_path=statements_path,
                       settings_path=settings_path)

    @property
    def name(self):
        return self._name

    @property
    def settings(self):
        return self._settings

    @property
    def db(self):
        return self._sqlite / config.DB_FILENAME

    @property
    def statements(self):
        return self._statements

    def statement_files(self) -> Dict:
        return _subfolder_csvs(self.statements)

    def add_statement(self, bank_name: str, statement_path: str | Path):
        statement_path = Path(statement_path)
        filename = statement_path.name
        new_statement_path = self.statements / bank_name / filename
        new_statement_path.parent.mkdir(parents=True, exist_ok=True)
        new_statement_path.write_bytes(statement_path.read_bytes())
        logging.info(f"Added Monzo statement file to {new_statement_path}")

    def add_df_statement(self, bank_name: str | Path, df: pd.DataFrame, filename: str):
        new_statement_path = self.statements / bank_name / filename
        new_statement_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(new_statement_path, index=False)
        logging.info(f"Added Monzo statement file with {len(df)} transactions to {new_statement_path}")


class DatasetDir:
    def __init__(self, path: str | Path):
        self._path = Path(path)
        if not self._path.exists():
            logging.warning(f"DatasetDir.__init__: Path {self._path} does not exist and will be created.")

        self._path.mkdir(parents=True, exist_ok=True)
        logging.info(f"New datasets directory in path '{path}'. #info#filesystem")

        self._datasets = []
        subpaths = list(self._path.iterdir())
        if len(subpaths) == 0:
            logging.warning(f"DatasetDir.__init__: Path {self._path} has no datasets inside.")

        for subpath in subpaths:
            if subpath.is_dir():
                self._datasets.append(Dataset.from_dirpath(subpath))
                logging.info(f"New dataset in path '{subpath}'. #info#filesystem")

        settings_path = self.path / config.SETTINGS_JSON_FILENAME
        self._settings = settings.Settings(settings_path)

    @property
    def name(self):
        return self.path.name

    @property
    def path(self):
        return self._path

    @property
    def settings(self):
        return self._settings

    def datasets(self):
        return self._datasets

    def is_empty(self):
        return len(self.datasets()) == 0

    def get_dataset(self, dataset_name) -> Dataset | None:
        if dataset_name is None or self.is_empty():
            return None

        dataset_path = self.path / dataset_name
        return Dataset.from_dirpath(dataset_path) if dataset_path.exists() else None
