import logging
import pathlib
from pathlib import Path
from typing import Dict, Literal

import pandas as pd

from mecon import config
from mecon import settings


def _subfolder_csvs(path):
    result = {}
    for subfolder in path.iterdir():
        if subfolder.is_dir():
            # csv_files = [p.name for p in subfolder.glob("*.csv")]
            csv_files = list(subfolder.glob("*.csv"))
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
        self._settings = settings.Settings(path=settings_path)


    @classmethod
    def from_dirpath(self, dir_path: Path | str):
        dir_path = pathlib.Path(dir_path)
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

    def __repr__(self):
        return f"Dataset({self.name}): {self.db}, {self.statements}"

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
    def __init__(self, path: str | Path, exist_ok: bool = False):
        self._path = Path(path)
        if not self._path.exists():
            if exist_ok:
                logging.warning(f"DatasetDir.__init__: Path {self._path} does not exist and will be created.")
                self._path.mkdir(parents=True, exist_ok=True)
            else:
                raise FileNotFoundError(f"DatasetDir.__init__: Path {self._path} does not exist.")

        # logging.info(f"New datasets directory in path '{path}'. #info#filesystem")

        self._datasets = {}
        subpaths = [p for p in self._path.iterdir() if not p.is_file()]
        if len(subpaths) == 0:
            raise FileNotFoundError(f"DatasetDir.__init__: Path {self._path} has no datasets inside.")

        logging.info(f"Adding {len(subpaths)} datasets from {path}. #info#filesystem")
        self.add_datasets_from_paths(subpaths)

    def add_datasets_from_paths(self,
                                paths: list[Path],
                                ignore_not_dir: bool = False,
                                invalid_dataset: Literal['ignore', 'raise', 'warn'] = 'warn'):
        for path in paths:
            if ignore_not_dir and not path.is_dir():
                logging.warning(f"DatasetDir.add_datasets_from_paths: Path {path} is not a directory.")
            else:
                logging.info(f"New dataset in path '{path}'. #info#filesystem")
                try:
                    new_dataset = Dataset.from_dirpath(path)
                except Exception as e:
                    if invalid_dataset == 'raise':
                        raise e
                    elif invalid_dataset == 'warn':
                        logging.warning(f"DatasetDir.add_datasets_from_paths: Error creating dataset from {path}: {e}")
                    continue

                if new_dataset.name in self._datasets:
                    logging.info(
                        f"DatasetDir.add_datasets_from_paths: Dataset {new_dataset.name} already exists and it will be overwritten.")

                self._datasets[new_dataset.name] = new_dataset

    @property
    def name(self):
        return self.path.name

    @property
    def path(self):
        return self._path

    def datasets(self):
        return list(self._datasets.values())

    def dataset_names(self):
        return list(self._datasets.keys())

    def is_empty(self):
        return len(self.datasets()) == 0

    def get_dataset(self, dataset_name: str) -> Dataset | None:
        if dataset_name is None or self.is_empty():
            return None

        # dataset_path = self.path / dataset_name
        # return Dataset.from_dirpath(dataset_path) if dataset_path.exists() else None
        return self._datasets.get(dataset_name)


class CustomisedDatasetDir(DatasetDir):
    def __init__(self, path: str | Path):
        super().__init__(path)
        settings_path = self.path / config.SETTINGS_JSON_FILENAME
        self._settings = settings.Settings(settings_path)

        external_datasets = [pathlib.Path(p) for p in self.settings.get('EXTERNAL_DATASETS_PATHS', [])]
        logging.info(f"Adding {len(external_datasets)} external datasets. #info#filesystem")
        self.add_datasets_from_paths(external_datasets)

    @property
    def settings(self):
        return self._settings
