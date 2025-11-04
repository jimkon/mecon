import logging
import pathlib
import shutil
from pathlib import Path
from typing import Dict, Literal
from datetime import datetime

import pandas as pd

from mecon import config
from mecon import settings
from mecon.settings import DictFile


def _subfolder_csvs(path):
    result = {}
    for subfolder in path.iterdir():
        if subfolder.is_dir():
            # csv_files = [p.name for p in subfolder.glob("*.csv")]
            csv_files = sorted(subfolder.glob("*.csv"))
            result[subfolder.name] = csv_files

    return result


class DatasetV1:
    # TODO enrich this class with much more functionality, everything related to specific Datasets should start from here. make this object a Mediator
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

        return DatasetV1(name=dir_path.name,
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

    def statement_files(self, filter_option: Literal['all', 'settings'] = 'settings') -> Dict:
        all_files = _subfolder_csvs(self.statements)
        # if filter_option == 'all':
        #     return all_files
        # elif filter_option == 'settings' and 'sources' in self.settings:
        #     selected_sources = self.settings['sources']
        #     selected_files = {k: all_files[v] for k, v in selected_sources}
        #     return selected_files
        # else:
        #     raise ValueError(f"Invalid filter_option: {filter_option}")
        if 'sources' not in self.settings:
            return all_files

        if self.settings['sources'][
            'Monzo'] == 'MonzoAPI':  # TODO temporary solution untill all sources are selected in the app
            del all_files['Monzo']
        else:
            del all_files['MonzoAPI']
        return all_files

    # TODO remove
    def add_statement(self, bank_name: str, statement_path: str | Path):
        statement_path = Path(statement_path)
        filename = statement_path.name
        new_statement_path = self.statements / bank_name / filename
        new_statement_path.parent.mkdir(parents=True, exist_ok=True)
        new_statement_path.write_bytes(statement_path.read_bytes())
        logging.info(f"Added Monzo statement file to {new_statement_path}")

    # TODO remove
    def add_df_statement(self, bank_name: str | Path, df: pd.DataFrame, filename: str):
        new_statement_path = self.statements / bank_name / filename
        new_statement_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(new_statement_path, index=False)
        logging.info(f"Added Monzo statement file with {len(df)} transactions to {new_statement_path}")


class DatasetV2:
    """
    Directory Dataset
    """

    def __init__(self,
                 path: Path | str):
        path = pathlib.Path(path)
        if path.is_file():
            raise ValueError(f"DatasetV2 can only be initialized from a directory: '{path}' given")

        if not path.exists():
            raise ValueError(f"DatasetV2 can only be initialized from an existing directory: '{path}' does not exist")

        self._path = pathlib.Path(path)
        self._name = self.path.stem
        self._data = self._path / 'data'
        self._current_data = self._data / 'current'
        self._statements = self._data / 'statements'
        self._settings = None
        self._build_file_structure()

    def __repr__(self):
        return f"DatasetV2({self.name}): {self._path}"

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    @property
    def current_data(self):
        return self._current_data

    @property
    def statements(self):
        return self._statements

    @property
    def settings(self):
        return self._settings

    @property
    def creds(self):
        path = self.path.parent / config.CREDS_FILENAME
        creds = DictFile(path)
        return creds

    @property
    def db(self):
        raise DeprecationWarning("db attribute is deprecated, use 'current_data' instead")
        return self.current_data

    def _build_file_structure(self):
        self.path.mkdir(parents=True, exist_ok=True)
        self.current_data.mkdir(parents=True, exist_ok=True)
        self.statements.mkdir(parents=True, exist_ok=True)
        self._settings = settings.Settings(path=self.path)
        # TODO version check

    def statement_files(self, filter_option: Literal['all', 'settings'] | None = 'settings') -> Dict:
        if filter_option is None:
            filter_option = "all" if 'sources' not in self.settings and 'filter' not in self.settings['sources'] else \
            self.settings['sources']['filter']

        all_files = _subfolder_csvs(self.statements)

        if filter_option == 'all':
            return all_files
        elif filter_option == 'settings':
            if 'sources' not in self.settings:
                logging.warning(f"No sources defined in settings file '{self._settings.path}'")
                return all_files

            if set(all_files.keys()) != set(self.settings['sources'].keys()):
                logging.warning(
                    f"Discrepancy between sources found ({set(all_files.keys())}) and the ones defined in settings file settings ({set(self.settings['sources'].keys())})")

            selected_files = {source_name: all_files[source_name] for source_name, is_enabled in
                              self.settings['sources'].items() if is_enabled}
            logging.info(f"Selected files: {self.settings['sources']}")
            return selected_files
        else:
            raise ValueError(f"Invalid filter_option: {filter_option}")

    def statement_files_info(self) -> Dict:
        transformed_dict = self.statement_files()

        for dir_name in transformed_dict:
            files_info = []
            for filename in transformed_dict[dir_name]:
                statement_filepath = self.statements / dir_name / filename
                try:
                    df = pd.read_csv(statement_filepath)
                    stats = len(df)
                except FileNotFoundError | ValueError:
                    stats = 'error while reading file'

                files_info.append((statement_filepath, filename, stats))
            transformed_dict[dir_name] = files_info

        return transformed_dict

    def statement_files_info_df(self) -> pd.DataFrame:
        info_json = self.statement_files_info()

        dfs = []
        for bank, rows in info_json.items():
            df = pd.DataFrame(rows, columns=['path', 'filename', 'rows'])
            df['source'] = bank
            dfs.append(df)

        merged_df = pd.concat(dfs, ignore_index=True)[['source', 'filename', 'rows', 'path', ]]
        return merged_df

    @classmethod
    def from_dirpath(cls, dirpath: Path):
        # redundant, just because it existed in DatasetV1
        return DatasetV2(dirpath)


# TODO, trick to easily replace Dataset original (v1) with V2, possibly a bad idea
class Dataset(DatasetV2):
    pass


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


class DateRollingDataset:
    def __init__(self,
                 path: str | Path,
                 max_number_of_datasets: int = 10):
        self._path = pathlib.Path(path)
        self.max_number_of_datasets = max_number_of_datasets

        self._datasets = {}
        self.find_datasets()

        # self.rollover()

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

    def find_datasets(self):
        self._datasets = {}
        logging.info(f"{list(self.path.iterdir())=}")
        for dataset in self.path.iterdir():
            if dataset.is_dir() and dataset.name.isnumeric() and len(dataset.name) == 8:
                self._datasets[dataset.name] = Dataset.from_dirpath(dataset)
            else:
                logging.info(f"Skipping {dataset} as it is not a valid path.")
        logging.info(f"Found {len(self._datasets)} datasets. #info#filesystem")

    def get_dataset(self, dataset_name: str) -> Dataset | None:
        if dataset_name is None or self.is_empty():
            return None

        # dataset_path = self.path / dataset_name
        # return Dataset.from_dirpath(dataset_path) if dataset_path.exists() else None
        return self._datasets.get(dataset_name)

    def get_last_dataset(self) -> Dataset | None:
        if self.is_empty():
            logging.info(f"DatasetDir.get_last_dataset: Dataset Directory '{self.path}' has no datasets inside.")
            return None

        dataset_names = self.dataset_names()
        last_dataset = max(dataset_names)
        return self.get_dataset(last_dataset)

    def delete_first_dataset(self):
        if self.is_empty():
            return

        dataset_names = self.dataset_names()
        first_dataset = min(dataset_names)
        shutil.rmtree(self.path / first_dataset)
        logging.info(f"Removed dataset {first_dataset}. #info#filesystem")
        self.find_datasets()

    def rollover(self):
        today_id = datetime.today().strftime("%Y%m%d")
        if today_id in self.dataset_names():
            logging.info(f"No Dataset rollover needed,  '{today_id}' dataset already exists. #info#filesystem")
            return

        logging.info(f"Dataset '{today_id}' not found among the datasets {self.dataset_names()}. Rolling over...")
        last_dataset_path = self.get_last_dataset().path
        today_path = last_dataset_path.parent / today_id
        shutil.copytree(last_dataset_path, today_path, dirs_exist_ok=True)
        logging.info(f"Dataset rollover from {last_dataset_path.name} to {today_id}. #info#filesystem")
        self.find_datasets()

        if len(self.datasets()) > self.max_number_of_datasets:
            self.delete_first_dataset()
