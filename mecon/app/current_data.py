import logging
import pathlib
from typing import Dict

import pandas as pd

from mecon import config
from mecon.data.data_management import CachedFileDataManager
from mecon.app.data_manager import CachedDBDataManager
from mecon.app.db_extension import DBWrapper
from mecon.data.datafields import InvalidInputDataFrameColumns, NullDataframeInDataframeWrapper
from mecon.etl.dataset import CustomisedDatasetDir, Dataset
from mecon.etl.statements import HSBCStatementCSV, MonzoStatementCSV, RevoStatementCSV


# todo rename to working/current and file system to datasets

class WorkingDatasetDir(CustomisedDatasetDir):
    def __init__(self):
        path = config.DEFAULT_DATASETS_DIR_PATH
        super().__init__(path)
        self._working_dataset = None

        first_dataset_name = self.datasets()[0].name if len(self.datasets()) > 0 else None
        curr_dataset_name = self.settings.get('CURRENT_DATASET', first_dataset_name)
        self.set_working_dataset(curr_dataset_name)

    @property
    def working_dataset(self) -> Dataset:
        return self._working_dataset

    def set_working_dataset(self, dataset_name: str) -> Dataset:
        logging.info(f"Setting new current working dataset to '{dataset_name}'")
        self._working_dataset = self.get_dataset(dataset_name)
        return self.working_dataset


class WorkingDatasetDirInfo:
    def __init__(self):
        self._dataset_dir = WorkingDatasetDir()

    def statement_files_info(self) -> Dict:
        current_dataset = self._dataset_dir.working_dataset
        dirs_path = current_dataset.statements
        dm = WorkingDataManager()
        transformed_dict = current_dataset.statement_files().copy()

        for dir_name in transformed_dict:
            files_info = []
            for filename in transformed_dict[dir_name]:
                statement_filepath = pathlib.Path(dirs_path) / dir_name / filename
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

class WorkingDataManager(CachedFileDataManager):
    def __init__(self):
        super().__init__(WorkingDatasetDir().working_dataset)


# class WorkingDataManager(CachedDBDataManager):
#     def __init__(self):
#         db_path = WorkingDatasetDir().working_dataset.db
#         db = DBWrapper(db_path)
#         super().__init__(db)
#
#     def reset(self):
#         logging.info(f"Resetting database")
#         self.reset_statements()
#         statements_dir = WorkingDatasetDir().working_dataset.statements
#
#         hsbc_dfs = HSBCStatementCSV.from_all_paths_in_dir(statements_dir / 'HSBC').dataframe()
#         self.add_statement(hsbc_dfs, bank='HSBC')
#
#         monzo_dfs = MonzoStatementCSV.from_all_paths_in_dir(statements_dir / 'Monzo').dataframe()
#         self.add_statement(monzo_dfs, bank='Monzo')
#
#         revo_dfs = RevoStatementCSV.from_all_paths_in_dir(statements_dir / 'Revolut').dataframe()
#         self.add_statement(revo_dfs, bank='Revolut')
#
#         self.reset_transactions()
#         logging.info(f"Resetting database finished")


class WorkingDataManagerInfo:
    def __init__(self):
        self._data_manager = WorkingDataManager()

    def statements_info(self):
        res = {}
        hsbc_trans = self._data_manager.get_hsbc_statements()
        if hsbc_trans is not None:
            # hsbc_trans['date'] = pd.to_datetime(hsbc_trans['date'], format="%d/%m/%Y") # ValueError: time data "2020-12-27 00:00:00.000000" doesn't match format "%d/%m/%Y", at position 0. You might want to try:
            hsbc_trans['date'] = pd.to_datetime(hsbc_trans['date'])
            res['HSBC'] = {
                'transactions': len(hsbc_trans),
                'days': hsbc_trans['date'].nunique(),
                'min_date': hsbc_trans['date'].min(),
                'max_date': hsbc_trans['date'].max(),
            }

        monzo_trans = self._data_manager.get_monzo_statements()
        if monzo_trans is not None:
            monzo_trans['date'] = pd.to_datetime(monzo_trans['date'], format="%Y-%m-%d")
            res['Monzo'] = {
                'transactions': len(monzo_trans),
                'days': monzo_trans['date'].nunique(),
                'min_date': monzo_trans['date'].min(),
                'max_date': monzo_trans['date'].max(),
            }

        revo_trans = self._data_manager.get_revo_statements()
        if revo_trans is not None:
            revo_trans['start_date'] = pd.to_datetime(revo_trans['start_date'], format="%Y-%m-%d %H:%M:%S")
            res['Revolut'] = {
                'transactions': len(revo_trans),
                'days': revo_trans['start_date'].nunique(),
                'min_date': revo_trans['start_date'].min(),
                'max_date': revo_trans['start_date'].max(),
            }

        return res

    def transactions_info(self):
        try:
            transactions = self._data_manager.get_transactions()
            res = {
                'rows': transactions.size()
            }
        except (NullDataframeInDataframeWrapper, InvalidInputDataFrameColumns):
            res = {'No transaction data'}
        return res
