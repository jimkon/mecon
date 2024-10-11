import pathlib
from io import StringIO
from typing import List, Union, Callable

import pandas as pd


class StatementDFMergeStrategies:
    @staticmethod
    def merge_last_first(dfs_list: List[pd.DataFrame], date_col_name: str) -> pd.DataFrame:
        dfs_sorted = sorted(dfs_list, key=lambda df: df[date_col_name].max())
        merged_df = dfs_sorted[-1].copy()
        for df in reversed(dfs_sorted[:-1]):
            filtered_df = df[df[date_col_name] < merged_df[date_col_name].min()]
            merged_df = pd.concat([filtered_df, merged_df], ignore_index=True)

        return merged_df


class StatementCSV:
    def __init__(self, df):
        self._df = df

    def validate(self):
        pass

    def transform(self):
        return self._df

    def dataframe(self) -> pd.DataFrame:
        self.validate()
        df = self.transform()
        return df

    @classmethod
    def from_path(cls, path: Union[pathlib.Path, str, StringIO]):
        return cls(pd.read_csv(path, index_col=None))

    def to_path(self, path: Union[pathlib.Path, str, StringIO]):
        self._df.to_csv(path, index=None)

    @classmethod
    def load_many_from_dir(cls, dir_path: Union[pathlib.Path, str],
                           filename_condition_f: Callable) -> List:  # TODO -> List[HSBCStatementCSV]:
        dir_path = pathlib.Path(dir_path)
        return [cls.from_path(file) for file in dir_path.glob('*.csv') if filename_condition_f(file.name)]

    @classmethod
    def from_all_paths_in_dir(cls, dir_path: Union[pathlib.Path, str],
                              date_col_name: str,
                              file_prefix: str):
        statements = cls.load_many_from_dir(dir_path, lambda name: name.startswith(file_prefix))
        dfs = [statement._df for statement in statements]
        single_df = StatementDFMergeStrategies.merge_last_first(dfs, date_col_name)
        return cls(single_df)

    def save_to_dest(self, dest_path):
        raise NotImplemented

    def split_and_store_files_based_on_year(self, dir_path: Union[pathlib.Path, str],
                                            date_col_name: str,
                                            filename_prefix: str) -> None:
        year_split_col = '_year'
        self._df[year_split_col] = self._df[date_col_name].dt.year
        for year in self._df[year_split_col].unique():
            year_df = self._df[self._df[year_split_col] == year]
            del year_df[year_split_col]
            filepath = dir_path / f"{filename_prefix}_FROM{year_df[date_col_name].min().strftime('%Y-%m-%d')}-TO{year_df[date_col_name].max().strftime('%Y-%m-%d')}"
            self.__class__(year_df).to_path(filepath)


class HSBCStatementCSV(StatementCSV):
    @classmethod
    def from_path(cls, path: Union[pathlib.Path, str, StringIO]):
        df = pd.read_csv(path, names=['date', 'description', 'amount'], header=None)  # super doesn't have header=None
        df['date'] = pd.to_datetime(df['date'].astype(str), format="%d/%m/%Y")
        return cls(df)

    def to_path(self, path: Union[pathlib.Path, str, StringIO]):
        self._df['date'] = self._df['date'].dt.strftime('%d/%m/%Y')
        self._df.to_csv(path, index=None, header=None)  # super doesn't have header=None

    @classmethod
    def from_all_paths_in_dir(cls, dir_path: Union[pathlib.Path, str],
                              date_col_name: str = 'date',
                              file_prefix: str = 'TransactionHistory'):
        return super().from_all_paths_in_dir(dir_path=dir_path,
                                             date_col_name=date_col_name,
                                             file_prefix=file_prefix)

    def split_and_store_files_based_on_year(self, dir_path: Union[pathlib.Path, str],
                                            date_col_name: str = 'date',
                                            filename_prefix: str = 'TransactionHistory') -> None:
        super().split_and_store_files_based_on_year(dir_path=dir_path,
                                                    date_col_name=date_col_name,
                                                    filename_prefix=filename_prefix)


class MonzoStatementCSV(StatementCSV):
    def transform(self):
        self._df.rename(
            columns={
                "Transaction ID": "id",
                "Date": "date",
                "Time": "time",
                "Type": "transaction_type",
                "Name": "name",
                "Emoji": "emoji",
                "Category": "category",
                "Amount": "amount",
                "Currency": "currency",
                "Local amount": "local_amount",
                "Local currency": "local_currency",
                "Notes and #tags": "notes_tags",
                "Address": "address",
                "Receipt": "receipt",
                "Description": "description",
                "Category split": "category_split",
                "Money Out": "money_out",
                "Money In": "money_in",
            },
            inplace=True,
            errors="raise")

        del self._df["id"]

        return self._df

    @classmethod
    def from_all_paths_in_dir(cls, dir_path: Union[pathlib.Path, str],
                              date_col_name: str = 'Date',
                              file_prefix: str = 'Monzo_Transactions'):
        return super().from_all_paths_in_dir(dir_path=dir_path,
                                             date_col_name=date_col_name,
                                             file_prefix=file_prefix)

    def split_and_store_files_based_on_year(self, dir_path: Union[pathlib.Path, str],
                                            date_col_name: str = 'Date',
                                            filename_prefix: str = 'Monzo_Transactions') -> None:
        super().split_and_store_files_based_on_year(dir_path=dir_path,
                                                    date_col_name=date_col_name,
                                                    filename_prefix=filename_prefix)


class RevoStatementCSV(StatementCSV):
    def transform(self):
        self._df.rename(
            columns={
                "Type": "type",
                "Product": "product",
                "Started Date": "start_date",
                "Completed Date": "completed_date",
                "Description": "description",
                "Amount": "amount",
                "Fee": "fee",
                "Currency": "currency",
                "State": "state",
                "Balance": "balance",
            },
            inplace=True,
            errors="raise")

        return self._df

    @classmethod
    def from_all_paths_in_dir(cls, dir_path: Union[pathlib.Path, str],
                              date_col_name: str = 'Started Date',
                              file_prefix: str = 'account-statement'):
        return super().from_all_paths_in_dir(dir_path=dir_path,
                                             date_col_name=date_col_name,
                                             file_prefix=file_prefix)

    def split_and_store_files_based_on_year(self, dir_path: Union[pathlib.Path, str],
                                            date_col_name: str = 'Started Date',
                                            filename_prefix: str = 'account-statement') -> None:
        super().split_and_store_files_based_on_year(dir_path=dir_path,
                                                    date_col_name=date_col_name,
                                                    filename_prefix=filename_prefix)

