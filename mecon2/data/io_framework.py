import abc
from typing import List

import pandas as pd


class TagsIOABC(abc.ABC):
    """
    The interface of tags io operations used by the app
    """
    @abc.abstractmethod
    def get_tag(self, name):
        pass

    @abc.abstractmethod
    def set_tag(self, name, value):
        pass

    @abc.abstractmethod
    def delete_tag(self, name):
        pass

    @abc.abstractmethod
    def all_tags(self):
        pass


class TransactionsIOABC(abc.ABC):
    @abc.abstractmethod
    def get_transactions(self) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def delete_all(self) -> None:
        pass


class RawTransactionsIOABC(TransactionsIOABC, abc.ABC):
    @abc.abstractmethod
    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        pass


class HSBCTransactionsIOABC(RawTransactionsIOABC, abc.ABC):
    """
    HSBC bank statement files
    """


class MonzoTransactionsIOABC(RawTransactionsIOABC, abc.ABC):
    """
    Monzo bank statement files
    """


class RevoTransactionsIOABC(RawTransactionsIOABC, abc.ABC):
    """
    Revolut bank statement files
    """


class CombinedTransactionsIOABC(TransactionsIOABC, abc.ABC):
    """
    The interface of transactions io operations used by the app
    """
    @abc.abstractmethod
    def load_transactions(self):
        pass

    @abc.abstractmethod
    def update_tags(self, df_tags):
        pass

