import abc
from typing import List, Any

import pandas as pd

"""
io_framework manages io for the tables AND the relations between them.
for example, there is no way to set a custom table for the transactions. 
Instea, transactions can only be set by the load function which gets
the statements from the different banks, transforms them and loads them
into the transaction table.

This will cause an issue if we decentralise where the statements of each bank
are stores. In that case, I may need to separate the io from the relationship
management, by making a clear io framework for each table (get, set, update, delete)
which is closer to the CRUD framework and a RESTful API, and a relation manager
that automates the triggers for each operation (this is partially implemented 
in the DataManager and may probably be implemented fully there).
"""


class TagsIOABC(abc.ABC):
    """
    The interface of tags io operations used by the app
    """

    @abc.abstractmethod
    def get_tag(self, name: str) -> list[str, Any] | None:
        pass

    @abc.abstractmethod
    def set_tag(self, name: str, conditions_json: list | dict) -> None:
        pass

    @abc.abstractmethod
    def delete_tag(self, name: str) -> bool:
        pass

    @abc.abstractmethod
    def all_tags(self) -> list[dict]:
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
    def load_transactions(self) -> None:
        pass

    @abc.abstractmethod
    def update_tags(self, df_tags) -> None:
        pass


class StatementsAccess:
    def __init__(self,
                 hsbc: HSBCTransactionsIOABC,
                 monzo: MonzoTransactionsIOABC,
                 revo: RevoTransactionsIOABC
                 ):
        self._hsbc = hsbc
        self._monzo = monzo
        self._revo = revo

    @property
    def hsbc_statements(self) -> HSBCTransactionsIOABC:
        return self._hsbc

    @property
    def monzo_statements(self) -> MonzoTransactionsIOABC:
        return self._monzo

    @property
    def revo_statements(self) -> RevoTransactionsIOABC:
        return self._revo
