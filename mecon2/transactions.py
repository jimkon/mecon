from __future__ import annotations  # TODO upgrade to python 3.11

import pandas as pd

import mecon2.datafields as fields
from mecon2 import tagging


class ZeroSizeTransactionsError(Exception):
    pass


class Transactions(fields.DateTimeColumnMixin, fields.AmountColumnMixin, fields.TagsColumnMixin, fields.DescriptionColumnMixin):
    """
    Responsible for holding the transactions dataframe and controlling the access to it, like a DataFrame Facade.
    Columns are specified and only accessed and modifies by the corresponding mixins. Key goal of this object
    is to solely represent sets of transactions and their app-level operations like the extraction of a subset (e.x
    filter out rows based on a field value) or a superset (e.x fill null dates to ensure non-empty groups)

    Not responsible for any IO operations.
    """
    def __init__(self, df):
        if df is None:
            raise ZeroSizeTransactionsError
        self._df = df
        super().__init__(df)

    def apply_rule(self, rule: tagging.AbstractRule) -> Transactions:
        return self

    def apply_tag(self, tag : tagging.Tag) -> Transactions:
        return self

    def filter_by(self, **kwargs) -> Transactions:
        return self

    def dataframe(self) -> pd.DataFrame:
        return self._df

    def copy(self) -> Transactions:
        return Transactions(self.dataframe())

    def merge(self, transactions) -> Transactions:
        df = pd.concat([self.dataframe(), transactions.dataframe()]).drop_duplicates()
        return Transactions(df)

    def size(self):
        return len(self._df)
