from __future__ import annotations  # TODO upgrade to python 3.11

import pandas as pd

import mecon2.datafields as fields
from mecon2 import tagging


class ZeroSizeTransactionsError(Exception):
    pass


class Transactions(fields.DataframeWrapper, fields.IdColumnMixin, fields.DateTimeColumnMixin, fields.AmountColumnMixin, fields.DescriptionColumnMixin, fields.TagsColumnMixin):
    """
    Responsible for holding the transactions dataframe and controlling the access to it, like a DataFrame Facade.
    Columns are specified and only accessed and modifies by the corresponding mixins. Key goal of this object
    is to solely represent sets of transactions and their app-level operations like the extraction of a subset (e.x
    filter out rows based on a field value) or a superset (e.x fill null dates to ensure non-empty groups)

    Not responsible for any IO operations.
    """

    def __init__(self, df: pd.DataFrame):
        if df is None:
            raise ZeroSizeTransactionsError
        super().__init__(df=df)
        fields.IdColumnMixin.__init__(self, df_wrapper=self)
        fields.DateTimeColumnMixin.__init__(self, df_wrapper=self)
        fields.AmountColumnMixin.__init__(self, df_wrapper=self)
        fields.DescriptionColumnMixin.__init__(self, df_wrapper=self)
        fields.TagsColumnMixin.__init__(self, df_wrapper=self)
        fields.TagsColumnMixin.__init__(self, df_wrapper=self)


