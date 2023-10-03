from __future__ import annotations  # TODO upgrade to python 3.11

import pandas as pd

import mecon2.datafields as fields
from mecon2.monitoring import logs


class Transactions(fields.DataframeWrapper, fields.IdColumnMixin, fields.DateTimeColumnMixin, fields.AmountColumnMixin,
                   fields.DescriptionColumnMixin, fields.TagsColumnMixin):
    """
    Responsible for holding the transactions dataframe and controlling the access to it, like a DataFrame Facade.
    Columns are specified and only accessed and modifies by the corresponding mixins. Key goal of this object
    is to solely represent sets of transactions and their app-level operations like the extraction of a subset (e.x
    filter out rows based on a field value) or a superset (e.x fill null dates to ensure non-empty groups)

    Not responsible for any IO operations.
    """

    @logs.codeflow_log_wrapper('#data#transactions#process')
    def __init__(self, df: pd.DataFrame):
        super().__init__(df=df)
        fields.IdColumnMixin.__init__(self, df_wrapper=self)
        fields.DateTimeColumnMixin.__init__(self, df_wrapper=self)
        fields.AmountColumnMixin.__init__(self, df_wrapper=self)
        fields.DescriptionColumnMixin.__init__(self, df_wrapper=self)
        fields.TagsColumnMixin.__init__(self, df_wrapper=self)

    def fill_values(self, fill_unit):

        return self.fill_dates(TransactionDateFiller(fill_unit=fill_unit))
        # filler =
        # return filler.fill(self)

    @classmethod
    def factory(cls, df: pd.DataFrame):
        # TODO check if sorted
        return super().factory(df)


class TransactionDateFiller(
    fields.DateFiller):  # TODO move other Transaction related classes here like TransactionAggregators
    def __init__(self,
                 fill_unit,
                 id_fill='',
                 amount_fill=0.0,
                 currency_fill='',
                 amount_curr=0.0,
                 description_fill='',
                 tags_fills=''):
        fill_values_dict = {
            'id': id_fill,
            'amount': amount_fill,
            'currency': currency_fill,
            'amount_cur': amount_curr,
            'description': description_fill,
            'tags': tags_fills
        }
        super().__init__(fill_unit, fill_values_dict)
