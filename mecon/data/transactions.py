from __future__ import annotations  # TODO:v2 upgrade to python 3.11

from collections import Counter

import pandas as pd

import data.datafields as fields
from mecon.monitoring import logs


class Transactions(fields.DatedDataframeWrapper, fields.IdColumnMixin, fields.AmountColumnMixin,
                   fields.DescriptionColumnMixin, fields.TagsColumnMixin):
    """
    Responsible for holding the transactions dataframe and controlling the access to it, like a DataFrame Facade.
    Columns are specified and only accessed and modifies by the corresponding mixins. Key goal of this object
    is to solely represent sets of transactions and their app-level operations like the extraction of a subset (e.x
    filter out rows based on a field value) or a superset (e.x fill null dates to ensure non-empty groups)

    Not responsible for any IO operations.
    """

    @logs.codeflow_log_wrapper('#import_data#transactions#process')
    def __init__(self, df: pd.DataFrame):
        super().__init__(df=df)
        fields.IdColumnMixin.__init__(self, df_wrapper=self)
        fields.AmountColumnMixin.__init__(self, df_wrapper=self)
        fields.DescriptionColumnMixin.__init__(self, df_wrapper=self)
        fields.TagsColumnMixin.__init__(self, df_wrapper=self)

    def fill_values(self, fill_unit):
        return self.fill_dates(TransactionDateFiller(fill_unit=fill_unit))

    @classmethod
    def factory(cls, df: pd.DataFrame):
        return super().factory(df)

    def to_html(self):
        def counts(curr_string):
            value_counts = Counter(curr_string.split(','))

            res_str = ''
            for curr, count in value_counts.items():
                res_str += f"{curr}:{count} "
            return res_str

        df = self.dataframe().iloc[::-1].reset_index(drop=True)
        df['currency'] = df['currency'].apply(counts)
        html_table = df.to_html()
        res_html = f"<h1>Transactions table ({len(df)} rows)</h1>\n{html_table}"
        return res_html


# TODO:v3 move other Transaction related classes here like TransactionAggregators
class TransactionDateFiller(fields.DateFiller):
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
