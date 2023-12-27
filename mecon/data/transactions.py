from __future__ import annotations  # TODO:v2 upgrade to python 3.11

import abc
from collections import Counter
from datetime import datetime, date

import pandas as pd

import data.datafields as fields
from mecon.monitoring import logs
from mecon.utils import dataframe_transformers, calendar_utils


class Transactions(fields.DatedDataframeWrapper, fields.IdColumnMixin, fields.AmountColumnMixin,
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
        fields.AmountColumnMixin.__init__(self, df_wrapper=self)
        fields.DescriptionColumnMixin.__init__(self, df_wrapper=self)
        fields.TagsColumnMixin.__init__(self, df_wrapper=self)

    def fill_values(self, fill_unit,
                    start_date: datetime | date | None = None,
                    end_date: datetime | date | None = None):
        return self.fill_dates(TransactionDateFiller(fill_unit=fill_unit),
                               start_date=start_date, end_date=end_date)

    @classmethod
    def factory(cls, df: pd.DataFrame):
        return super().factory(df)

    def to_html(self):
        styles = """
        <style>
        ul {
            margin-right: 10px;
            padding-left: 20px;
        }
        .tag_label {
            font-size: 12px;
            background-color: #dad;
            padding: 3px;
            border: 1px solid #555;
            display: inline-block;
        }
        .datetime_label {
            white-space: nowrap;
            font-size: 16px;
        }
        .description_phrase_label {
            font-size: 14px;
            background-color: #ddd;
            padding: 2px;
        }
    </style>
        """
        df = self.dataframe(df_transformer=TransactionsHTMLTableFormat())
        html_table = df.to_html(escape=False, index=False)
        res_html = f"{styles}<h1>Transactions table ({len(df)} rows)</h1>\n{html_table}"
        return res_html

    def dataframe(self, df_transformer: AbstractTransactionsTransformer = None):
        df = super().dataframe()
        if df_transformer:
            df = df_transformer.transform(df)
        return df


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


class AbstractTransactionsTransformer(dataframe_transformers.DataframeTransformer, abc.ABC):
    def validate_input_df(self, df_in):
        missing_cols = []
        for col in ['id', 'amount', 'currency', 'amount_cur', 'description', 'tags']:
            if col not in df_in.columns:
                missing_cols.append(col)

        if len(missing_cols) > 0:
            raise ValueError(f"Missing required columns for Transaction Transformer: {missing_cols}")


class TransactionsHTMLTableFormat(AbstractTransactionsTransformer):
    def _transform(self, df_in) -> pd.DataFrame:
        df_in = df_in.copy().iloc[::-1].reset_index(drop=True)

        df_out = pd.DataFrame(df_in['id'].apply(lambda _id: f"<h5>{_id}</h5>"))
        df_out['Date/Time'] = df_in['datetime'].apply(self._format_datetime)
        df_out['Amount (£)'] = df_in['amount'].apply(self._format_amount)
        df_out['Curr'] = df_in['currency'].apply(self._format_currency_count)
        df_out['Amount (curr)'] = df_in['amount_cur'].apply(self._format_amount)
        df_out['Description'] = df_in['description'].apply(self._format_description)
        df_out['Tags'] = df_in['tags'].apply(self._format_tags)
        return df_out

    @staticmethod
    def _format_datetime(dt):
        formatted_date = dt.strftime("%A, %b %w, %Y")
        formatted_time = dt.strftime("%I:%M:%S %p")

        html_representation = f"""<label class="datetime_label">{formatted_date}</label><br><label class="datetime_label">{formatted_time}</label>"""

        return html_representation

    @staticmethod
    def _format_amount(amount_str):
        amount = float(amount_str)
        text_color = 'orange' if amount < 0 else 'green' if amount > 0 else 'black'
        return f"""<h4 style="color: {text_color}">{amount:.2f}</h4>"""

    @staticmethod
    def _format_currency_count(curr_string):
        value_counts = Counter(curr_string.split(','))
        if len(value_counts) == 1:
            curr, count = value_counts.popitem()
            if count > 1:
                return f"{curr} ({count})"
            return curr_string

        res_str = '<ul>'
        for curr, count in value_counts.items():
            res_str += f"<li>{curr}:{count}</li>"
        res_str += '</ul>'

        return res_str

    @staticmethod
    def _format_description(descr_str):
        descr_html = """<div>"""
        for phrase in descr_str.split(','):
            descr_html += f'<label class="description_phrase_label">{phrase}</label>'
        descr_html += "</div>"
        return descr_html

    @staticmethod
    def _format_tags(tag_str):
        tags = tag_str.split(',')
        tags_html = "<div>"
        for tag in tags:
            tags_html += f"""<label class="tag_label">{tag}</label>"""
        tags_html += "</div>"

        return tags_html
