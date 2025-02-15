from __future__ import annotations  # TODO:v2 upgrade to python 3.11

import abc
import logging
from collections import Counter
from datetime import datetime, date

import pandas as pd

import mecon.data.datafields as fields
from mecon.data.aggregators import CustomisableAmountTransactionAggregator
from mecon.data.groupings import LabelGrouping
from mecon.utils import dataframe_transformers

ID_FILL_VALUE = 'filled'


class Transactions(fields.DatedDataframeWrapper, fields.IdColumnMixin, fields.AmountColumnMixin,
                   fields.DescriptionColumnMixin, fields.TagsColumnMixin):
    """
    Responsible for holding the transactions dataframe and controlling the access to it, like a DataFrame Facade.
    Columns are specified and only accessed and modifies by the corresponding mixins. Key goal of this object
    is to solely represent sets of transactions and their app-level operations like the extraction of a subset (e.x
    filter out rows based on a field value) or a superset (e.x fill null dates to ensure non-empty groups)

    Not responsible for any IO operations.
    """

    # fields = ['datetime', 'amount', 'currency', 'amount_cur', 'description', 'tags']
    columns = ['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description', 'tags']

    def __init__(self, df: pd.DataFrame):
        super().__init__(df=df)
        fields.IdColumnMixin.__init__(self, df_wrapper=self)
        fields.AmountColumnMixin.__init__(self, df_wrapper=self)
        fields.DescriptionColumnMixin.__init__(self, df_wrapper=self)
        fields.TagsColumnMixin.__init__(self, df_wrapper=self)

    def invalid_transactions(self):
        invalid_ids = self.invalid_ids()
        invalid_dts = self.invalid_datetimes()
        invalid_amounts = self.invalid_amounts()
        invalid_currencies = self.invalid_currencies()
        invalid_amount_curs = self.invalid_amount_curs()
        invalid_amount_descrs = self.invalid_descriptions()
        invalid_amount_tags = self.invalid_tags()

        logging.warning(f"Spotted {invalid_ids.sum()} invalid ids")
        logging.warning(f"Spotted {invalid_dts.sum()} invalid datetimes")
        logging.warning(f"Spotted {invalid_amounts.sum()} invalid amounts")
        logging.warning(f"Spotted {invalid_currencies.sum()} invalid currencies")
        logging.warning(f"Spotted {invalid_amount_curs.sum()} invalid amount_cur values")
        logging.warning(f"Spotted {invalid_amount_descrs.sum()} invalid descriptions")
        logging.warning(f"Spotted {invalid_amount_tags.sum()} invalid tag values")

        invalid_rows = invalid_ids | invalid_dts | invalid_amounts | invalid_currencies | invalid_amount_curs | invalid_amount_descrs | invalid_amount_tags
        if invalid_rows.sum() == 0:
            return None

        invalid_rows_df = self.dataframe()[invalid_rows]
        invalid_trans = Transactions(invalid_rows_df)
        return invalid_trans

    def fill_values(self, fill_unit,
                    start_date: datetime | date | None = None,
                    end_date: datetime | date | None = None):
        return self.fill_dates(TransactionDateFiller(fill_unit=fill_unit),
                               start_date=start_date, end_date=end_date)

    @classmethod
    def factory(cls, df: pd.DataFrame):
        return super().factory(df)

    def to_html(self, df_transformer=None):
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
            background-color: #ffe;
            padding: 2px;
            z-index: 1;
            border: 1px solid #eee;
            box-shadow: 0px 8px 16px 0px rgba(0, 0, 0, 0.2);
        }
        tr {
            border: 1px solid #ccc;
            background-color: #eff;
        }
        td {
            text-align: center;
            border: 1px solid #ccc;
            padding: 4px;
            
        }
        th {
            font-size: 18px;
            text-align: left;
            border: 1px solid #ccc;
            padding: 10px;
        }
        
        </style>
        """
        df_transformer = df_transformer if df_transformer is not None else TransactionsDataTransformationToHTMLTable()
        df = self.dataframe(df_transformer=df_transformer)
        html_table = df.to_html(escape=False, index=False)
        res_html = f"{styles}<h3>Transactions table ({len(df)} rows)</h3>\n{html_table}"
        return res_html

    def dataframe(self, df_transformer: AbstractTransactionsTransformer = None):
        df = super().dataframe(df_transformer=df_transformer)
        return df

    def get_filtered_transactions(self, start_date, end_date, tags) -> Transactions:
        transactions = self.containing_tags(tags)

        transactions = transactions.select_date_range(start_date, end_date)

        return transactions

    def group_and_fill_transactions(self, grouping_key, aggregation_key,
                                    fill_dates_before_groupagg=False,
                                    fill_dates_after_groupagg=False) -> Transactions:

        transactions = self.copy()

        if grouping_key == 'none':
            return transactions

        if fill_dates_before_groupagg:
            transactions = transactions.fill_values(grouping_key)

        transactions = transactions.groupagg(
            LabelGrouping.from_key(grouping_key),
            CustomisableAmountTransactionAggregator(aggregation_key, grouping_key)
        )

        if fill_dates_after_groupagg:
            transactions = transactions.fill_values(grouping_key)

        return transactions

    def get_filtered_and_grouped_transactions(self, start_date, end_date, tags, grouping_key, aggregation_key,
                                              fill_dates_before_groupagg=False,
                                              fill_dates_after_groupagg=False) -> Transactions:
        transactions = self.containing_tags(tags)

        transactions = transactions.select_date_range(start_date, end_date)

        if grouping_key != 'none':
            if fill_dates_before_groupagg:
                transactions = transactions.fill_values(grouping_key)

            transactions = transactions.groupagg(
                LabelGrouping.from_key(grouping_key),
                CustomisableAmountTransactionAggregator(aggregation_key, grouping_key)
            )

            if fill_dates_after_groupagg:
                transactions = transactions.fill_values(grouping_key)

        return transactions

    def diff(self,
             transactions: Transactions,
             columns=None) -> Transactions:
        if columns is None:
            columns = ['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description', 'tags']

        df_this, df_other = self.dataframe(), transactions.dataframe()
        diffs = {}
        for column in columns:
            if column == 'tags':
                diff = [set(tags_this.split(',')) != set(tags_other.split(',')) for tags_this, tags_other in
                        zip(df_this[column], df_other[column])]
            else:
                diff = df_this[column] != df_other[column]
            diffs[column] = diff
        df_diffs = pd.DataFrame(diffs)
        the_other_minus_this = df_other[df_diffs.any(axis=1)]
        diff_trans = Transactions(the_other_minus_this)
        return diff_trans

    def tags_diff(self,
                  transactions: Transactions,
                  target_tags: list[str] | str | None = None,
                  # comparison: Literal['']
                  ) -> Transactions:
        # TODO decide if this is in transactions or fields.TagsColumn
        target_tags = [target_tags] if target_tags is not None and isinstance(target_tags, str) else target_tags
        df_this, df_other = self.dataframe(), transactions.dataframe()
        comparison_results = []
        for tags_this, tags_other in zip(df_this['tags'], df_other['tags']):
            tags_this_set, tags_other_set = set(tags_this.split(',')), set(tags_other.split(','))
            tags_this_focused = tags_this_set.intersection(target_tags) if target_tags is not None else tags_this_set
            tags_other_focused = tags_other_set.intersection(target_tags) if target_tags is not None else tags_other_set
            is_different = tags_this_focused != tags_other_focused#len(this_tags_focused.intersection(tags_other_set)) != len(this_tags_focused)
            comparison_results.append(is_different)

        the_other_minus_this = df_other[comparison_results]
        diff_trans = Transactions(the_other_minus_this)
        return diff_trans

    def equals(self,
               transactions: Transactions,
               columns=None) -> bool:
        diff = self.diff(transactions=transactions, columns=columns)
        res = diff.size() == 0
        return res

    @classmethod
    def from_csv(cls, path) -> Transactions:
        df = pd.read_csv(path, index_col=None)
        df['datetime'] = pd.to_datetime(df['datetime'])
        return cls(df)


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
    def validate_input_df(self, df_in: pd.DataFrame, **kwargs):
        missing_cols = []
        for col in Transactions.columns:
            if col not in df_in.columns:
                missing_cols.append(col)

        if len(missing_cols) > 0:
            raise ValueError(f"Missing required columns for Transaction Transformer: {missing_cols}")


# TODO remove this and any other html table code, unless I use it as shiny.ui.HTML elements
class TransactionsDataTransformationToHTMLTable(AbstractTransactionsTransformer):
    def _transform(self, df_in: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df_in = df_in.copy().iloc[::-1].reset_index(drop=True)

        df_out = pd.DataFrame(df_in['id'].apply(self._format_id))
        df_out['Date/Time'] = df_in['datetime'].apply(self._format_datetime)
        df_out['Amount (£)'] = df_in['amount'].apply(self._format_amount)
        # df_out['Curr'] = df_in['currency'].apply(self._format_currency_count)
        # df_out['Amount (curr)'] = df_in['amount_cur'].apply(self._format_amount)
        df_out['Local amount'] = df_in.apply(lambda row: self._format_local_amount(row['amount_cur'], row['currency']),
                                             axis=1, result_type='reduce')
        df_out['Description'] = df_in['description'].apply(self._format_description)
        df_out['Tags'] = df_in['tags'].apply(self._format_tags)

        return df_out

    @staticmethod
    def _format_id(id_str):
        def wrap(string, chunk_size):
            return [string[i:i + chunk_size] for i in range(0, len(string), chunk_size)]

        id_parts = wrap(id_str, 6)
        id_html = '<br>'.join([f"<small><small><strong>{part}</strong></small></small>" for part in id_parts])
        return id_html

    @staticmethod
    def _format_datetime(dt):
        formatted_date = dt.strftime("%b %d, %Y")
        formatted_time = dt.strftime("%A, %H:%M:%S")

        html_representation = f"""<label class="datetime_label">{formatted_time}</label><br><label class="datetime_label">{formatted_date}</label>"""

        return html_representation

    @staticmethod
    def _format_amount(amount_str):
        amount = float(amount_str)
        text_color = 'orange' if amount < 0 else 'green' if amount > 0 else 'black'
        return f"""<h6 style="color: {text_color}">{amount:.2f}</h6>"""

    @staticmethod
    def _format_local_amount(amount_str, currency):
        symbol_mapping = {
            'EUR': '€',
            'GBP': '£',
            'USD': '$'
        }
        if ',' in currency:
            split_currs = currency.split(',')
            currency_symbol = f"({','.join(symbol_mapping.get(curr, curr) for curr in split_currs)})"
        else:
            currency_symbol = symbol_mapping.get(currency, f"({currency})")

        amount = float(amount_str)
        text_color = 'orange' if amount < 0 else 'green' if amount > 0 else 'black'
        return f"""<h6 style="color: {text_color}">{amount:.2f}{currency_symbol}</h6>"""

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
