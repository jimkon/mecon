from __future__ import annotations  # TODO:v2 upgrade to python 3.11

import abc
import itertools
import json
import logging
from collections import Counter
from itertools import chain
from typing import List
from datetime import datetime, date

import pandas as pd

from mecon.tag_tools import tagging
from mecon.utils import calendar_utils
from mecon.monitoring import logs
from mecon.utils import dataframe_transformers


class NullDataframeInDataframeWrapper(Exception):
    pass


class DataframeWrapper:
    def __init__(self, df: pd.DataFrame):
        if df is None:  # or len(df) == 0:
            raise NullDataframeInDataframeWrapper
        self._df = df

    def dataframe(self, df_transformer: dataframe_transformers.DataframeTransformer = None) -> pd.DataFrame:
        if df_transformer:
            return df_transformer.transform(self._df.copy())
        return self._df

    def copy(self) -> DataframeWrapper:
        return self.factory(self.dataframe())

    def merge(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        df = pd.concat([self.dataframe(), df_wrapper.dataframe()]).drop_duplicates()
        return self.factory(df)

    def size(self):
        return len(self.dataframe())

    def select_by_index(self, index: List[bool] | pd.Series):
        return self.factory(self.dataframe().copy()[index])

    def select_by_index_range(self, index_start: int, index_end: int):
        index_start, index_end = index_start if index_start<self.size() else 0, min(index_end, self.size())
        index = pd.Series(list(range(0, self.size(), 1)))
        boolean_index = (index >= index_start) & (index <= index_end)
        return self.select_by_index(boolean_index)

    @logs.codeflow_log_wrapper('#data#transactions#tags')
    def apply_rule(self, rule: tagging.AbstractRule) -> DataframeWrapper | None:
        df = self.dataframe().copy()
        new_df = tagging.Tagger.filter_df_with_rule(df, rule)
        return self.factory(new_df) if len(new_df) > 0 else None

    @logs.codeflow_log_wrapper('#data#transactions#tags')
    def apply_negated_rule(self, rule: tagging.AbstractRule) -> DataframeWrapper:
        df = self.dataframe().copy()
        new_df = tagging.Tagger.filter_df_with_negated_rule(df, rule)
        return self.factory(new_df)

    @logs.codeflow_log_wrapper('#data#transactions#groupagg')
    def groupagg(self, grouper: Grouping, aggregator: InTypeAggregator) -> DataframeWrapper:
        groups = grouper.group(self)
        aggregated_groups = aggregator.aggregate(groups)
        return aggregated_groups

    @classmethod
    def factory(cls, df: pd.DataFrame):
        return cls(df)


class MissingRequiredColumnInDataframeWrapperError(Exception):
    pass


class ColumnMixin:
    _required_column = None

    def __init__(self, df_wrapper: DataframeWrapper):
        self._df_wrapper_obj = df_wrapper
        # self._validate_column() TODO tests have to be adapted

    def _validate_column(self):
        if self._required_column is not None:
            _req_set = set(self._required_column) if isinstance(self._required_column, list) else {
                self._required_column}
            if _req_set.issubset(self._df_wrapper_obj.dataframe().columns):
                raise MissingRequiredColumnInDataframeWrapperError(
                    f"Column '{self._required_column}' is required from {self.__class__.__name__}")

    @property
    def dataframe_wrapper_obj(self):
        return self._df_wrapper_obj


class IdColumnMixin(ColumnMixin):
    _required_column = 'id'

    @property
    def id(self):
        return self._df_wrapper_obj.dataframe()['id']


class DateTimeColumnMixin(ColumnMixin):
    _required_column = 'datetime'

    @property
    def datetime(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['datetime']

    @property
    def date(self) -> pd.Series:
        return self.datetime.dt.date

    @property
    def time(self) -> pd.Series:
        return self.datetime.dt.time

    def date_range(self):
        return self.date.min(), self.date.max()

    def select_date_range(self, start_date: [str | datetime | date], end_date: [str | datetime | date]) -> DatedDataframeWrapper | None:
        start_date, end_date = calendar_utils.to_date(start_date), calendar_utils.to_date(end_date)

        rule = tagging.Conjunction([
            tagging.Condition.from_string_values('datetime', 'date', 'greater_equal', start_date),
            tagging.Condition.from_string_values('datetime', 'date', 'less_equal', end_date),
        ])
        return self._df_wrapper_obj.apply_rule(rule)


class AmountColumnMixin(ColumnMixin):
    _required_column = ['amount', 'currency', 'amount_cur']

    @property
    def amount(self):
        return self._df_wrapper_obj.dataframe()['amount']

    @property
    def currency(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['currency']

    @property
    def currency_list(self) -> pd.Series:
        return self.currency.apply(lambda s: s.split(','))

    @property
    def amount_cur(self):
        return self._df_wrapper_obj.dataframe()['amount_cur']

    def all_currencies(self):
        flattened = list(itertools.chain(*self.currency_list))
        return json.dumps(dict(sorted(Counter(flattened).items(), reverse=True)))


class DescriptionColumnMixin(ColumnMixin):
    _required_column = 'description'

    def description(self):
        return self._df_wrapper_obj.dataframe()['description']


class TagsColumnDoesNotExistInDataframe(Exception):
    pass


class TagsColumnMixin(ColumnMixin):
    _required_column = 'tags'

    @property
    def tags(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['tags']

    def all_tags(self):
        tags_split = self.tags.apply(
            lambda s: s.split(',')).to_list()
        tags_list = [tag for tag in chain.from_iterable(tags_split) if len(tag) > 0]
        tags_dict = dict(sorted(Counter(tags_list).items(), key=lambda item: item[1], reverse=True))
        return tags_dict

    def contains_tag(self, tags):
        tags = [tags] if isinstance(tags, str) else tags
        tag_rules = [tagging.TagMatchCondition(tag) for tag in tags]
        rule = tagging.Conjunction(tag_rules)
        index_col = tagging.Tagger.get_index_for_rule(self._df_wrapper_obj.dataframe(), rule)

        return index_col

    def containing_tag(self, tags):
        tags = [tags] if isinstance(tags, str) else tags
        tag_rules = [tagging.TagMatchCondition(tag) for tag in tags]
        rule = tagging.Conjunction(tag_rules)
        return self._df_wrapper_obj.apply_rule(rule)

    def reset_tags(self):
        new_df = self._df_wrapper_obj.dataframe().copy()
        new_df['tags'] = ''
        return self._df_wrapper_obj.factory(new_df)

    def apply_tag(self, tag: tagging.Tag) -> DataframeWrapper:
        logging.info(f"Applying {tag.name} tag to transaction.")
        new_df = self._df_wrapper_obj.dataframe().copy()
        tagging.Tagger.tag(tag, new_df)
        return self._df_wrapper_obj.factory(new_df)


class Grouping(abc.ABC):
    @logs.codeflow_log_wrapper('#data#transactions#process')
    def group(self, df_wrapper: DataframeWrapper) -> List[DataframeWrapper]:
        indexes = self.compute_group_indexes(df_wrapper)

        df_wrapper_list = []
        for index_group in indexes:
            sub_df_wrapper = df_wrapper.select_by_index(index_group)
            df_wrapper_list.append(sub_df_wrapper)

        return df_wrapper_list

    @abc.abstractmethod
    def compute_group_indexes(self, df_wrapper: DataframeWrapper) -> List[pd.Series]:
        pass


class AggregatorABC(abc.ABC):  # TODO:v3 tested only through InTypeAggregator
    def aggregate_result_df(self, lists_of_df_wrapper: List[DataframeWrapper]) -> pd.DataFrame:
        aggregated_df_wrappers_list = [self.aggregation(df_wrapper) for df_wrapper in lists_of_df_wrapper]
        df_agg = pd.concat([df_wrapper.dataframe() for df_wrapper in aggregated_df_wrappers_list])
        return df_agg

    @abc.abstractmethod
    def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        pass


class InTypeAggregator(AggregatorABC):
    def __init__(self, aggregation_functions):
        self._agg_functions = aggregation_functions

    @logs.codeflow_log_wrapper('#data#transactions#process')
    def aggregate(self, lists_of_df_wrapper: List[DataframeWrapper]) -> DataframeWrapper:
        df_agg = self.aggregate_result_df(lists_of_df_wrapper)
        res_df_wrapper = lists_of_df_wrapper[0].factory(df_agg)
        return res_df_wrapper

    def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        res_dict = {}
        for col_name, agg_func in self._agg_functions.items():
            res_dict[col_name] = [agg_func(df_wrapper.dataframe()[col_name])]

        new_df = pd.DataFrame(res_dict)
        new_df_wrapper = df_wrapper.factory(new_df)
        return new_df_wrapper


class UnorderedDatedDataframeWrapper(Exception):
    pass


class DatedDataframeWrapper(DataframeWrapper, DateTimeColumnMixin):
    def __init__(self, df: pd.DataFrame):
        super().__init__(df=df)
        DateTimeColumnMixin.__init__(self, df_wrapper=self)
        self._validate_datetime_order()

    def _validate_datetime_order(self):
        if not self.datetime.is_monotonic_increasing:
            raise UnorderedDatedDataframeWrapper

    def merge(self, df_wrapper: DatedDataframeWrapper) -> DatedDataframeWrapper:
        df = pd.concat([self.dataframe(), df_wrapper.dataframe()]).drop_duplicates()
        df.sort_values(by='datetime', inplace=True)
        return self.factory(df)

    def fill_dates(self,
                   filler: DateFiller = None,
                   start_date: datetime | date | None = None,
                   end_date: datetime | date | None = None):
        return filler.fill(self, start_date, end_date)


class DateFiller:
    def __init__(self, fill_unit, fill_values_dict):
        self._fill_unit = fill_unit
        self._fill_values = fill_values_dict

    @logs.codeflow_log_wrapper('#data#transactions#process')
    def fill(self, df_wrapper: DatedDataframeWrapper,
             start_date: datetime | date | None = None,
             end_date: datetime | date | None = None
             ) -> DatedDataframeWrapper:
        df_start_date, df_end_date = df_wrapper.date_range()
        start_date = df_start_date if start_date is None else start_date
        end_date = df_end_date if end_date is None else end_date

        fill_df = self.produce_fill_df_rows(start_date, end_date, df_wrapper.date)
        fill_df_wrapper = df_wrapper.factory(fill_df)
        filtered_df_wrapper = df_wrapper.select_date_range(start_date, end_date)
        merged_df_wrapper = filtered_df_wrapper.merge(fill_df_wrapper)
        return merged_df_wrapper

    def produce_fill_df_rows(self, start_date: datetime | date, end_date: datetime | date, remove_dates=None):
        date_range = calendar_utils.date_range_group_beginning(start_date, end_date, step=self._fill_unit)
        date_range.name = 'datetime'

        if remove_dates is not None:
            remove_dates = pd.to_datetime(remove_dates).to_list()
            date_range = [date for date in date_range if date not in remove_dates]

        fill_df = pd.DataFrame({'datetime': date_range})
        for column, default_value in self._fill_values.items():
            fill_df[column] = default_value

        fill_df.sort_values(by=['datetime'], inplace=True)
        return fill_df
