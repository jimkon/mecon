from __future__ import annotations  # TODO upgrade to python 3.11

import abc
from collections import Counter
from itertools import chain
from typing import List

import pandas as pd

from mecon2 import tagging
from mecon2.utils import calendar_utils


class DataframeWrapper:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def dataframe(self) -> pd.DataFrame:
        return self._df

    def copy(self) -> DataframeWrapper:
        return self.factory(self.dataframe())

    def merge(self, df_wrapper) -> DataframeWrapper:  # TODO sort before merging, and test
        df = pd.concat([self.dataframe(), df_wrapper.dataframe()]).drop_duplicates()
        if 'datetime' in df.columns:  # TODO datetime is not a always present
            df = df.sort_values(by='datetime')
        return self.factory(df)

    def size(self):
        return len(self.dataframe())

    def select_by_index(self, index: List[bool] | pd.Series):
        return self.factory(self.dataframe()[index])

    def apply_rule(self, rule: tagging.AbstractRule) -> DataframeWrapper:  # TODO unittest
        df = self.dataframe().copy()
        new_df = tagging.Tagger.filter_df_with_rule(df, rule)
        return self.factory(new_df)

    def apply_negated_rule(self, rule: tagging.AbstractRule) -> DataframeWrapper:  # TODO unittest
        df = self.dataframe().copy()
        new_df = tagging.Tagger.filter_df_with_negated_rule(df, rule)
        return self.factory(new_df)

    def groupagg(self, grouper: Grouping, aggregator: Aggregator) -> DataframeWrapper:
        groups = grouper.group(self)
        aggregated_groups = aggregator.aggregate(groups)
        return aggregated_groups

    @classmethod
    def factory(cls, df: pd.DataFrame):
        return cls(df)


class IdColumnMixin:
    def __init__(self, df_wrapper: DataframeWrapper):
        self._df_wrapper_obj = df_wrapper

    @property
    def id(self):
        return self._df_wrapper_obj.dataframe()['id']


class DateTimeColumnMixin:
    def __init__(self, df_wrapper: DataframeWrapper):
        self._df_wrapper_obj = df_wrapper

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

    def select_date_range(self, start_date: [str | datetime], end_date: [str | datetime]) -> DataframeWrapper:
        rule = tagging.Conjunction([
            tagging.Condition.from_string_values('datetime', 'str', 'greater_equal', str(start_date)),
            tagging.Condition.from_string_values('datetime', 'str', 'less_equal', str(end_date)),
        ])
        return self._df_wrapper_obj.apply_rule(rule)


class AmountColumnMixin:
    def __init__(self, df_wrapper: DataframeWrapper):
        self._df_wrapper_obj = df_wrapper

    @property
    def amount(self):
        return self._df_wrapper_obj.dataframe()['amount']

    @property
    def currency(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['currency']

    @property
    def amount_cur(self):
        return self._df_wrapper_obj.dataframe()['amount_cur']

    def all_currencies(self):
        return self.currency.unique().tolist()


class DescriptionColumnMixin:
    def __init__(self, df_wrapper: DataframeWrapper):
        pass

    def description(self):
        pass


class TagsColumnDoesNotExistInDataframe(Exception):
    pass


class TagsColumnMixin:
    def __init__(self, df_wrapper: DataframeWrapper):
        self._df_wrapper_obj = df_wrapper
        df = self._df_wrapper_obj.dataframe()
        # if 'tags' not in df.columns:  # TODO only tags col is validated
        #     raise TagsColumnDoesNotExistInDataframe

    @property
    def tags(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['tags']

    def all_tags(self):
        tags_split = self.tags.apply(
            lambda s: s.split(',')).to_list()  # TODO duplicated code in aggregators aggregate_tags_set
        tags_list = [tag for tag in chain.from_iterable(tags_split) if len(tag) > 0]
        tags_set = dict(sorted(Counter(tags_list).items(), key=lambda item: item[1], reverse=True))
        return tags_set

    def contains_tag(self, tags):
        tags = [tags] if isinstance(tags, str) else tags
        tag_rules = [tagging.Condition.from_string_values('tags', None, 'contains', tag) for tag in tags]
        rule = tagging.Conjunction(tag_rules)
        return self._df_wrapper_obj.apply_rule(rule)

    def reset_tags(self):
        new_df = self._df_wrapper_obj.dataframe().copy()
        new_df['tags'] = ''
        return self._df_wrapper_obj.factory(new_df)

    def apply_tag(self, tag: tagging.Tag) -> DataframeWrapper:
        new_df = self._df_wrapper_obj.dataframe().copy()
        tagging.Tagger.tag(tag, new_df)
        return self._df_wrapper_obj.factory(new_df)


class Grouping(abc.ABC):
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


class Aggregator:
    def __init__(self, aggregation_functions):
        self._agg_functions = aggregation_functions

    def aggregate(self, lists_of_df_wrapper: List[DataframeWrapper]) -> DataframeWrapper:
        aggregated_df_wrappers_list = [self.aggregation(df_wrapper) for df_wrapper in lists_of_df_wrapper]

        # shorter but not uses df_wrapper.merge
        new_df = pd.concat([df_wrapper.dataframe() for df_wrapper in aggregated_df_wrappers_list])
        res_df_wrapper = aggregated_df_wrappers_list[0].factory(new_df)

        # longer but not uses df_wrapper.merge
        # res_df_wrapper = aggregated_df_wrappers_list[0]
        # if len(aggregated_df_wrappers_list) > 1:
        #     for df_wrapper in aggregated_df_wrappers_list[1:]:
        #         res_df_wrapper = res_df_wrapper.merge(df_wrapper)

        return res_df_wrapper

    def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        res_dict = {}
        for col_name, agg_func in self._agg_functions.items():
            res_dict[col_name] = [agg_func(df_wrapper.dataframe()[col_name])]

        new_df = pd.DataFrame(res_dict)
        new_df_wrapper = df_wrapper.factory(new_df)
        return new_df_wrapper


class DateFiller:
    def __init__(self, fill_unit, fill_values_dict):
        self._fill_unit = fill_unit
        self._fill_values = fill_values_dict

    def fill(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        start_date, end_date = df_wrapper.date_range()  # TODO if has datetime column
        fill_df = self.produce_fill_df_rows(start_date, end_date, df_wrapper.date)
        fill_df_wrapper = df_wrapper.factory(fill_df)
        merged_df_wrapper = df_wrapper.merge(fill_df_wrapper)
        return merged_df_wrapper

    def produce_fill_df_rows(self, start_date, end_date, remove_dates=None):
        date_range = calendar_utils.date_range_group_beginning(start_date, end_date, step=self._fill_unit)
        date_range.name = 'datetime'

        if remove_dates is not None:
            remove_dates = pd.to_datetime(remove_dates).to_list()
            date_range = [date for date in date_range if date not in remove_dates]

        fill_df = pd.DataFrame({'datetime': date_range})
        for column, default_value in self._fill_values.items():
            fill_df[column] = default_value

        return fill_df
