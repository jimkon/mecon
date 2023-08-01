from __future__ import annotations  # TODO upgrade to python 3.11

import abc
from itertools import chain

import pandas as pd

from mecon2 import tagging


class DataframeWrapper:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def dataframe(self) -> pd.DataFrame:
        return self._df

    def copy(self) -> DataframeWrapper:
        return DataframeWrapper(self.dataframe())

    def merge(self, df_wrapper) -> DataframeWrapper:
        df = pd.concat([self.dataframe(), df_wrapper.dataframe()]).drop_duplicates()
        return DataframeWrapper(df)

    def size(self):
        return len(self.dataframe())

    @classmethod
    def dataframe_wrapper_type(cls):
        return cls


class IdColumnMixin:
    def __init__(self, df_wrapper: DataframeWrapper):
        pass

    def id(self):
        pass


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
        if 'tags' not in df.columns:
            raise TagsColumnDoesNotExistInDataframe

    @property
    def tags(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['tags']

    def all_tags(self):
        tags = self.tags.apply(lambda s: s.split(',')).to_list()
        tags_set = set(chain.from_iterable(tags))
        if '' in tags_set:
            tags_set.remove('')
        return tags_set

    def contains_tag(self, tag_name):
        rule = tagging.Condition.from_string_values('tags', None, 'contains', tag_name)
        tag_contained = self._df_wrapper_obj.dataframe().apply(rule.compute, axis=1)
        new_df = self._df_wrapper_obj.dataframe()[tag_contained]
        return self._df_wrapper_obj.dataframe_wrapper_type()(new_df)





