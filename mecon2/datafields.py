from __future__ import annotations  # TODO upgrade to python 3.11

import abc
from itertools import chain
from typing import List

import pandas as pd

from mecon2 import tagging


class DataframeWrapper:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def dataframe(self) -> pd.DataFrame:
        return self._df

    def copy(self) -> DataframeWrapper:
        return self.factory(self.dataframe())

    def merge(self, df_wrapper) -> DataframeWrapper:
        df = pd.concat([self.dataframe(), df_wrapper.dataframe()]).drop_duplicates()
        return self.factory(df)

    def size(self):
        return len(self.dataframe())

    @classmethod
    def factory(cls, df: pd.DataFrame):
        return cls(df)

    def select_by_index(self, index: List[bool] | pd.Series):
        return self.factory(self.dataframe()[index])

    def groupagg(self, grouper, aggregator):
        # TODO move this to the df wrapper
        pass


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

    def date_range(self):
        return self.date.min(), self.date.max()


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

    def contains_tag(self, tags):
        tags = [tags] if isinstance(tags, str) else tags
        tag_rules = [tagging.Condition.from_string_values('tags', None, 'contains', tag) for tag in tags]
        rule = tagging.Conjunction(tag_rules)
        tag_contained = self._df_wrapper_obj.dataframe().apply(rule.compute, axis=1)
        new_df = self._df_wrapper_obj.dataframe()[tag_contained]
        return self._df_wrapper_obj.factory(new_df)

    def reset_tags(self):
        new_df = self._df_wrapper_obj.dataframe().copy()
        new_df['tags'] = ''
        return self._df_wrapper_obj.factory(new_df)

    def apply_tag(self, tag: tagging.Tag) -> DataframeWrapper:
        tagger = tagging.Tagger()
        new_df = self._df_wrapper_obj.dataframe().copy()
        tagger.tag(tag, new_df)
        return self._df_wrapper_obj.factory(new_df)


class Grouper(abc.ABC):  # TODO rename to grouping(??)
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


class Aggregator(abc.ABC):
    def aggregate(self, lists_of_df_wrapper: List[DataframeWrapper]) -> DataframeWrapper:
        df_wrappers_list = [self.aggregation(df_wrapper) for df_wrapper in lists_of_df_wrapper]

        res_df_wrapper = df_wrappers_list[0]

        if len(df_wrappers_list) > 1:
            for df_wrapper in lists_of_df_wrapper[1:]:
                res_df_wrapper = res_df_wrapper.merge(df_wrapper)

        return res_df_wrapper

    @abc.abstractmethod
    def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        pass



