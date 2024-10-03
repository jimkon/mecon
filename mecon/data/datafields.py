from __future__ import annotations  # TODO:v2 upgrade to python 3.11

import abc
import itertools
import json
import logging
from collections import Counter
from datetime import datetime, date
from itertools import chain
from typing import List

import pandas as pd

from mecon.monitoring import logging_utils
from mecon.tags import tagging
from mecon.utils import calendar_utils
from mecon.utils import dataframe_transformers


class NullDataframeInDataframeWrapper(Exception):
    pass


class InvalidInputDataFrameColumns(Exception):
    pass


class DataframeWrapper:
    def __init__(self, df: pd.DataFrame):
        self._check_input_df(df)
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
        index_start, index_end = index_start if index_start < self.size() else 0, min(index_end, self.size())
        index = pd.Series(list(range(0, self.size(), 1)))
        boolean_index = (index >= index_start) & (index <= index_end)
        return self.select_by_index(boolean_index)

    @logging_utils.codeflow_log_wrapper('#data#transactions#tags')
    def apply_rule(self, rule: tagging.AbstractRule) -> DataframeWrapper | None:
        df = self.dataframe().copy()
        new_df = tagging.Tagger.filter_df_with_rule(df, rule)
        return self.factory(new_df)

    @logging_utils.codeflow_log_wrapper('#data#transactions#tags')
    def apply_negated_rule(self, rule: tagging.AbstractRule) -> DataframeWrapper:
        df = self.dataframe().copy()
        new_df = tagging.Tagger.filter_df_with_negated_rule(df, rule)
        return self.factory(new_df)

    @logging_utils.codeflow_log_wrapper('#data#transactions#groupagg')
    def groupagg(self, grouper: Grouping, aggregator: InTypeAggregator) -> DataframeWrapper:
        if self.size() == 0:
            return self.factory(self.dataframe())

        groups = grouper.group(self)
        aggregated_groups = aggregator.aggregate(groups)
        return aggregated_groups

    def group(self, grouper: Grouping) -> list[DataframeWrapper]:
        if self.size() == 0:
            return []

        groups = grouper.group(self)
        return groups

    @classmethod
    def factory(cls, df: pd.DataFrame):
        return cls(df)

    @staticmethod
    def _check_input_df(df: pd.DataFrame):
        if df is None:  # or len(df) == 0:
            raise NullDataframeInDataframeWrapper

        if len(df.columns) == 0:
            raise InvalidInputDataFrameColumns(f"No columns found in the input Dataframe.")


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
        if self._df_wrapper_obj.size() == 0:
            return None, None
        return self.date.min(), self.date.max()

    def select_date_range(self,
                          start_date: [str | datetime | date, None],
                          end_date: [str | datetime | date, None]
                          ) -> DatedDataframeWrapper:  # TODO fix type hinting issues
        if start_date is None or end_date is None:
            self_start_date, self_end_date = self.date_range()
            if self_start_date is None and self_end_date is None:
                return self._df_wrapper_obj.factory(self._df_wrapper_obj.dataframe())

        start_date = calendar_utils.to_date(start_date) if start_date is not None else self_start_date
        end_date = calendar_utils.to_date(end_date) if end_date is not None else self_end_date

        rule = tagging.Conjunction([
            tagging.Condition.from_string_values('datetime', 'date', 'greater_equal', start_date),
            tagging.Condition.from_string_values('datetime', 'date', 'less_equal', end_date),
        ])
        new_df_wrapper = self._df_wrapper_obj.apply_rule(
            rule)  # TODO if self._df_wrapper is empty, self._df_wrapper_obj.apply_rule returned object has no columns
        return new_df_wrapper


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

    def positive_amounts(self, include_zero=True):
        rule = tagging.Conjunction([
            tagging.Condition.from_string_values(field='amount',
                                                 transformation_op_key=None,
                                                 compare_op_key='greater_equal' if include_zero else 'greater',
                                                 value=.0),
        ])
        new_df_wrapper = self._df_wrapper_obj.apply_rule(
            rule)
        return new_df_wrapper

    def negative_amounts(self, include_zero=False):
        rule = tagging.Conjunction([
            tagging.Condition.from_string_values(field='amount',
                                                 transformation_op_key=None,
                                                 compare_op_key='less_equal' if include_zero else 'less',
                                                 value=.0),
        ])
        new_df_wrapper = self._df_wrapper_obj.apply_rule(
            rule)
        return new_df_wrapper


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
        """ Returns the 'tags' column of the dataframe wrapper. """
        return self._df_wrapper_obj.dataframe()['tags']

    def all_tags(self) -> dict:
        """ Returns all the unique tags in the dataframe wrapper along with their counts. """
        tags_split = self.tags.apply(
            lambda s: s.split(',')).to_list()
        tags_list = [tag for tag in chain.from_iterable(tags_split) if len(tag) > 0]
        tags_dict = dict(sorted(Counter(tags_list).items(), key=lambda item: item[1], reverse=True))
        return tags_dict

    def contains_tag(self, tags: str | list | None) -> pd.Series:
        """
        Returns a boolean pd.Series with True for each row where tags present.
        if not tags presented, it return True for all the rows
        """
        if tags is None or len(tags) == 0:
            return pd.Series(self._df_wrapper_obj.size() * [True])

        tags = [tags] if isinstance(tags, str) else tags
        tag_rules = [tagging.TagMatchCondition(tag) for tag in tags]
        rule = tagging.Conjunction(tag_rules)
        index_col = tagging.Tagger.get_index_for_rule(self._df_wrapper_obj.dataframe(), rule)
        return index_col

    def not_contains_tags(self, tags: str | list | None) -> pd.Series:
        """
        The opposite of contains_tag, it returns a boolean pd.Series with True for each row where tags present.
        if not tags presented, it return False for all the rows
        """
        contains_tags_flags = self.contains_tag(tags)
        not_contains_tags_flags = ~contains_tags_flags
        return not_contains_tags_flags

    def containing_tag(self, tags: str | list | None) -> DataframeWrapper:
        """
        Returns a copy of the df_wrapper with all the rows where tags are present.
        """
        # todo maybe use contains_tags, similar to not_containing_tag
        # if tags is None or len(tags) == 0:
        #     return self._df_wrapper_obj.copy()
        #
        # tags = [tags] if isinstance(tags, str) else tags
        # tag_rules = [tagging.TagMatchCondition(tag) for tag in tags]
        # rule = tagging.Conjunction(tag_rules)
        # return self._df_wrapper_obj.apply_rule(rule)
        contains_tags_flags = self.contains_tag(tags)
        df = self.dataframe_wrapper_obj.dataframe()[contains_tags_flags]
        return self._df_wrapper_obj.factory(df)

    def not_containing_tag(self, tags: str | list | None) -> DataframeWrapper:
        """
        The opposite of containing_tag, it returns a copy of the df_wrapper with all the rows where tags are NOT present.
        """

        not_contains_tags_flags = self.not_contains_tags(tags)
        df = self.dataframe_wrapper_obj.dataframe()[not_contains_tags_flags]
        return self._df_wrapper_obj.factory(df)

    def reset_tags(self):
        """
        Resets the 'tags' column of the dataframe wrapper, and therefore all the tags.
        """
        new_df = self._df_wrapper_obj.dataframe().copy()
        new_df['tags'] = ''
        return self._df_wrapper_obj.factory(new_df)

    def apply_tag(self, tag: tagging.Tag) -> DataframeWrapper:
        """
        Calculates and sets the tag to the df_wrapper and returns it.
        """
        logging.info(f"Applying {tag.name} tag to transaction.")
        new_df = self._df_wrapper_obj.dataframe().copy()
        tagging.Tagger.tag(tag, new_df)
        return self._df_wrapper_obj.factory(new_df)


class Grouping(abc.ABC):
    @logging_utils.codeflow_log_wrapper('#data#transactions#process')
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


class InvalidInputToAggregator(Exception):
    pass


class AggregatorABC(abc.ABC):  # TODO:v3 tested only through InTypeAggregator
    def aggregate_result_df(self, lists_of_df_wrapper: List[DataframeWrapper]) -> pd.DataFrame:
        if len(lists_of_df_wrapper) == 0:
            raise InvalidInputToAggregator(f"No DataframeWrapper object to aggregate.")

        aggregated_df_wrappers_list = [self.aggregation(df_wrapper) for df_wrapper in lists_of_df_wrapper]
        df_agg = pd.concat(
            [df_wrapper.dataframe() for df_wrapper in aggregated_df_wrappers_list if df_wrapper.size() > 0])
        return df_agg

    @abc.abstractmethod
    def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        pass


class InTypeAggregator(AggregatorABC):
    def __init__(self, aggregation_functions):
        self._agg_functions = aggregation_functions

    @logging_utils.codeflow_log_wrapper('#data#transactions#process')
    def aggregate(self, lists_of_df_wrapper: List[DataframeWrapper]) -> DataframeWrapper:
        # TODO what if lists_of_df_wrapper = []
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

    def merge(self, df_wrapper: DatedDataframeWrapper) -> DatedDataframeWrapper:  # TODO add to DataframeWrapper too
        not_empty_dfs = [df for df in [self.dataframe(), df_wrapper.dataframe()] if
                         len(df) > 0]  # silencing FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated
        df = pd.concat(not_empty_dfs).drop_duplicates()
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

    @logging_utils.codeflow_log_wrapper('#data#transactions#process')
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
