import abc
from typing import List

import pandas as pd

from mecon2.datafields import DataframeWrapper, Grouping
from mecon2.utils import calendar_utils
from mecon2.utils.multiton import Multiton


class LabelGroupingABC(Grouping, Multiton, abc.ABC):
    def __init__(self, instance_name):
        super().__init__(instance_name=instance_name)

    def compute_group_indexes(self, df_wrapper: DataframeWrapper) -> List[pd.Series]:
        labels = self.labels(df_wrapper)
        unique_labels = labels.unique()

        indexes = []
        for label in unique_labels:
            index = labels == label
            indexes.append(index)

        return indexes

    @abc.abstractmethod
    def labels(self, df_wrapper: DataframeWrapper) -> pd.Series:
        pass


class LabelGrouping(LabelGroupingABC, abc.ABC):
    def __init__(self, name, label_function):
        super().__init__(instance_name=name)
        self._label_function = label_function

    def labels(self, df_wrapper: DataframeWrapper) -> pd.Series:
        res = self._label_function(df_wrapper)
        return res


# class DayGrouping(LabelGroupingABC):
#     def __init__(self):
#         super().__init__(instance_name='day')
#
#     def labels(self, df_wrapper: DataframeWrapper) -> pd.Series:
#         assert hasattr(df_wrapper, 'date')
#         # if not isinstance(df_wrapper, DateTimeColumnMixin):  # TODO validation doesn't work properly
#         #     raise ValueError(f"To group in 'days' the input df_wrapper has to implement DateTimeColumnMixin.")
#         date = df_wrapper.date.astype(str)
#         return date
# _DAY = DayGrouping()  # TODO investigate why _DAY and DAY multitons don't have a conflict

DAY = LabelGrouping('day', lambda df_wrapper: df_wrapper.datetime.apply(calendar_utils.datetime_to_date_id_str))
WEEK = LabelGrouping('week', lambda df_wrapper: df_wrapper.datetime.apply(calendar_utils.get_closest_past_monday).dt.date.astype(str))
MONTH = LabelGrouping('month', lambda df_wrapper: df_wrapper.datetime.apply(lambda dt: calendar_utils.datetime_to_date_id_str(dt)[:6]))
YEAR = LabelGrouping('year', lambda df_wrapper: df_wrapper.datetime.apply(lambda dt: str(dt.year)))
