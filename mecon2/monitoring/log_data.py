from collections import OrderedDict

import pandas as pd

import tagging
from mecon2 import datafields
from mecon2 import groupings


def _extract_tags(string):
    if len(string) == 0 or '#' not in string:
        return ''

    tag_split = string.split('#')
    tags_fetched = tag_split[1:] if len(tag_split) > 1 else []
    tags_string = ','.join([tag.strip() for tag in tags_fetched if len(tag) > 0])
    return tags_string


def transform_raw_dataframe(df_logs_raw: pd.DataFrame) -> pd.DataFrame:
    dt_col = pd.to_datetime(df_logs_raw['datetime'] + ',' + df_logs_raw['msecs'].astype(str),
                            format="%Y-%m-%d %H:%M:%S,%f")
    df_transformed = pd.DataFrame({'datetime': dt_col})
    df_transformed['level'] = df_logs_raw['level']
    df_transformed['module'] = df_logs_raw['module']
    df_transformed['funcName'] = df_logs_raw['funcName']
    df_transformed['description'] = df_logs_raw['message'].apply(lambda text: text)
    df_transformed['tags'] = df_transformed['description'].apply(lambda text: _extract_tags(text))
    return df_transformed


class LogInfoMixin:
    def __init__(self, df_wrapper: datafields.DataframeWrapper):
        self._df_wrapper_obj = df_wrapper

    @property
    def level(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['level']

    @property
    def module(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['module']

    @property
    def funcName(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['funcName']


class LogData(datafields.DataframeWrapper,
              datafields.DateTimeColumnMixin,
              LogInfoMixin,
              datafields.DescriptionColumnMixin,
              datafields.TagsColumnMixin):
    def __init__(self, df: pd.DataFrame):
        super().__init__(df=df)
        datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)
        LogInfoMixin.__init__(self, df_wrapper=self)
        datafields.DescriptionColumnMixin.__init__(self, df_wrapper=self)
        datafields.TagsColumnMixin.__init__(self, df_wrapper=self)

    @classmethod
    def from_raw_logs(cls, df_logs_raw: pd.DataFrame):  # -> LogData: TODO upgrade to python 3.11
        df_transformed = transform_raw_dataframe(df_logs_raw)
        return LogData(df_transformed)


def _isolate_function_tags(tags_column):  # TODO unittest
    tags_df = pd.DataFrame(tags_column)

    tagging.Tagger.remove_tag('codeflow', tags_df)
    tagging.Tagger.remove_tag('start', tags_df)
    tagging.Tagger.remove_tag('end', tags_df)

    functions = [tags.split(',')[0] for tags in tags_df['tags'].to_list()]
    return functions


def _distinct_function_tags(tags_column):  # TODO unittest
    """We assume that the first tag after codeflow, and start or end, is the function name tag"""
    function_tags = list(OrderedDict.fromkeys(_isolate_function_tags(tags_column)))
    return function_tags


class ExecutionInfoMixin:
    def __init__(self, df_wrapper: datafields.DataframeWrapper):
        self._df_wrapper_obj = df_wrapper

    @property
    def execution_time(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['execution_time']

    @property
    def start_datetime(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['datetime']

    @property
    def end_datetime(self) -> pd.Series:
        return self.start_datetime + pd.to_timedelta(self.execution_time, unit='ms')

    @property
    def is_finished(self) -> pd.Series:
        return self.execution_time >= 0

    def finished(self):  # TODO -> PerformanceData:
        return self._df_wrapper_obj.factory(self._df_wrapper_obj.dataframe()[self.is_finished])

    @property
    def functions(self):
        return pd.Series(_isolate_function_tags(self._df_wrapper_obj.dataframe()['tags']))

    def logged_functions(self):
        tags_list = _distinct_function_tags(self._df_wrapper_obj.dataframe()['tags'])
        return tags_list

    def group_by_function(self):  # TODO unittest
        tags_list = _distinct_function_tags(self._df_wrapper_obj.dataframe()['tags'])
        grouper = groupings.TagGrouping(tags_list=tags_list)
        groups = grouper.group(self._df_wrapper_obj)
        return groups

    def group_by_tags(self):  # TODO unittest
        tags_list = self._df_wrapper_obj.all_tags().keys()
        grouper = groupings.TagGrouping(tags_list=tags_list)
        groups = grouper.group(self._df_wrapper_obj)
        return groups


class PerformanceDataAggregator(datafields.AggregatorABC):
    def aggregation(self, df_wrapper: LogData):  # -> PerformanceData: TODO upgrade to python 3.11
        # will not work for recursive calls
        df_logs = df_wrapper.dataframe().copy()
        df_logs.sort_values('datetime', inplace=True)

        df_logs['datetime_ms'] = pd.to_datetime(df_logs['datetime'], unit='ms')
        df_logs['execution_time'] = -df_logs['datetime_ms'].diff(periods=-1).dt.total_seconds() * 1000
        df_logs['started'] = df_wrapper.contains_tag('start')
        df_logs['finished'] = df_wrapper.contains_tag('end').shift(periods=-1, fill_value=False)

        df_started_finished = df_logs[(df_logs['started']) & (df_logs['finished'])]
        df_started_not_finished = df_logs[(df_logs['started']) & (~df_logs['finished'])]
        df_started_not_finished['execution_time'] = -1

        perf_df = pd.concat([df_started_finished, df_started_not_finished])
        perf_df = perf_df[['datetime', 'execution_time', 'tags']]
        perf_df = perf_df.sort_values(by='datetime').reset_index(drop=True)

        for tag in ['codeflow', 'start', 'end']:
            tagging.Tagger.remove_tag(tag, perf_df)

        perf_df['execution_time'] = perf_df['execution_time'].astype('int64')

        perf_logs = PerformanceData(perf_df)
        return perf_logs


class PerformanceData(datafields.DataframeWrapper,
                      datafields.DateTimeColumnMixin,
                      ExecutionInfoMixin,
                      datafields.TagsColumnMixin):
    def __init__(self, df: pd.DataFrame):
        super().__init__(df=df)
        datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)
        ExecutionInfoMixin.__init__(self, df_wrapper=self)
        datafields.TagsColumnMixin.__init__(self, df_wrapper=self)

    @classmethod
    def from_log_data(cls, log_data: LogData):  # -> PerformanceData: TODO upgrade to python 3.11
        codeflow_logs = log_data.containing_tag('codeflow')
        # maybe add tags for level, module, funcName
        # tags_list = sorted(list(set(codeflow_logs.all_tags().keys()) - {'codeflow', 'start', 'end'}))
        tags_list = _distinct_function_tags(codeflow_logs.tags)

        grouper = groupings.TagGrouping(tags_list=tags_list)
        groups = grouper.group(codeflow_logs)

        aggregator = PerformanceDataAggregator()
        performance_df = aggregator.aggregate_result_df(groups).sort_values('datetime').reset_index(drop=True)

        return PerformanceData(performance_df)
