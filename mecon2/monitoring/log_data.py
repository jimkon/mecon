import re

import pandas as pd

from mecon2 import datafields


def _extract_description(string):
    match = re.search(r'#"(.*?)"#', string)
    return match.group(1)


def _extract_tags(string):
    if len(string) == 0 or '#' not in string:
        return ''

    tag_split = string.split('#')
    tags_fetched = tag_split[1:] if len(tag_split) > 1 else []
    tags_string = ','.join([tag.strip() for tag in tags_fetched if len(tag) > 0])
    return tags_string


def transform_raw_dataframe(df_logs_raw: pd.DataFrame) -> pd.DataFrame:
    dt_col = pd.to_datetime(df_logs_raw['datetime'] + ',' + df_logs_raw['msecs'].astype(str))
    df_transformed = pd.DataFrame({'datetime': dt_col})
    df_transformed['level'] = df_logs_raw['level']
    df_transformed['module'] = df_logs_raw['module']
    df_transformed['funcName'] = df_logs_raw['funcName']
    df_transformed['description'] = df_logs_raw['message'].apply(lambda text: text) #_extract_description(text))
    df_transformed['tags'] = df_transformed['description'].apply(lambda text: _extract_tags(text))
    return df_transformed


class LogData(datafields.DataframeWrapper,
              datafields.DateTimeColumnMixin,
              datafields.DescriptionColumnMixin,
              datafields.TagsColumnMixin):
    def __init__(self, df: pd.DataFrame):
        super().__init__(df=df)
        datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)
        datafields.DescriptionColumnMixin.__init__(self, df_wrapper=self)
        datafields.TagsColumnMixin.__init__(self, df_wrapper=self)
        datafields.TagsColumnMixin.__init__(self, df_wrapper=self)

    @classmethod
    def from_raw_logs(cls, df_logs_raw: pd.DataFrame):  # -> LogData: TODO upgrade to python 3.11
        df_transformed = transform_raw_dataframe(df_logs_raw)
        return LogData(df_transformed)
