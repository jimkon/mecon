import os.path
from ast import literal_eval
from functools import lru_cache, cached_property

import pandas as pd

from mecon.statements.combined_statement import Statement
from mecon.tagging.tags import ALL_TAGS
from mecon import utils
from mecon import logs


class TaggedData:
    def __init__(self, df):
        self._df = df
        if 'tags' not in self._df.columns:
            self._df['tags'] = [[] for _ in range(len(self._df))]

    def apply_taggers(self, taggers):
        df = self.dataframe()
        for tagger in taggers:
            tagger.tag(df)
        return TaggedData(df)

    def fill_dates(self):
        df_res = self.dataframe()
        df_res = utils.fill_dates(df_res)
        df_res = df_res.sort_values(by=['date', 'time']).reset_index(drop=True)
        return TaggedData(df_res)

    def dataframe(self):
        return self._df.copy()

    @cached_property
    def all_different_tags(self):
        result = []
        for row_tags in self._df['tags']:
            result.extend(row_tags)
        return sorted(set(result))

    def get_rows_tagged_as(self, tag):
        df = self.dataframe()

        select_rows_condition = df['tags'].apply(lambda x: (tag in x) if tag else True)
        res_df = df[select_rows_condition]

        if select_rows_condition.sum() == 0:
            print(f'{" WARNING ":#^100}\n\tTag "{tag}" returned no rows.')

        return TaggedData(res_df)

    @staticmethod
    @lru_cache
    def fully_tagged_data(recalculate_tags=False):
        cached_file_path = r"C:\Users\jim\PycharmProjects\mecon\statements\fully_tagged_statement.csv"
        if os.path.exists(cached_file_path):
            logs.log_disk_io("Loading tagged data...")
            df_statement = pd.read_csv(cached_file_path, index_col=None)
            df_statement['date'] = pd.to_datetime(df_statement['date'])
            df_statement['tags'] = df_statement['tags'].apply(lambda x: literal_eval(x))
            tagged_data = TaggedData(df_statement)
            if recalculate_tags:
                tagged_data.apply_taggers(ALL_TAGS)
        else:
            logs.log_calculation("Producing tagged data...")
            df_statement = Statement().dataframe()
            tagged_data = TaggedData(df_statement).apply_taggers(ALL_TAGS)
            logs.log_disk_io("Saving tagged data...")
            df_statement.to_csv(cached_file_path, index=None)

        return tagged_data


if __name__ == "__main__":
    data = TaggedData.fully_tagged_data()
    print(data.dataframe().head())
    print(data.all_different_tags)

