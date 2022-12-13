import os.path
from functools import lru_cache

import pandas as pd

from mecon.statements.combined_statement import Statement
from mecon.tagging.tags import *
from mecon import utils


class TaggedData:
    def __init__(self, df, taggers):
        self._df = df

        self.taggers = taggers if taggers else []
        self._init_tags()

    def _init_tags(self):
        self._df['tags'] = [[] for _ in range(len(self._df))]
        for tagger in self.taggers:
            tagger().tag(self._df)

    @lru_cache
    def dataframe(self, fill_dates=True):
        df_res = self._df.copy()

        if fill_dates:
            df_res = utils.fill_dates(df_res)
        df_res = df_res.sort_values(by=['date', 'time']).reset_index(drop=True)

        return df_res

    def get_tagged_rows(self, tag):
        df = self.dataframe()

        select_rows_condition = df['tags'].apply(lambda x: (tag in x) if tag else True)
        res_df = df[select_rows_condition]

        if select_rows_condition.sum() == 0:
            print(f'{" WARNING ":#^100}\n\tTag "{tag}" returned no rows.')

        return res_df

    def count_tag_appearance(self, tag):
        return len(self.get_tagged_rows(tag))

    @staticmethod
    @lru_cache
    def fully_tagged_statement():
        if os.path.exists(r"C:\Users\jim\PycharmProjects\mecon\statements\fully_tagged_statement.csv"):
            print("Loading tagged data...")
            df_statement = pd.read_csv(r"C:\Users\jim\PycharmProjects\mecon\statements\fully_tagged_statement.csv", index_col=None)
            df_statement['date'] = pd.to_datetime(df_statement['date'])
            tagged_data = TaggedData(df_statement, ALL_TAGS)  # if recalculate_tags else None)
        else:
            print("Producing tagged data...")
            df_statement = Statement().dataframe()
            tagged_data = TaggedData(df_statement, ALL_TAGS)
            print("Saving tagged data...")
            df_statement.to_csv(r"C:\Users\jim\PycharmProjects\mecon\statements\fully_tagged_statement.csv", index=None)

        return tagged_data
