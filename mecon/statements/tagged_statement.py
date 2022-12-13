from functools import lru_cache

import pandas as pd

from mecon.statements.combined_statement import Statement
from mecon.tagging.tags import *
from mecon import utils


class TaggedStatement(Statement):
    def __init__(self, taggers):
        super().__init__()
        self.taggers = taggers
        self._df['tags'] = [[] for _ in range(len(self._df))]

    def tags(self):
        df_copy = self._df.copy()
        df_copy['tags'] = [[] for _ in range(len(self.date()))]
        for tagger in self.taggers:
            tagger().tag(df_copy)
        return df_copy['tags']

    @lru_cache
    def dataframe(self, fill_dates=True):
        df_res = pd.DataFrame({
            'date': self.date(),
            'month_date': self.month_date(),
            'time': self.time(),
            'amount': self.amount(),
            'currency': self.currency(),
            'amount_curr': self.amount_curr(),
            'description': self.description(),
            'tags': self.tags(),
        })

        if fill_dates:
            df_res = utils.fill_dates(df_res)
        df_res = df_res.sort_values(by=['date', 'time']).reset_index(drop=True)

        return df_res

    def get_tagged_rows(self, tag):
        cond = self.dataframe()['tags'].apply(lambda x: (tag in x) if tag else True)
        if cond.sum() == 0:
            print(f'{" WARNING ":#^100}\n\tTag "{tag}" returned no rows.')
        return self.dataframe()[cond]

    def count_tag_appearance(self, tag):
        return len(self.get_tagged_rows(tag))

    @staticmethod
    @lru_cache
    def fully_tagged_statement():
        return TaggedStatement(ALL_TAGS)
