import os.path
from ast import literal_eval
from functools import lru_cache, cached_property

import pandas as pd

from mecon.statements.combined_statement import Transactions
from mecon.tagging.tags import ALL_TAGS
from mecon import calendar_utils
from mecon import logs
from mecon import configs


class TaggedData:
    def __init__(self, df):
        self._df = df
        if 'tags' not in self._df.columns:
            self._df['tags'] = [[] for _ in range(len(self._df))]

    def copy(self):
        return TaggedData(self.dataframe())

    def query_data(self, query):
        new_df = query(self.dataframe())
        return TaggedData(new_df)

    def apply_taggers(self, taggers):
        if not isinstance(taggers, list):
            taggers = [taggers]

        df = self.dataframe()
        for tagger in taggers:
            tagger.tag(df)
        return TaggedData(df)

    def fill_days(self):
        df_res = self.dataframe()
        df_res = calendar_utils.fill_days(df_res)
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

    def get_rows_tagged_as(self, tags):
        if not isinstance(tags, list):
            tags = [tags]

        df = self.dataframe()

        select_rows_condition = df['tags'].apply(lambda x: any([((tag in x) if tag else True) for tag in tags]))
        res_df = df[select_rows_condition]

        if select_rows_condition.sum() == 0:
            print(f'{" WARNING ":#^100}\n\tTags "{tags}" returned no rows.')

        return TaggedData(res_df)

    def merge(self, tagged_data): # TODO check
        df = pd.concat([self.dataframe(), tagged_data.dataframe()]).drop_duplicates()
        return TaggedData(df)


class UntaggedData(TaggedData):
    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = UntaggedData()
        return cls._instance

    def __init__(self, reset_tags=False):
        df = Transactions().dataframe()
        super().__init__(df)


class FullyTaggedData(TaggedData):
    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = FullyTaggedData()
        return cls._instance

    def __init__(self, reset_tags=False):
        if os.path.exists(configs.FULLY_TAGGED_DATASET_CSV) and not reset_tags:
            logs.log_disk_io("Loading tagged datasets...")
            df_statement = pd.read_csv(configs.FULLY_TAGGED_DATASET_CSV, index_col=None)
            df_statement['date'] = pd.to_datetime(df_statement['date'])
            df_statement['tags'] = df_statement['tags'].apply(lambda x: literal_eval(x))
            super().__init__(df_statement)
        else:
            logs.log_calculation("Producing tagged datasets...")
            df_statement = Transactions().dataframe()
            super().__init__(df_statement)
            self.apply_taggers(ALL_TAGS)
            logs.log_disk_io("Saving tagged datasets...")
            self.dataframe().to_csv(configs.FULLY_TAGGED_DATASET_CSV, index=None)


if __name__ == "__main__":
    data = UntaggedData.instance()
    print(data.dataframe().head())
    print(data.all_different_tags)

    data = FullyTaggedData.instance()
    print(data.dataframe().head())
    print(data.all_different_tags)

    from mecon.tagging.json_tags import SelectQuery
    res = data.query_data(SelectQuery([
    {
        "description": {
            "contains": "Bank:Revolut"
        }
    }
]))
    t = 0



