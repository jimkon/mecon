import logging
from typing import List

import pandas as pd

from mecon.data.transactions import Transactions
from mecon.import_data import io_framework
from mecon.tag_tools.tagging import Tag


class DataManagerStreamsCannotChange(Exception):
    pass


class DataManager:
    def __init__(self,
                 trans_io: io_framework.CombinedTransactionsIOABC,
                 tags_io: io_framework.TagsIOABC,
                 hsbc_stats_io: io_framework.HSBCTransactionsIOABC,
                 monzo_stats_io: io_framework.MonzoTransactionsIOABC,
                 revo_stats_io: io_framework.RevoTransactionsIOABC):
        self._transactions = trans_io
        self._tags = tags_io
        self._hsbc_statements = hsbc_stats_io
        self._monzo_statements = monzo_stats_io
        self._revo_statements = revo_stats_io
        self._cache = DataCache()  # TODO use cache

    def get_transactions(self) -> Transactions:
        return Transactions(self._transactions.get_transactions())

    def reset_transactions(self):
        self._cache.reset()
        self._transactions.delete_all()
        self._transactions.load_transactions()
        self.reset_tags()

    def add_statement(self, df_statement: pd.DataFrame | List[pd.DataFrame], bank: str):
        valid_banks = ['HSBC', 'Monzo', 'Revolut']
        if bank not in valid_banks:
            raise ValueError(f"Bank name can be one of {valid_banks}: {bank} was given instead")

        if bank == 'HSBC':
            self._hsbc_statements.import_statement(df_statement)
        elif bank == 'Monzo':
            self._monzo_statements.import_statement(df_statement)
        elif bank == 'Revolut':
            self._revo_statements.import_statement(df_statement)

    def get_hsbc_statements(self) -> pd.DataFrame:
        return self._hsbc_statements.get_transactions()

    def get_monzo_statements(self) -> pd.DataFrame:
        return self._monzo_statements.get_transactions()

    def get_revo_statements(self) -> pd.DataFrame:
        return self._revo_statements.get_transactions()

    def reset_statements(self):
        # self._cache.reset()
        self._hsbc_statements.delete_all()
        self._monzo_statements.delete_all()
        self._revo_statements.delete_all()

    def get_tag(self, tag_name) -> Tag | None:
        tag_dict = self._tags.get_tag(tag_name)

        if tag_dict is None:
            return None

        return Tag.from_json_string(tag_name, tag_dict['conditions_json'])

    def update_tag(self, tag: Tag, update_tags=True):
        self._tags.set_tag(tag.name, tag.rule.to_json())

        if update_tags:
            self.reset_tags()

    def delete_tag(self, tag_name: str, update_tags=True):
        self._tags.delete_tag(tag_name)

        if update_tags:
            self.reset_tags()

    def all_tags(self) -> List[Tag]:
        tags_dict = self._tags.all_tags()
        tags = [Tag.from_json_string(tag_dict['name'], tag_dict['conditions_json']) for tag_dict in tags_dict]
        return tags

    def reset_tags(self):
        transactions = self.get_transactions().reset_tags()
        all_tags = self.all_tags()
        self._cache.reset()

        for tag in all_tags:
            transactions = transactions.apply_tag(tag)

        data_df = transactions.dataframe()
        self._transactions.update_tags(data_df)


class DataCache(dict):
    def reset(self):
        for key in self.keys():
            del self[key]
