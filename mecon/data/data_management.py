from typing import List

import pandas as pd

from mecon.app import WorkingDatasetDir
from mecon.data.transactions import Transactions
from mecon.import_data import io_framework
from mecon.tag_tools.tagging import Tag
from mecon.monitoring import tag_monitoring
from mecon import config


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

    def get_transactions(self) -> Transactions:
        return Transactions(self._transactions.get_transactions())

    def reset_transactions(self):
        self._transactions.delete_all()
        self._transactions.load_transactions()
        self.reset_transaction_tags()

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
            self.reset_transaction_tags()

    def delete_tag(self, tag_name: str, update_tags=True):
        self._tags.delete_tag(tag_name)

        if update_tags:
            self.reset_transaction_tags()

    def all_tags(self) -> List[Tag]:
        tags_dict = self._tags.all_tags()
        tags = [Tag.from_json_string(tag_dict['name'], tag_dict['conditions_json']) for tag_dict in tags_dict]
        return tags

    def reset_transaction_tags(self):
        transactions = self.get_transactions().reset_tags()
        all_tags = self.all_tags()

        for tag in all_tags:
            transactions = transactions.apply_tag(tag)

        data_df = transactions.dataframe()
        self._transactions.update_tags(data_df)


class CacheDataManager:
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
        if self._cache.transaction is None:
            trans_df = self._transactions.get_transactions()  # TODO make sure df has the right columns even if it is empty
            self._cache.transaction = Transactions(trans_df)
        return self._cache.transaction

    def reset_transactions(self):
        self._cache.reset_transactions()
        self._transactions.delete_all()
        self._transactions.load_transactions()
        self.reset_transaction_tags()
        self.get_transactions()  # preload

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

        self._cache.reset_statements()

    def get_hsbc_statements(self) -> pd.DataFrame:
        if self._cache.hsbc_statements is None:
            self._cache.hsbc_statements = self._hsbc_statements.get_transactions()
        return self._cache.hsbc_statements

    def get_monzo_statements(self) -> pd.DataFrame:
        if self._cache.monzo_statements is None:
            self._cache.monzo_statements = self._monzo_statements.get_transactions()
        return self._cache.monzo_statements

    def get_revo_statements(self) -> pd.DataFrame:
        if self._cache.revo_statements is None:
            self._cache.revo_statements = self._revo_statements.get_transactions()
        return self._cache.revo_statements

    def reset_statements(self):
        self._hsbc_statements.delete_all()
        self._monzo_statements.delete_all()
        self._revo_statements.delete_all()
        self._cache.reset_statements()

    def get_tag(self, tag_name) -> Tag | None:
        if self._cache.tags is None or len(self._cache.tags) == 0:
            self.all_tags()

        if tag_name not in self._cache.tags:
            return None
        return self._cache.tags[tag_name]

    def update_tag(self, tag: Tag, update_tags=True):
        self._tags.set_tag(tag.name, tag.rule.to_json())

        self._cache.reset_tags()
        if update_tags:
            self.reset_transaction_tags()

    def delete_tag(self, tag_name: str, update_tags=True):
        self._tags.delete_tag(tag_name)

        self._cache.reset_tags()
        if update_tags:
            self.reset_transaction_tags()

    def all_tags(self) -> List[Tag]:
        if self._cache.tags is None or len(self._cache.tags) == 0:
            tags_dict = self._tags.all_tags()
            tags = []
            for tag_dict in tags_dict:
                tag = Tag.from_json_string(tag_dict['name'],
                                           tag_dict['conditions_json'])
                tags.append(tag)

            self._cache.tags = {tag.name: tag for tag in tags}
        return list(self._cache.tags.values())

    def reset_transaction_tags(self):
        transactions = self.get_transactions().reset_tags()
        all_tags = self.all_tags()

        if config.TAG_MONITORING:
            tagging_monitor = tag_monitoring.TaggingStatsMonitoringSystem(all_tags)

        for tag in all_tags:
            transactions = transactions.apply_tag(tag)

        if config.TAG_MONITORING:
            tagging_report = tagging_monitor.produce_report()
            tagging_report.store(WorkingDatasetDir.get_instance().working_dataset.path)

        data_df = transactions.dataframe()
        self._transactions.update_tags(data_df)

        self._cache.reset_transactions()


class DataCache:
    def __init__(self):
        self.transaction = None
        self.tags = {}
        self.hsbc_statements = None
        self.monzo_statements = None
        self.revo_statements = None

    def reset_transactions(self):
        self.transaction = None

    def reset_tags(self):
        self.tags = None

    def reset_statements(self):
        self.hsbc_statements = None
        self.monzo_statements = None
        self.revo_statements = None

    def reset_all(self):
        self.reset_statements()
        self.reset_tags()
        self.reset_transactions()
