from typing import List

import pandas as pd

from mecon.data.transactions import Transactions
from mecon.etl import io_framework
from mecon.tags.tag_helpers import tag_stats_from_transactions
from mecon.tags.tagging import Tag


class BaseDataManager:
    def __init__(self,
                 trans_io: io_framework.CombinedTransactionsIOABC,
                 tags_io: io_framework.TagsIOABC,
                 tags_metadata_io: io_framework.TagsMetadataIOABC,
                 hsbc_stats_io: io_framework.RawTransactionsIOABC,
                 monzo_stats_io: io_framework.RawTransactionsIOABC,
                 revo_stats_io: io_framework.RawTransactionsIOABC):
        self._transactions = trans_io
        self._tags = tags_io
        self._tags_metadata = tags_metadata_io
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
        return Tag.from_json(tag_name, tag_dict['conditions_json'])

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
        tags = [Tag.from_json(tag_dict['name'], tag_dict['conditions_json']) for tag_dict in tags_dict]
        return tags

    def reset_transaction_tags(self):
        transactions = self.get_transactions().reset_tags()
        all_tags = self.all_tags()

        for tag in all_tags:
            transactions = transactions.apply_tag(tag)

        data_df = transactions.dataframe()
        self._transactions.update_tags(data_df)

        tags_metadata = tag_stats_from_transactions(transactions)
        self.replace_tags_metadata(tags_metadata)

    def get_tags_metadata(self):
        tags_metadata_df = self._tags_metadata.get_all_metadata()
        # TODO date_created currently not returned by the tags_io
        # tags_df = pd.DataFrame(self._tags.all_tags())
        # tags_metadata_df = tags_df[['name', 'date_created']].merge(tags_metadata_df, on='name', how='left')
        return tags_metadata_df

    def replace_tags_metadata(self, metadata_df: pd.DataFrame):
        self._tags_metadata.replace_all_metadata(metadata_df)


class DataManager(BaseDataManager):
    pass


class CachedDataManager(BaseDataManager):
    def __init__(self,
                 trans_io: io_framework.CombinedTransactionsIOABC,
                 tags_io: io_framework.TagsIOABC,
                 tags_metadata_io: io_framework.TagsMetadataIOABC,
                 hsbc_stats_io: io_framework.RawTransactionsIOABC,
                 monzo_stats_io: io_framework.RawTransactionsIOABC,
                 revo_stats_io: io_framework.RawTransactionsIOABC):
        super().__init__(trans_io=trans_io,
                         tags_io=tags_io,
                         tags_metadata_io=tags_metadata_io,
                         hsbc_stats_io=hsbc_stats_io,
                         monzo_stats_io=monzo_stats_io,
                         revo_stats_io=revo_stats_io)
        self._cache = DataCache()

    def get_transactions(self) -> Transactions:
        if self._cache.transaction is None:
            trans_df = self._transactions.get_transactions()
            self._cache.transaction = Transactions(trans_df)
        return self._cache.transaction

    def reset_transactions(self):
        self._cache.reset_transactions()
        super().reset_transactions()
        self.get_transactions()  # preload cache

    def add_statement(self, df_statement: pd.DataFrame | List[pd.DataFrame], bank: str):
        super().add_statement(df_statement, bank)
        self._cache.reset_statements()

    def get_hsbc_statements(self) -> pd.DataFrame:
        if self._cache.hsbc_statements is None:
            self._cache.hsbc_statements = super().get_hsbc_statements()
        return self._cache.hsbc_statements

    def get_monzo_statements(self) -> pd.DataFrame:
        if self._cache.monzo_statements is None:
            self._cache.monzo_statements = super().get_monzo_statements()
        return self._cache.monzo_statements

    def get_revo_statements(self) -> pd.DataFrame:
        if self._cache.revo_statements is None:
            self._cache.revo_statements = super().get_revo_statements()
        return self._cache.revo_statements

    def reset_statements(self):
        super().reset_statements()
        self._cache.reset_statements()

    def get_tag(self, tag_name) -> Tag | None:
        if not self._cache.tags:
            self.all_tags()

        return self._cache.tags.get(tag_name)

    def update_tag(self, tag: Tag, update_tags=True):
        self._cache.reset_tags()
        super().update_tag(tag, update_tags)

    def delete_tag(self, tag_name: str, update_tags=True):
        self._cache.reset_tags()
        super().delete_tag(tag_name, update_tags)

    def all_tags(self) -> List[Tag]:
        if not self._cache.tags:
            tags = super().all_tags()
            self._cache.tags = {tag.name: tag for tag in tags}
        return list(self._cache.tags.values())

    def reset_transaction_tags(self):
        super().reset_transaction_tags()
        self._cache.reset_transactions()

    def get_tags_metadata(self):
        if self._cache.tags_metadata is None:
            self._cache.tags_metadata = super().get_tags_metadata()
        return self._cache.tags_metadata

    def replace_tags_metadata(self, metadata_df: pd.DataFrame):
        super().replace_tags_metadata(metadata_df)
        self._cache.reset_tags_metadata()


class DataCache:
    def __init__(self):
        self.transaction = None
        self.tags = {}
        self.tags_metadata = None
        self.hsbc_statements = None
        self.monzo_statements = None
        self.revo_statements = None

    def reset_transactions(self):
        self.transaction = None

    def reset_tags(self):
        self.tags = {}

    def reset_tags_metadata(self):
        self.tags_metadata = None

    def reset_statements(self):
        self.hsbc_statements = None
        self.monzo_statements = None
        self.revo_statements = None

    def reset_all(self):
        self.reset_statements()
        self.reset_tags()
        self.reset_transactions()
        self.reset_tags_metadata()
