import json
import logging
import pathlib
from datetime import datetime
from typing import List

import pandas as pd

from mecon.data.transactions import Transactions
from mecon.etl import io_framework
from mecon.etl.dataset import Dataset
from mecon.tags.process import OptREPTagging
from mecon.tags.tag_helpers import tag_stats_from_transactions
from mecon.tags.tagging import Tag
from mecon.etl import transformers


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

        sess = OptREPTagging(all_tags).create_rule_execution_plan().create_optimised_rule_execution_plan()
        transactions = sess.tag(transactions)

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


class CachedFileDataManager:
    def __init__(self, dataset: Dataset):
        self.dataset = dataset
        self.files_dirpath = dataset.db.parent
        self.statements_dirpath = self.files_dirpath / "statements"

        self.transactions = None
        self._transactions_path = self.files_dirpath / 'transactions.csv'
        self._load_transactions()

        self.tags_df = None
        self._tags_path = self.files_dirpath / 'tags.csv'
        self._load_tags()

        self.tags_metadata_df = None
        self._tags_metadata_path = self.files_dirpath / 'tags_metadata.csv'
        self._load_tags_metadata()
        pass

    def _load_transactions(self):
        self.transactions = Transactions.from_csv(self._transactions_path) if self._transactions_path.exists() else None

    def _load_tags(self):
        self.tags_df = pd.read_csv(self._tags_path, index_col=None) if self._tags_path.exists() else None

    def _load_tags_metadata(self):
        self.tags_metadata_df = pd.read_csv(self._tags_metadata_path,
                                            index_col=None) if self._tags_metadata_path.exists() else None

    def _save_transactions(self):
        self.transactions.dataframe().to_csv(self._transactions_path, index=False)

    def _save_tags(self):
        self.tags_df.to_csv(self._tags_path, index=False)

    def _save_tags_metadata(self):
        self.tags_metadata_df.to_csv(self._tags_metadata_path, index=False)

    def get_statement_filepaths(self) -> dict[str, list[pathlib.Path]]:
        # all_statement_paths = list(self.statements_dirpath.rglob("*.csv"))
        # sources_and_filenames = {}
        # for statement_filepath in all_statement_paths_dict:
        #     key = str(statement_filepath.parent.name)
        #     if key not in sources_and_filenames:
        #         sources_and_filenames[key] = []
        #
        #     sources_and_filenames[key].append(statement_filepath)

        sources_and_filenames = self.dataset.statement_files()
        return sources_and_filenames

    def get_statements(self) -> dict[str, list[pd.DataFrame]]:
        filepaths = self.get_statement_filepaths()
        sources_and_statements = {
            source: [transformers.StatementTransformer.factory(source).read_df(filepath) for filepath in filepaths]
            for source, filepaths in filepaths.items() if source in transformers.StatementTransformer.SOURCES}
        return sources_and_statements

    def get_transformed_statements(self) -> dict[str, pd.DataFrame]:
        sources_and_statements = self.get_statements().copy()
        sources_and_merged_statenents = {}
        for source, statements in sources_and_statements.items():
            try:
                merged_statements = pd.concat(statements)
                transformer = transformers.StatementTransformer.factory(source)
                transformed_statement = transformer.transform(merged_statements)
                sources_and_merged_statenents[source] = transformed_statement
            except ValueError:
                # del sources_and_statements[source]
                logging.error(f"Failed to parse '{source}' statements, {len(statements)} files.")
                raise
            except Exception as e:
                raise

        return sources_and_merged_statenents

    def reset_transactions(self):
        sources_and_statements = self.get_transformed_statements()
        df_merged = pd.concat(sources_and_statements.values())
        df_merged.sort_values(by=["datetime"], inplace=True)

        self.transactions = Transactions(df_merged)
        df_merged['tags'] = ''  # '[[] for _ in range(len(df_merged))]
        self._save_transactions()
        # logging.info(f"Wrote {self.transactions.size()} transactions to {self.files_dirpath}")
        return self

    def get_transactions(self) -> Transactions:
        return self.transactions

    def get_tagged_transactions(self) -> Transactions:
        if 'tags' not in self.transactions.dataframe().columns:
            self.reset_transaction_tags()
        return self.transactions

    def get_tag(self, tag_name) -> Tag | None:
        tags_dict = self.tags_df.set_index('name').to_dict('index')

        if tag_name not in tags_dict:
            return None

        tag = Tag.from_json_string(tag_name, tags_dict[tag_name]['conditions_json'])

        return tag

    def update_tag(self, tag: Tag, update_tags=True):
        tags_dict = self.tags_df.set_index('name').to_dict('index')

        tag_name = tag.name
        if tag_name not in tags_dict:
            tags_dict[tag_name] = {
                'conditions_json': None,
                'date_created': datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
            }
        tags_dict[tag_name]['conditions_json'] = json.dumps(tag.rule.to_json())
        self.tags_df = pd.DataFrame.from_dict(tags_dict, orient='index').reset_index().rename(columns={'index': 'name'})
        if update_tags:
            self.reset_transaction_tags()

        self._save_tags()

    def delete_tag(self, tag_name: str, update_tags=True):
        tags_dict = self.tags_df.set_index('name').to_dict('index')

        if tag_name not in tags_dict:
            return

        del tags_dict[tag_name]
        self.tags_df = pd.DataFrame.from_dict(tags_dict, orient='index').reset_index().rename(columns={'index': 'name'})

        if update_tags:
            self.reset_transaction_tags()

        self._save_tags()

    def all_tags(self) -> List[Tag]:
        tags = [Tag.from_json_string(row['name'], row['conditions_json']) for i, row in self.tags_df.iterrows()]
        return tags

    def reset_transaction_tags(self):
        transactions = self.get_transactions().reset_tags()
        all_tags = self.all_tags()

        sess = OptREPTagging(all_tags) \
            .create_rule_execution_plan() \
            .create_optimised_rule_execution_plan()
        transactions = sess.tag(transactions)
        self.transactions = transactions
        self._save_transactions()

        tags_metadata = tag_stats_from_transactions(transactions)
        self.replace_tags_metadata(tags_metadata)

    def get_tags_metadata(self):
        if self.tags_metadata_df is None:
            path = self.files_dirpath / 'tags_metadata.csv'
            self.tags_metadata_df = pd.read_csv(path, index_col=None)

        self.all_tags()  # load tags if not already loaded
        df_metadata = self.tags_df.merge(self.tags_metadata_df, on='name')
        del df_metadata['conditions_json']

        return df_metadata

    def replace_tags_metadata(self, metadata_df: pd.DataFrame):
        self.tags_metadata_df = metadata_df
        self.tags_metadata_df['date_modified'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        self._save_tags_metadata()


    def reset(self):
        self.reset_transactions()
        self._load_tags()
        self.reset_transaction_tags()

