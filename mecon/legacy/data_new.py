import pathlib
import logging

import pandas as pd

from legacy.statements.bank_statements_new import BankStatement, LocalDirBankStatementFactory


class BankStatementsHandler:
    @classmethod
    def load_from_dir(cls, statement_dir):
        bank_statement_factory = LocalDirBankStatementFactory(statement_dir)
        bank_statements = bank_statement_factory.load_statements()

        handler = BankStatementsHandler()
        handler.add_statement(bank_statements)
        return handler

    def __init__(self):
        self._statements = {}
        self._merged_statements = None

    @property
    def statements(self):
        return self._statements

    @property
    def merged_statements(self):
        return self._merged_statements

    def add_statement(self, bank_statements_dict):
        for path, statement in bank_statements_dict.items():
            if path in self._statements.keys():
                logging.warning(f"Bank statement {path} already exists and it will be replaced.")
            self._statements[path] = statement

    def remove_statement(self, paths_to_remove):
        for path in paths_to_remove:
            if path not in self._statements.keys():
                logging.warning(f"Bank statement {path} cannot be removed.")
            del self._statements[path]

    def merge_statements(self):
        df_merged = pd.concat([stat.dataframe() for stat in self._statements])
        self._merged_statements = BankStatement(df_merged)


class AppDataManagement:
    def __init__(self, data_dir):
        self._dir = pathlib.Path(data_dir)

        self._bank_statement_handler = BankStatementsHandler.load_from_dir(self._dir / 'statements')

        raise NotImplementedError

    @property
    def statements(self):
        return self._bank_statement_handler.statements

    def add_bank_statement(self, bank_statement):
        self._bank_statement_handler.add_statement(bank_statement)
        self._bank_statement_handler.merge_statements()

    def remove_bank_statement(self, bank_statement):
        self._bank_statement_handler.remove_statement(bank_statement)
        self._bank_statement_handler.merge_statements()

    def _combine_bank_statements(self):
        raise NotImplementedError

    def load_transactions(self):
        raise NotImplementedError

    @property
    def transactions(self):
        raise NotImplementedError

    def load_tagged_transactions(self):
        raise NotImplementedError

    @property
    def tagged_transactions(self):
        raise NotImplementedError

    def set_tagger(self, tag_name, new_tagger, remove_old_tags=True):
        raise NotImplementedError

    def load_taggers(self):
        raise NotImplementedError

    @property
    def taggers(self):
        raise NotImplementedError

    def _apply_taggers(self):
        raise NotImplementedError




