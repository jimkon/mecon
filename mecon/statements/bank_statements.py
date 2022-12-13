import os
from abc import ABC
import datetime

import pandas as pd

from mecon.statements.statement_core import ABCFetchStatement
from mecon import currency


def _grab_statement_dfs(_dir=''):
    dir_path = os.path.join(r"C:\Users\jim\PycharmProjects\mecon\statements", _dir)
    csv_files = [os.path.join(dir_path, filename) for filename in os.listdir(dir_path) if filename.endswith('.csv')]

    list_df_raw = []
    for statement_path in csv_files:
        df_statement_raw = pd.read_csv(statement_path)
        list_df_raw.append(df_statement_raw)
        print(
            f"Filepath: {statement_path}, rows: {len(df_statement_raw)}, columns({len(df_statement_raw.columns)}): {df_statement_raw.columns}")

    return list_df_raw


class BankStatement(ABCFetchStatement, ABC):
    bank_name = ''

    def __init__(self, df_raw):
        super().__init__(df_raw)

    @classmethod
    def read_from_source(cls):
        dfs = _grab_statement_dfs(cls.bank_name)
        df_raws = []
        for df_raw in dfs:
            df_raws.append(df_raw)
        print(f"{len(df_raws)} statements have been read from {cls.bank_name} directory.")

        df_combined = pd.concat(df_raws, axis=0)
        print(f"{len(df_combined)} total rows.")
        # print(f"{len(df_combined.columns)} total columns. {df_combined.columns}")

        print(f"Duplicates removed: {sum(df_combined.duplicated())}")
        df_combined = df_combined.drop_duplicates()
        return cls(df_combined)


class MonzoStatement(BankStatement):
    bank_name = 'Monzo'

    def date(self):
        return pd.to_datetime(self.df_raw['Date'].astype(str), format='%d/%m/%Y').dt.date

    def time(self):
        return self.df_raw['Time']

    def amount(self):
        return self.df_raw['Amount']

    def currency(self):
        return self.df_raw['Local currency']

    def amount_curr(self):
        return self.df_raw['Local amount']

    def description(self):
        description_cols = ['Type', 'Name', 'Emoji', 'Category', 'Notes and #tags', 'Address', 'Receipt', 'Description',
                            'Category split']
        self.df_raw['description'] = 'Bank:Monzo,'

        for col in description_cols:
            self.df_raw['description'] = self.df_raw['description'] + col + ':' + self.df_raw[col].fillna('None') + ','
        return self.df_raw['description']


class HSBCStatement(BankStatement):
    bank_name = 'HSBC'

    def __init__(self, df_raw):
        df_raw.columns = ['date', 'description', 'amount']
        super().__init__(df_raw)

    def date(self):
        return pd.to_datetime(self.df_raw['date'].astype(str), format='%d/%m/%Y').dt.date

    def time(self):
        return datetime.time()

    def amount(self):
        return self.df_raw['amount'].apply(lambda x: self._to_int(x))

    def currency(self):
        return 'GBP'

    def amount_curr(self):
        return self.df_raw['amount'].apply(lambda x: self._to_int(x))

    def description(self):
        return self.df_raw['description']+",Bank:HSBC"

    @staticmethod
    def _to_int(s):
        try:
            s = s.replace(',', '')
            res = float(s)
        except ValueError as e:
            res = 0.0
            print(f"Conversion of amount {s} to float failed with exception {e}. 0.0 returned.")
        return res


class RevolutStatement(BankStatement):
    bank_name = 'Revolut'

    def date(self):
        return pd.to_datetime(self.df_raw['Started Date'], errors='coerce').dt.date

    def time(self):
        return pd.to_datetime(self.df_raw['Started Date'], errors='coerce').dt.time

    def amount(self):
        gbp_amount = self.df_raw['Amount'] * self.df_raw['Currency'].apply(currency.GBP_to_curr_exchange_rate)
        return gbp_amount

    def currency(self):
        return self.df_raw['Currency']

    def amount_curr(self):
        return self.df_raw['Amount']

    def description(self):
        return self.df_raw['Description']+",Bank:Revolut"