import os

import pandas as pd

from mecon.statements.statement_core import ABCFetchStatement
from mecon.statements.bank_statements import MonzoStatement, RevolutStatement, HSBCStatement
from mecon.utils import date_to_month_date


class CombinedStatement(ABCFetchStatement):
    def __init__(self):
        df_monzo_statement = MonzoStatement.read_from_source().dataframe()
        df_hsbc_statement = HSBCStatement.read_from_source().dataframe()
        df_revo_statement = RevolutStatement.read_from_source().dataframe()

        df_combined = pd.concat([df_monzo_statement, df_hsbc_statement, df_revo_statement])
        super().__init__(df_combined)
        self.to_csv()

    def date(self):
        return pd.to_datetime(self.df_raw['date'])

    def time(self):
        return self.df_raw['time']

    def amount(self):
        return self.df_raw['amount']

    def currency(self):
        return self.df_raw['currency']

    def amount_curr(self):
        return self.df_raw['amount_curr']

    def description(self):
        return self.df_raw['description']

    def to_csv(self):
        self.dataframe().to_csv(r'C:/Users/jim/PycharmProjects/mecon/statements/combined/combined_statement.csv')


class Statement:
    _cached_file_path = r"C:\Users\jim\PycharmProjects\mecon\statements\statement.csv"

    def __init__(self, reset=False):
        if os.path.exists(self._cached_file_path) and not reset:
            self._df = pd.read_csv(self._cached_file_path)
        else:
            self._df = CombinedStatement().dataframe().reset_index(drop=True)
            self._df.to_csv(self._cached_file_path)

    def date(self):
        return pd.to_datetime(self._df['date'])

    def month_date(self):
        return date_to_month_date(self.date())
        # return self.date().apply(date_to_month_date)
        # return self.date().dt.year.astype(str)+'-'+self.date().dt.month.astype(str).apply(lambda x:f'{x:0>2}')

    def time(self):
        return self._df['time']

    def amount(self):
        return self._df['amount'].astype(float)

    def currency(self):
        return self._df['currency']

    def amount_curr(self):
        return self._df['amount_curr'].astype(float)

    def description(self):
        return self._df['description']

    def dataframe(self):
        df_res = pd.DataFrame({
            'date': self.date(),
            'month_date': self.month_date(),
            'time': self.time(),
            'amount': self.amount(),
            'currency': self.currency(),
            'amount_curr': self.amount_curr(),
            'description': self.description(),
        })

        df_res = df_res.sort_values(by=['date', 'time']).reset_index(drop=True)

        return df_res

