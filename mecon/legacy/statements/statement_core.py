from abc import ABC, abstractmethod

import pandas as pd


class ABCFetchStatement(ABC):
    bank_name = None

    def __init__(self, df_raw):
        self.df_raw = df_raw

        if 'tags' not in self.df_raw.columns:
            self.df_raw['tags'] = ''

    def len(self):
        return len(self.df_raw)

    @abstractmethod
    def date(self):
        pass

    @abstractmethod
    def time(self):
        pass

    @abstractmethod
    def amount(self):
        pass

    @abstractmethod
    def currency(self):
        pass

    @abstractmethod
    def amount_curr(self):
        pass

    @abstractmethod
    def description(self):
        pass

    def dataframe(self):
        return pd.DataFrame({
            'date': self.date(),
            'time': self.time(),
            'amount': self.amount(),
            'currency': self.currency(),
            'amount_curr': self.amount_curr(),
            'description': self.description(),
        })

    def print(self):
        print(self.dataframe().head(10))








