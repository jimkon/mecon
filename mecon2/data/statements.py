import numpy as np
import pandas as pd


class StatementCSV:
    def __init__(self, path):
        self._path = path
        self._df = self.load()

    def load(self):
        return pd.read_csv(self._path)

    def validate(self):
        pass

    def transform(self):
        return self._df

    def dataframe(self) -> pd.DataFrame:
        self.validate()
        df = self.transform()
        return df


class HSBCStatementCSV(StatementCSV):
    pass


class MonzoStatementCSV(StatementCSV):
    def transform(self):
        self._df.rename(
            columns={
                "Transaction ID": "id",
                "Date": "date",
                "Time": "time",
                "Type": "transaction_type",
                "Name": "name",
                "Emoji": "emoji",
                "Category": "category",
                "Amount": "amount",
                "Currency": "currency",
                "Local amount": "local_amount",
                "Local currency": "local_currency",
                "Notes and #tags": "notes_tags",
                "Address": "address",
                "Receipt": "receipt",
                "Description": "description",
                "Category split": "category_split",
                "Money Out": "money_out",
                "Money In": "money_in",
            },
            inplace=True,
            errors="raise")

        del self._df["id"]

        return self._df


class RevoStatementCSV(StatementCSV):
    def transform(self):
        self._df.rename(
            columns={
                "Type": "type",
                "Product": "product",
                "Started Date": "start_date",
                "Completed Date": "completed_date",
                "Description": "description",
                "Amount": "amount",
                "Fee": "fee",
                "Currency": "currency",
                "State": "state",
                "Balance": "balance",
            },
            inplace=True,
            errors="raise")

        return self._df
