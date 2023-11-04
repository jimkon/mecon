import abc
import pathlib

import pandas as pd


class BankStatementAdapter(abc.ABC):
    def __init__(self, bank_name):
        self._bank_name = bank_name

    @property
    def bank_name(self):
        return self._bank_name

    def adapt_dataframe(self, df):
        res_dict = dict()

        res_dict['datetime'] = self.map_datetime(df)
        res_dict['amount'] = self.map_amount(df)
        res_dict['currency'] = self.map_currency(df)
        res_dict['description'] = self.map_description(df)

        return pd.DataFrame(res_dict)

    @abc.abstractmethod
    def map_datetime(self, df):
        pass

    @abc.abstractmethod
    def map_amount(self, df):
        pass

    @abc.abstractmethod
    def map_currency(self, df):
        pass

    @abc.abstractmethod
    def map_description(self, df):
        pass


class MonzoStatementAdapter(BankStatementAdapter):
    def __init__(self):
        super().__init__('Monzo')

    def map_datetime(self, df):
        return pd.to_datetime(df['Date'].astype(str), format='%d/%m/%Y').dt.date

    def map_amount(self, df):
        return df['Amount']

    def map_currency(self, df):
        return df['Local currency']

    def map_description(self, df):
        description_cols = ['Type', 'Name', 'Emoji', 'Category', 'Notes and #tags', 'Address', 'Receipt', 'Description',
                            'Category split']
        df['description'] = 'Bank:Monzo,'

        for col in description_cols:
            df['description'] = df['description'] + col + ':' + df[col].fillna('None') + ','
        return df['description']


class HSBCStatementAdapter(BankStatementAdapter):
    def __init__(self):
        super().__init__('HSBC')

    @staticmethod
    def _to_int(s):
        try:
            s = s.replace(',', '')
            res = float(s)
        except ValueError as e:
            res = 0.0
            print(f"Conversion of amount {s} to float failed with exception {e}. 0.0 returned.")
        return res

    def map_datetime(self, df):
        return pd.to_datetime(df['date'].astype(str), format='%d/%m/%Y').dt.date

    def map_amount(self, df):
        return df['amount'].apply(lambda x: self._to_int(x))

    def map_currency(self, df):
        return 'GBP'

    def map_description(self, df):
        return "Bank:HSBC,"+df['description']


class RevolutStatementAdapter(BankStatementAdapter):
    def __init__(self):
        super().__init__('Revolut')

    def map_datetime(self, df):
        return pd.to_datetime(df['Started Date'], errors='coerce').dt.date

    def map_amount(self, df):
        return df['Amount']

    def map_currency(self, df):
        return df['Currency']

    def map_description(self, df):
        return "Bank:HSBC," + df['Description']


class NotValidBankStatementError(Exception):
    pass


class BankStatement:
    columns = ['datetime', 'amount', 'currency', 'description']

    @staticmethod
    def validate_input_dataframe_columns(cols):
        if cols != BankStatement.columns:
            raise NotValidBankStatementError(f"Expecting columns {BankStatement.columns} for a bank statement. Received: {cols}")

    def __init__(self, df):
        self._df = df
        BankStatement.validate_input_dataframe_columns(self._df)

    def dataframe(self):
        return self._df

    def merge(self, bank_statements):
        if not isinstance(bank_statements, list):
            bank_statements = [bank_statements]

        return pd.concat([self.dataframe(), *bank_statements], axis=0)


class LocalDirBankStatementFactory:
    @staticmethod
    def load_from_path(path):
        path = pathlib.Path(path)
        df = pd.read_csv(path)

        parent_dir = str(path.parent)
        if path == "Monzo":
            adapter = MonzoStatementAdapter()
        elif path == "HSBC":
            adapter = HSBCStatementAdapter()
        elif path == "Revolut":
            adapter = RevolutStatementAdapter()
        else:
            raise ValueError(f"Adapter for {parent_dir} bank statements not found.")

        adapted_df = adapter.adapt_dataframe(df)
        bank_statement = BankStatement(adapted_df)
        return bank_statement

    def __init__(self, statements_dir):
        self._dir = statements_dir

    def explore_statements(self):
        statement_paths = pathlib.Path(self._dir).rglob("**")
        return statement_paths

    def load_statements(self):
        statements = [LocalDirBankStatementFactory.load_from_path(path) for path in self.explore_statements()]
        # can also group and merge based on the bank
        return statements


