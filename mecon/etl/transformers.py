import abc
import logging

import numpy as np
import pandas as pd

from mecon.utils import currencies
from mecon.utils.dataframe_transformers import DataframeTransformer


def factory_db(source):
    if source == 'Monzo':
        return MonzoStatementTransformer()
    elif source == 'HSBC':
        return HSBCStatementTransformer()
    elif source == 'Revolut':
        return RevoStatementTransformer()
    else:
        raise ValueError(f"Invalid or unknown transaction source name: {source}")


def transaction_id_formula(transaction, source):
    if source == 'Monzo':
        bank_abr = 'MZN'
    elif source == 'HSBC':
        bank_abr = 'HSBC'
    elif source == 'Revolut':
        bank_abr = 'RVLT'
    else:
        raise ValueError(f"Invalid or unknown transaction source name: {source}")

    datetime_str = transaction['datetime'].strftime("d%Y%m%dt%H%M%S")
    amount_str = f"a{'p' if transaction['amount']>0 else 'n'}{int(100 * abs(transaction['amount']))}"
    ordinal_value = f"i{transaction['id']}" # TODO that can change depending on the dataset. maybe get different counter for each day
    result = f"{bank_abr}{datetime_str}{amount_str}{ordinal_value}"
    return result

def _convert_df_column_names(df):
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    return df


#TODO remove
class HSBCStatementTransformer(DataframeTransformer):
    def _transform(self, df_hsbc: pd.DataFrame) -> pd.DataFrame:
        # TODO:v3 make it more readable
        logging.info(f"Transforming HSBC raw transactions ({df_hsbc.shape} shape)")
        # Add prefix to id
        # df_hsbc['id'] = ('1' + df_hsbc['id'].astype(str)).astype(np.int64)

        # Combine date and time to create datetime
        df_hsbc['datetime'] = pd.to_datetime(df_hsbc['date'])

        # Set currency to GBP and amount_cur to amount
        df_hsbc['currency'] = 'GBP'

        df_hsbc['amount'] = df_hsbc['amount'].astype(str).str.replace(',', '').astype(float)
        df_hsbc['amount_cur'] = df_hsbc['amount']
        df_hsbc['description'] = 'bank:HSBC, ' + df_hsbc['description']

        df_hsbc['id'] = df_hsbc.apply(lambda row: transaction_id_formula(row, 'HSBC'), axis=1)

        # Select and rename columns
        df_transformed = df_hsbc[['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description']]
        df_transformed = df_transformed.rename(columns={'id': 'id', 'datetime': 'datetime', 'amount': 'amount',
                                                        'currency': 'currency', 'amount_cur': 'amount_cur',
                                                        'description': 'description'})

        return df_transformed

#TODO remove
class MonzoStatementTransformer(DataframeTransformer):
    def _transform(self, df_monzo: pd.DataFrame) -> pd.DataFrame:  # TODO:v3 make it more readable
        logging.info(f"Transforming Monzo raw transactions ({df_monzo.shape} shape)")

        # df_monzo['id'] = ('2' + df_monzo['id'].astype(str)).astype(np.int64)
        try:
            df_monzo['datetime'] = pd.to_datetime(df_monzo['date'], format="%Y-%m-%d") + pd.to_timedelta(
                df_monzo['time'].astype(str))
        except ValueError as ve:
            logging.warning(f"Unexpected date format (%d-%m-%Y): {ve}")
            df_monzo['datetime'] = pd.to_datetime(df_monzo['date'], format="%d/%m/%Y") + pd.to_timedelta(
                df_monzo['time'].astype(str))
        # df_monzo['datetime'] = pd.to_datetime(df_monzo['date'], format="%d/%m/%Y") + pd.to_timedelta(
        #     df_monzo['time'].astype(str))
        df_monzo['currency'] = df_monzo['local_currency']
        df_monzo['amount_cur'] = df_monzo['local_amount']

        df_monzo['id'] = df_monzo.apply(lambda row: transaction_id_formula(row, 'Monzo'), axis=1)

        # Concatenate columns to create description
        cols_to_concat = ['transaction_type', 'name', 'emoji', 'category', 'notes_tags', 'address', 'receipt',
                          'description',
                          'category_split', 'money_out', 'money_in']

        df_transformed = df_monzo[['id', 'datetime', 'amount', 'currency', 'amount_cur']].copy()

        df_transformed['description'] = df_monzo[cols_to_concat].apply(
            lambda x: ', '.join(
                [col + ": " + (str(x[col]) if pd.notnull(x[col]) else 'none') for col in cols_to_concat]),
            axis=1
        )
        df_transformed.loc[:, 'description'] = 'bank:Monzo, ' + df_transformed['description']

        df_transformed = df_transformed.reindex(
            columns=['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description'])

        return df_transformed

#TODO remove
class RevoStatementTransformer(DataframeTransformer):
    def __init__(self, currency_converter=None):
        self._currency_converter = currency_converter if currency_converter is not None else currencies.FixedRateCurrencyConverter()

    def convert_amounts(self, amount_ser, currency_ser, datetime_ser):
        return [self._currency_converter.amount_to_gbp(amount, currency, date)
                for amount, currency, date
                in zip(amount_ser, currency_ser, datetime_ser)]

    def _transform(self, df_revo: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Transforming Revolut raw transactions ({df_revo.shape} shape)")

        df_transformed = pd.DataFrame({'id': ('3' + df_revo['id'].astype(str)).astype(np.int64)})
        df_transformed['datetime'] = pd.to_datetime(df_revo['start_date'], format="%Y-%m-%d %H:%M:%S")
        df_transformed['amount'] = self.convert_amounts(df_revo['amount'], df_revo['currency'],
                                                        df_transformed['datetime'].dt.date)
        df_transformed['currency'] = df_revo['currency']
        df_transformed['amount_cur'] = df_revo['amount']

        df_transformed['id'] = df_transformed.apply(lambda row: transaction_id_formula(row, 'Revolut'), axis=1)

        # Concatenate columns to create description
        cols_to_concat = ['type', 'product', 'completed_date', 'description', 'fee', 'state', 'balance']
        df_transformed['description'] = df_revo[cols_to_concat].apply(
            lambda x: ', '.join([f"{col}: {x[col]}" for col in cols_to_concat if pd.notnull(x[col])]),
            axis=1
        )
        df_transformed['description'] = 'bank:Revolut, ' + df_transformed['description']

        return df_transformed


class StatementTransformer(DataframeTransformer, abc.ABC):
    SOURCES = ['Monzo', 'HSBC', 'Revolut']

    def read_csv(self, path):
        df =  pd.read_csv(path, index_col=None)
        df = _convert_df_column_names(df)
        return df

    @classmethod
    def factory(cls, source):
        if source == 'Monzo':
            return MonzoFileStatementTransformer()
        elif source == 'HSBC':
            return HSBCFileStatementTransformer()
        elif source == 'Revolut':
            return RevoFileStatementTransformer()
        else:
            raise ValueError(f"Invalid or unknown transaction source name: {source}")



class HSBCFileStatementTransformer(StatementTransformer):
    def read_csv(self, path):
        df = pd.read_csv(path, header=None, index_col=None)
        df.columns = ['date', 'description', 'amount']
        return df

    def _transform(self, df_hsbc: pd.DataFrame) -> pd.DataFrame:
        # TODO:v3 make it more readable
        logging.info(f"Transforming HSBC raw transactions ({df_hsbc.shape} shape)")
        df_hsbc = df_hsbc.copy()

        # Add prefix to id
        # df_hsbc['id'] = ('1' + df_hsbc['id'].astype(str)).astype(np.int64)

        # Combine date and time to create datetime
        df_hsbc['datetime'] = pd.to_datetime(df_hsbc['date'], dayfirst=True)

        # Set currency to GBP and amount_cur to amount
        df_hsbc['currency'] = 'GBP'

        df_hsbc['amount'] = df_hsbc['amount'].astype(str).str.replace(',', '').astype(float)
        df_hsbc['amount_cur'] = df_hsbc['amount']
        df_hsbc['description'] = 'bank:HSBC, ' + df_hsbc['description']

        df_hsbc['id'] = list(range(len(df_hsbc)))
        df_hsbc['id'] = df_hsbc.apply(lambda row: transaction_id_formula(row, 'HSBC'), axis=1)

        # Select and rename columns
        df_transformed = df_hsbc[['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description']]
        df_transformed = df_transformed.rename(columns={'id': 'id', 'datetime': 'datetime', 'amount': 'amount',
                                                        'currency': 'currency', 'amount_cur': 'amount_cur',
                                                        'description': 'description'})

        return df_transformed


class MonzoFileStatementTransformer(StatementTransformer):
    def _transform(self, df_monzo: pd.DataFrame) -> pd.DataFrame:  # TODO:v3 make it more readable
        logging.info(f"Transforming Monzo raw transactions ({df_monzo.shape} shape)")
        df_monzo = df_monzo.copy()


        df_monzo['id'] = df_monzo['transaction_id']
        # df_monzo['id'] = ('2' + df_monzo['id'].astype(str)).astype(np.int64)
        try:
            df_monzo['datetime'] = pd.to_datetime(df_monzo['date'], format="%Y-%m-%d") + pd.to_timedelta(
                df_monzo['time'].astype(str))
        except ValueError as ve:
            logging.warning(f"Unexpected date format (%d-%m-%Y) while parsing Monzo file statement: {ve}")
            df_monzo['datetime'] = pd.to_datetime(df_monzo['date'], format="%d/%m/%Y") + pd.to_timedelta(
                df_monzo['time'].astype(str))
        # df_monzo['datetime'] = pd.to_datetime(df_monzo['date'], format="%d/%m/%Y") + pd.to_timedelta(
        #     df_monzo['time'].astype(str))
        df_monzo['currency'] = df_monzo['local_currency']
        df_monzo['amount'] = df_monzo['amount'].astype(float)
        df_monzo['amount_cur'] = df_monzo['local_amount'].astype(float)


        df_monzo['id'] = df_monzo.apply(lambda row: transaction_id_formula(row, 'Monzo'), axis=1)

        # Concatenate columns to create description
        cols_to_concat = ['type', 'name', 'emoji', 'category', 'notes_and_#tags', 'address', 'receipt',
                          'description',
                          'category_split', 'money_out', 'money_in']

        df_transformed = df_monzo[['id', 'datetime', 'amount', 'currency', 'amount_cur']].copy()

        df_transformed['description'] = df_monzo[cols_to_concat].apply(
            lambda x: ', '.join(
                [col + ": " + (str(x[col]) if pd.notnull(x[col]) else 'none') for col in cols_to_concat]),
            axis=1
        )
        df_transformed.loc[:, 'description'] = 'bank:Monzo, ' + df_transformed['description']

        df_transformed = df_transformed.reindex(
            columns=['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description'])

        return df_transformed


class RevoFileStatementTransformer(StatementTransformer):
    def __init__(self, currency_converter=None):
        self._currency_converter = currency_converter if currency_converter is not None else currencies.FixedRateCurrencyConverter()

    def convert_amounts(self, amount_ser, currency_ser, datetime_ser):
        return [self._currency_converter.amount_to_gbp(amount, currency, date)
                for amount, currency, date
                in zip(amount_ser, currency_ser, datetime_ser)]

    def _transform(self, df_revo: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Transforming Revolut raw transactions ({df_revo.shape} shape)")
        df_revo = df_revo.copy()

        df_revo['id'] = list(range(len(df_revo)))
        df_transformed = pd.DataFrame({'id': ('3' + df_revo['id'].astype(str)).astype(np.int64)})
        df_transformed['datetime'] = pd.to_datetime(df_revo['started_date'], format="%Y-%m-%d %H:%M:%S")
        df_transformed['amount'] = self.convert_amounts(df_revo['amount'], df_revo['currency'],
                                                        df_transformed['datetime'].dt.date)
        df_transformed['currency'] = df_revo['currency']
        df_transformed['amount_cur'] = df_revo['amount']

        df_transformed['id'] = df_transformed.apply(lambda row: transaction_id_formula(row, 'Revolut'), axis=1)

        # Concatenate columns to create description
        cols_to_concat = ['type', 'product', 'completed_date', 'description', 'fee', 'state', 'balance']
        df_transformed['description'] = df_revo[cols_to_concat].apply(
            lambda x: ', '.join([f"{col}: {x[col]}" for col in cols_to_concat if pd.notnull(x[col])]),
            axis=1
        )
        df_transformed['description'] = 'bank:Revolut, ' + df_transformed['description']

        return df_transformed