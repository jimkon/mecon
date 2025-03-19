import abc
import logging

import numpy as np
import pandas as pd

from mecon.utils import currencies
from mecon.utils.dataframe_transformers import DataframeTransformer


# TODO remove
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
    source_abr = StatementTransformer.factory(source).source_name_abr
    # if source == 'Monzo':
    #     source_abr = 'MZN'
    # elif source == 'HSBC':
    #     source_abr = 'HSBC'
    # elif source == 'Revolut':
    #     source_abr = 'RVLT'
    # elif source == 'INVENG':
    #     source_abr = 'INVENG'
    # elif source == 'HSBCSVR':
    #     source_abr = 'HSBCSVR'
    # else:
    #     raise ValueError(f"Invalid or unknown transaction source name: {source}")

    datetime_str = transaction['datetime'].strftime("d%Y%m%dt%H%M%S")
    amount_str = f"a{'p' if transaction['amount'] > 0 else 'n'}{int(100 * abs(transaction['amount']))}"
    ordinal_value = f"i{transaction['id']}"  # TODO that can change depending on the dataset. maybe get different counter for each day
    result = f"{source_abr}{datetime_str}{amount_str}{ordinal_value}"
    return result


def _convert_df_column_names(df):
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    return df


# TODO remove
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


# TODO remove
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


# TODO remove
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
    SOURCES = ['Monzo', 'HSBC', 'Revolut', 'INVENG', 'HSBCSVR', 'TRD212']

    def read_df(self, path):
        df = pd.read_csv(path, index_col=None)
        df = _convert_df_column_names(df)
        return df

    @classmethod
    def factory(cls, source):
        if source == MonzoFileStatementTransformer.source_name:
            return MonzoFileStatementTransformer()
        elif source == HSBCFileStatementTransformer.source_name:
            return HSBCFileStatementTransformer()
        elif source == RevoFileStatementTransformer.source_name:
            return RevoFileStatementTransformer()
        elif source == InvestEngineStatementTransformer.source_name:
            return InvestEngineStatementTransformer()
        elif source == HSBCSaverStatementTransformer.source_name:
            return HSBCSaverStatementTransformer()
        elif source == Trading212StatementTransformer.source_name:
            return Trading212StatementTransformer()
        else:
            raise ValueError(f"Invalid or unknown transaction source name: {source}")


class HSBCFileStatementTransformer(StatementTransformer):
    source_name = 'HSBC'
    source_name_abr = 'HSBC'

    def read_df(self, path):
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
        df_hsbc['description'] = f'bank:{self.source_name}, ' + df_hsbc['description']

        df_hsbc['id'] = list(range(len(df_hsbc)))
        df_hsbc['id'] = df_hsbc.apply(lambda row: transaction_id_formula(row, self.source_name), axis=1)

        # Select and rename columns
        df_transformed = df_hsbc[['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description']]
        df_transformed = df_transformed.rename(columns={'id': 'id', 'datetime': 'datetime', 'amount': 'amount',
                                                        'currency': 'currency', 'amount_cur': 'amount_cur',
                                                        'description': 'description'})

        return df_transformed


class MonzoFileStatementTransformer(StatementTransformer):
    source_name = 'Monzo'
    source_name_abr = 'MZN'

    def _transform(self, df_monzo: pd.DataFrame) -> pd.DataFrame:
        type_1_cols = {'Transaction ID', 'Date', 'Time', 'Type', 'Name', 'Emoji', 'Category', 'Amount',
                       'Currency', 'Local amount', 'Local currency', 'Notes and #tags', 'Address',
                       'Receipt', 'Category split', 'Description', 'Money In', 'Money Out'}
        type_1_cols = set(map(lambda col: col.lower().replace(' ', '_'), type_1_cols))

        if df_monzo.shape[1] == len(type_1_cols):
            logging.info(f"Type 1: Transforming Monzo transactions ({df_monzo.shape} shape)")
            df_transformed = self._transform_type1(df_monzo)
        else:
            logging.info(f"Type 2: Transforming Monzo transactions ({df_monzo.shape} shape)")
            df_transformed = self._transform_type2(df_monzo)

        return df_transformed

    def _transform_type1(self, df_monzo: pd.DataFrame) -> pd.DataFrame:  # TODO:v3 make it more readable
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

        df_monzo['id'] = df_monzo.apply(lambda row: transaction_id_formula(row, self.source_name), axis=1)

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
        df_transformed.loc[:, 'description'] = f'bank:{self.source_name}, ' + df_transformed['description']

        df_transformed = df_transformed.reindex(
            columns=['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description'])

        return df_transformed

    def _transform_type2(self, df_monzo: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Transforming Monzo raw transactions ({df_monzo.shape} shape)")
        df_monzo = df_monzo.copy()
        # df_monzo['id'] = df_monzo['id']
        df_monzo['datetime'] = pd.to_datetime(df_monzo['created'])
        df_monzo['currency'] = df_monzo['local_currency']
        df_monzo['amount'] = df_monzo['amount'].astype(float) / 100
        df_monzo['amount_cur'] = df_monzo['local_amount'].astype(float) / 100

        df_monzo['id'] = df_monzo.apply(lambda row: transaction_id_formula(row, self.source_name), axis=1)

        current_cols = set(df_monzo.columns)

        cols_to_not_concat = {'id', 'datetime', 'amount', 'currency', 'amount_cur',
                              'local_currency', 'created', 'local_amount'}
        cols_to_concat = current_cols.difference(cols_to_not_concat)

        df_transformed = df_monzo[['id', 'datetime', 'amount', 'currency', 'amount_cur']].copy()

        df_other_cols = df_monzo[list(cols_to_concat)].copy()
        descs = [{k: v for k, v in record.items() if v != 'None'} for record in df_other_cols.fillna(value='None').to_dict('records')] # TODO understand how to treat nan values and replace .fillna(value='None')
        descs_str = [f'bank:{self.source_name}, other_fields: ' + str(_dict).replace("'", "") for _dict in descs]
        df_transformed['description'] = descs_str

        df_transformed = df_transformed.reindex(
            columns=['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description'])

        return df_transformed

if __name__ == '__main__':
    files = ['/Users/wimpole/PycharmProjects/mecon/datasets/shared/data/statements/Monzo/Monzo_Transactions_11Oct2018-11Oct2024_2024-10-11_224411.csv',
             '/Users/wimpole/PycharmProjects/mecon/services/main_shiny/Monzo_transactions.csv']
    for file in files:
        df = MonzoFileStatementTransformer().read_df(file)
        df_trans = MonzoFileStatementTransformer().transform(df)

class RevoFileStatementTransformer(StatementTransformer):
    source_name = 'Revolut'
    source_name_abr = 'RVLT'

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

        df_transformed['id'] = df_transformed.apply(lambda row: transaction_id_formula(row, self.source_name), axis=1)

        # Concatenate columns to create description
        cols_to_concat = ['type', 'product', 'completed_date', 'description', 'fee', 'state', 'balance']
        df_transformed['description'] = df_revo[cols_to_concat].apply(
            lambda x: ', '.join([f"{col}: {x[col]}" for col in cols_to_concat if pd.notnull(x[col])]),
            axis=1
        )
        df_transformed['description'] = f'bank:{self.source_name}, ' + df_transformed['description']

        return df_transformed


class InvestEngineStatementTransformer(StatementTransformer):
    source_name = 'INVENG'
    source_name_abr = 'INVENG'

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Transforming InvestEngineer raw transactions ({df.shape} shape)")
        df = df.copy()

        df['id'] = list(range(len(df)))
        df['datetime'] = pd.to_datetime(df['datetime'], format="%d/%m/%Y %H:%M:%S")
        df['amount_cur'] = df['amount']
        df['description'] = df['description'].apply(lambda x: f'bank:{self.source_name}, ' + x)

        df['id'] = df.apply(lambda row: transaction_id_formula(row, self.source_name), axis=1)

        df_final = df[['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description']]

        return df_final


class HSBCSaverStatementTransformer(HSBCFileStatementTransformer):
    source_name = 'HSBCSVR'
    source_name_abr = 'HSBCSVR'


class Trading212StatementTransformer(StatementTransformer):
    source_name = 'TRD212'
    source_name_abr = 'TRD212'

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Transforming Trading212 raw transactions ({df.shape} shape)")
        df = df.copy()

        df['datetime'] = pd.to_datetime(df['time'].apply(lambda s: s[:19]), format="%Y-%m-%d %H:%M:%S")
        df['amount'] = df['total']
        df['amount_cur'] = df['amount']
        df['currency'] = df['currency_(total)']

        df['raw_id'] = df['id']
        del df['id']

        cols_to_concat = ['action', 'notes', 'raw_id']
        df['description'] = df[cols_to_concat].apply(
            lambda x: ', '.join([f"{col}: {x[col]}" for col in cols_to_concat if pd.notnull(x[col])]),
            axis=1
        )

        df['description'] = df['description'].apply(lambda x: f'bank:{self.source_name}, ' + x)

        df['id'] = list(range(len(df)))
        df['id'] = df.apply(lambda row: transaction_id_formula(row, self.source_name), axis=1)

        df_final = df[['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description']]

        return df_final
