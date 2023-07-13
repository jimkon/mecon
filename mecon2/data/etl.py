import numpy as np
import pandas as pd

from mecon2.utils.currency import currency_rate_function


class HSBCTransformer:
    def transform(self, df_hsbc: pd.DataFrame) -> pd.DataFrame:  # TODO make it more readable
        # Add prefix to id
        df_hsbc['id'] = ('1' + df_hsbc['id'].astype(str)).astype(np.int64)

        # Combine date and time to create datetime
        df_hsbc['datetime'] = pd.to_datetime(df_hsbc['date'], format="%d/%m/%Y") + pd.Timedelta('00:00:00')

        # Set currency to GBP and amount_cur to amount
        df_hsbc['currency'] = 'GBP'
        df_hsbc['amount_cur'] = df_hsbc['amount']

        # Select and rename columns
        df_transformed = df_hsbc[['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description']]
        df_transformed = df_transformed.rename(columns={'id': 'id', 'datetime': 'datetime', 'amount': 'amount',
                                                        'currency': 'currency', 'amount_cur': 'amount_cur',
                                                        'description': 'description'})

        return df_transformed


class MonzoTransformer:
    def transform(self, df_monzo: pd.DataFrame) -> pd.DataFrame:  # TODO make it more readable
        df_monzo['id'] = ('2' + df_monzo['id'].astype(str)).astype(np.int64)
        df_monzo['datetime'] = pd.to_datetime(df_monzo['date'], format="%d/%m/%Y") + pd.to_timedelta(df_monzo['time'].astype(str))
        df_monzo['currency'] = df_monzo['local_currency']
        df_monzo['amount_cur'] = df_monzo['local_amount']

        # Concatenate columns to create description
        cols_to_concat = ['transaction_type', 'name', 'emoji', 'category', 'notes_tags', 'address', 'receipt', 'description',
                          'category_split', 'money_out', 'money_in']

        df_transformed = df_monzo[['id', 'datetime', 'amount', 'currency', 'amount_cur']]

        df_transformed['description'] = df_monzo[cols_to_concat].apply(
            lambda x: ', '.join([col+": "+(str(x[col]) if pd.notnull(x[col]) else 'none') for col in cols_to_concat]),
            axis=1
        )

        df_transformed = df_transformed.reindex(columns=['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description'])

        return df_transformed


class RevoTransformer:
    def transform(self, df_revo: pd.DataFrame) -> pd.DataFrame:
        df_transformed = pd.DataFrame({'id': ('3' + df_revo['id'].astype(str)).astype(np.int64)})
        df_transformed['datetime'] = pd.to_datetime(df_revo['start_date'], format="%Y-%m-%d %H:%M:%S")
        currency_rates = df_revo['currency'].apply(lambda curr: currency_rate_function(curr)) # TODO find why it doesn't work without the lambda
        df_transformed['amount'] = df_revo['amount'] / currency_rates
        df_transformed['currency'] = df_revo['currency']
        df_transformed['amount_cur'] = df_revo['amount']

        # Concatenate columns to create description
        cols_to_concat = ['type', 'product', 'completed_date', 'description', 'fee', 'state', 'balance']
        df_transformed['description'] = df_revo[cols_to_concat].apply(
            lambda x: ', '.join([f"{col}: {x[col]}" for col in cols_to_concat if pd.notnull(x[col])]),
            axis=1
        )

        # df_transformed = df_revo[['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description']]
        # df_transformed = df_transformed.reindex(
        #     columns=['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description'])

        return df_transformed
