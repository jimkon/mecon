import pathlib
import uuid

import pandas as pd

from mecon.etl import transformers

if __name__ == '__main__':
    for file_in in pathlib.Path('in').glob('*.csv'):
        df_in = pd.read_csv(file_in, index_col=None, header=None)
        df_in.columns = ['date', 'description', 'amount']

        all_cols = list('timestamp, description, transaction_type, transaction_category, transaction_classification, amount, currency, transaction_id, provider_transaction_id, normalised_provider_transaction_id, meta.provider_reference, meta.provider_category, meta.transaction_type, meta.provider_id'.split(', '))
        df_out = pd.DataFrame({
            'timestamp': pd.to_datetime(df_in['date'], dayfirst=True).apply(lambda dt: dt.isoformat()).astype(str)+'Z',
            'description': df_in['description'],
            'amount': df_in['amount'].apply(lambda s: s.replace(',', '')).astype(float),
            'currency': 'GBP',
        })

        df_out['transaction_id'] = [f"MOCK{uuid.uuid4()}" for _ in range(len(df_out))]
        df_out['transaction_type'] = 'CREDIT'
        df_out['transaction_category'] = 'INTEREST'

        current_cols = df_out.columns
        cols_to_nullify = set(all_cols) - set(current_cols)
        for col in cols_to_nullify:
            df_out[col] = None

        df_final = df_out[all_cols]
        file_out = pathlib.Path('out') / file_in.name
        file_out.parent.mkdir(parents=True, exist_ok=True)
        df_final.to_csv(file_out, index=False)


        transformer = transformers.TrueLayerStatementTransformer('TLHSBC')
        transformed_df = transformer.transform(df_final)
        file_transformed = pathlib.Path('transformed') / file_in.name
        file_transformed.parent.mkdir(parents=True, exist_ok=True)

        transformed_df.to_csv(file_transformed, index=False)
