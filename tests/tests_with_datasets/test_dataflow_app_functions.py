from datetime import date

import importlib


def test_create_tagged_transactions_info_dataframe(dataset_manager):
    _, manager, _ = dataset_manager
    from services.main_shiny import dataflow_app as dataflow_app_module

    dataflow_app = importlib.reload(dataflow_app_module)
    df = dataflow_app.create_tagged_transactions_info_dataframe(manager)

    assert list(df.columns) == ['tag', 'transaction_count', 'min_date', 'max_date']
    assert not df.empty

    commute_row = df[df['tag'] == 'Commute'].iloc[0]
    assert commute_row['transaction_count'] == 2
    assert commute_row['min_date'] == date(2024, 3, 4)
    assert commute_row['max_date'] == date(2024, 3, 6)

    assert (df['min_date'] <= df['max_date']).all()
