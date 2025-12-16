import pandas as pd

from mecon.etl.transformers import MonzoAPIFileStatementTransformer, TrueLayerStatementTransformer


def test_monzoapi_transformer_strips_timezone():
    df = pd.DataFrame(
        {
            "created": ["2025-01-01T12:30:00+00:00"],
            "id": ["txn_1"],
            "local_currency": ["GBP"],
            "amount": [100],
            "local_amount": [100],
            "extra": ["value"],
        }
    )

    transformed = MonzoAPIFileStatementTransformer().transform(df)

    assert transformed["datetime"].apply(lambda dt: dt.tzinfo is None).all()


def test_truelayer_transformer_strips_timezone():
    df = pd.DataFrame(
        {
            "transaction_id": ["abc"],
            "timestamp": ["2025-02-01T10:00:00Z"],
            "amount": [10],
            "currency": ["GBP"],
            "description": ["test"],
            "meta": ["info"],
        }
    )

    transformed = TrueLayerStatementTransformer(source="TrueLayerHSBC").transform(df)

    assert transformed["datetime"].apply(lambda dt: dt.tzinfo is None).all()
