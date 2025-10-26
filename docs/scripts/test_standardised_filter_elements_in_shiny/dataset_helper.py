"""Helpers for building temporary datasets used in tests or manual checks."""

from __future__ import annotations

import datetime
import json
import tempfile
from pathlib import Path
from typing import Iterable

import pandas as pd

from mecon.etl.account_statements import ACCOUNT_STATEMENT_SOURCE_MAPPING

__all__ = ["create_manual_check_dataset"]


_TRANSACTIONS_DATA: list[tuple[str, str, float, str, float, str, str]] = [
    (
        "TLHSBC-d20240301t080000-ap300050-ihsbc_001",
        '2025-10-01 08:00:00',
        3000.5,
        "GBP",
        3000.5,
        "bank:TLHSBC, Salary payment from Deloitte other_fields:{'transaction_category': 'income', 'transaction_type': 'Credit'}",
        "All,Friday,HSBC,Money In,Morning,Source TrueLayerHSBC,Weekday,£",
    ),
    (
        "INVENG-d20240301t090000-ap25000-id.0",
        '2025-10-01 09:00:00',
        250.0,
        "GBP",
        250.0,
        "bank:INVENG, Monthly deposit from HSBC",
        "All,Friday,InvestEngine,Money In,Morning,Source INVENG,Weekday,£",
    ),
    (
        "MonzoAPI-d20240302t081500-ap250000-id.tx_0001",
        '2025-10-02 08:15:00',
        2500.0,
        "GBP",
        2500.0,
        "bank:MonzoAPI, other_fields: {category: income, description: Monthly salary payment from ITV}",
        "All,Money In,Monzo,Morning,Saturday,Source MonzoAPI,Weekend,£",
    ),
    (
        "TLHSBC-d20240303t103000-an120000-ihsbc_002",
        '2025-10-03 10:30:00',
        -1200.0,
        "GBP",
        -1200.0,
        "bank:TLHSBC, Rent payment to Landlord other_fields:{'transaction_category': 'rent', 'transaction_type': 'Debit'}",
        "All,HSBC,Money Out,Morning,Source TrueLayerHSBC,Sunday,Weekend,£",
    ),
    (
        "MonzoAPI-d20240304t074500-an320-id.tx_0002",
        '2025-10-04 07:45:00',
        -3.2,
        "GBP",
        -3.2,
        "bank:MonzoAPI, other_fields: {category: transport, merchant: TfL, description: TFL Travel - Zone 1 to 3}",
        "All,Commute,Monday,Money Out,Monzo,Night,Source MonzoAPI,Weekday,£",
    ),
    (
        "MonzoAPI-d20240305t182000-an4520-id.tx_0003",
        '2025-10-05 18:20:00',
        -45.2,
        "GBP",
        -45.2,
        "bank:MonzoAPI, other_fields: {category: shopping, merchant: Tesco, description: Tesco Supermarket groceries}",
        "Afternoon,All,Money Out,Monzo,Source MonzoAPI,Tuesday,Weekday,£",
    ),
    (
        "TLHSBC-d20240306t071500-an4560-ihsbc_003",
        '2025-10-06 07:15:00',
        -45.6,
        "GBP",
        -45.6,
        "bank:TLHSBC, TFL Travelcard renewal other_fields:{'transaction_category': 'transport', 'transaction_type': 'Debit'}",
        "All,Commute,HSBC,Money Out,Night,Source TrueLayerHSBC,Wednesday,Weekday,£",
    ),
    (
        "INVENG-d20240308t153000-an7550-id.1",
        '2025-10-08 15:30:00',
        -75.5,
        "GBP",
        -75.5,
        "bank:INVENG, ETF purchase VWRL",
        "Afternoon,All,Friday,InvestEngine,Money Out,Source INVENG,Weekday,£",
    ),
    (
        "TRD212-d20240311t100500-an15000-i9001",
        '2025-10-11 10:05:00',
        -150.0,
        "GBP",
        -150.0,
        "bank:Trading212API,  other_fields:{'action': 'Buy', 'currency_(total)': 'GBP', 'price': 150.0, 'quantity': 1, 'ticker': 'VUSA'}",
        "All,Monday,Money Out,Morning,Source Trading212API,Trading212,Weekday,£",
    ),
    (
        "INVENG-d20240319t101500-ap1230-id.2",
        '2025-10-19 10:15:00',
        12.3,
        "GBP",
        12.3,
        "bank:INVENG, Dividend payout VWRL",
        "All,InvestEngine,Money In,Morning,Source INVENG,Tuesday,Weekday,£",
    ),
    (
        "TRD212-d20240325t073000-ap525-i9002",
        '2025-10-25 07:30:00',
        5.25,
        "GBP",
        5.25,
        "bank:Trading212API,  other_fields:{'action': 'Dividend', 'currency_(total)': 'GBP', 'price': 5.25, 'quantity': 0, 'ticker': 'VUSA'}",
        "All,Monday,Money In,Night,Source Trading212API,Trading212,Weekday,£",
    ),
]


def _write_transactions_csv(path: Path, rows: Iterable[tuple[str, int, float, str, float, str, str]]) -> None:
    df = pd.DataFrame(
        rows,
        columns=[
            "id",
            "datetime",
            "amount",
            "currency",
            "amount_cur",
            "description",
            "tags",
        ],
    )
    today = datetime.datetime.today()
    df["datetime"] = pd.to_datetime(df["datetime"], unit="ns").dt.strftime("%Y-%m-%d %H:%M:%S")
    df['datetime'] = df['datetime'].apply(lambda dt: dt.replace('2025-10', f"{today.year}-{today.month}"))
    df.to_csv(path, index=False)


def _write_tags_csv(path: Path) -> None:
    custom_tags = pd.DataFrame(
        [
            {
                "name": "Commute",
                "conditions_json": json.dumps([{"description.lower": {"contains": "tfl"}}]),
                "date_created": "2024-03-01 00:00:00",
            }
        ]
    )
    custom_tags.to_csv(path, index=False)


def _write_empty_tags_metadata(path: Path) -> None:
    pd.DataFrame(
        columns=["name", "count", "total_money_in", "total_money_out", "date_modified"]
    ).to_csv(path, index=False)


def _write_settings(path: Path, dataset_name: str) -> None:
    settings = {
        "CURRENT_DATASET": dataset_name,
        "sources": {
            "HSBC": False,
            "HSBCSVR": False,
            "INVENG": True,
            "Monzo": False,
            "MonzoAPI": True,
            "Revolut": False,
            "Trading212API": True,
            "TRD212": False,
            "TRD212_CASH_ISA": False,
            "TrueLayerHSBC": True,
            "TrueLayerHSBCSaver": False,
            "TrueLayerRevolutEUR": False,
            "TrueLayerRevolutGBP": False,
            "TrueLayerRevolutHUF": False,
            "TrueLayerRevolutRON": False,
        },
    }
    path.write_text(json.dumps(settings, indent=4, sort_keys=True))


def _write_credentials(path: Path) -> None:
    credentials = {
        "truelayer": {
            "client_id": "dummy",
            "client_secret": "dummy",
            "redirect_uri": "https://example.com/callback",
            "sources": {
                "ob-hsbc": {
                    "token": {
                        "access_token": "token",
                        "refresh_token": "refresh",
                        "expires_at": "1970-01-01T00:00:00Z",
                        "fetched_at": "1970-01-01T00:00:00Z",
                    }
                }
            },
        },
        "trading212": {"api_key": "dummy", "mode": "demo"},
        "monzo-api": {
            "client_id": "dummy",
            "client_secret": "dummy",
            "redirect_url": "https://example.com/callback",
            "token": {
                "access_token": "token",
                "expiry": 0,
                "refresh_token": "refresh",
            },
        },
    }
    path.write_text(json.dumps(credentials, indent=4, sort_keys=True))


def create_manual_check_dataset(dataset_name: str = "manual_check_dataset") -> Path:
    """Create a fresh dataset folder backed by temporary storage.

    The dataset contains a transactions CSV with a curated set of transactions and a
    ``tags.csv`` file with a single custom tag (``Commute``). Built-in tags are
    automatically provided by :class:`~mecon.data.data_management.CachedFileDataManager`.

    Args:
        dataset_name: Name of the dataset directory to create inside the temporary root.

    Returns:
        Path to the dataset directory. The caller is responsible for cleaning up the
        dataset by removing ``path.parent`` when finished.
    """

    temp_root = Path(tempfile.mkdtemp(prefix="mecon_manual_dataset_"))
    dataset_path = temp_root / dataset_name
    current_dir = dataset_path / "data" / "current"
    statements_dir = dataset_path / "data" / "statements"

    current_dir.mkdir(parents=True, exist_ok=True)
    statements_dir.mkdir(parents=True, exist_ok=True)

    for source_dir in ACCOUNT_STATEMENT_SOURCE_MAPPING.keys():
        (statements_dir / source_dir).mkdir(parents=True, exist_ok=True)

    _write_transactions_csv(current_dir / "transactions.csv", _TRANSACTIONS_DATA)
    _write_tags_csv(current_dir / "tags.csv")
    _write_empty_tags_metadata(current_dir / "tags_metadata.csv")
    _write_settings(dataset_path / "settings.json", dataset_name)
    _write_credentials(temp_root / "credentials.json")

    return dataset_path
