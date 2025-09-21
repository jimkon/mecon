import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from mecon.data.data_management import CachedFileDataManager
from mecon.etl.dataset import Dataset


@pytest.fixture
def dataset_copy(tmp_path):
    source_dataset = Path(__file__).parent / "test_datasets" / "test_statements_and_tags"
    dataset_path = tmp_path / "test_statements_and_tags"
    shutil.copytree(source_dataset, dataset_path)

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
        "trading212": {
            "api_key": "dummy",
            "mode": "demo",
        },
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
    (tmp_path / "credentials.json").write_text(json.dumps(credentials))

    dataset = Dataset.from_dirpath(dataset_path)
    return dataset, dataset_path


def test_cached_file_data_manager_creates_expected_files(dataset_copy):
    dataset, dataset_path = dataset_copy
    manager = CachedFileDataManager(dataset)

    manager.reset_transactions()
    manager.reset_transaction_tags()

    current_data = dataset_path / "data" / "current"
    transactions_path = current_data / "transactions.csv"
    tags_metadata_path = current_data / "tags_metadata.csv"

    assert transactions_path.exists()
    assert tags_metadata_path.exists()

    actual_transactions = pd.read_csv(transactions_path)
    expected_transactions = pd.DataFrame([
        {
            "id": "TLHSBC-d20240301t080000-ap300050-ihsbc_001",
            "datetime": "2024-03-01 08:00:00",
            "amount": 3000.5,
            "currency": "GBP",
            "amount_cur": 3000.5,
            "description": "bank:TLHSBC, Salary payment from Deloitte other_fields:{'transaction_category': 'income', 'transaction_type': 'Credit'}",
            "tags": "All,Friday,HSBC,Money In,Morning,Source TrueLayerHSBC,Weekday,£",
        },
        {
            "id": "INVENG-d20240301t090000-ap25000-id.0",
            "datetime": "2024-03-01 09:00:00",
            "amount": 250.0,
            "currency": "GBP",
            "amount_cur": 250.0,
            "description": "bank:INVENG, Monthly deposit from HSBC",
            "tags": "All,Friday,InvestEngine,Money In,Morning,Source INVENG,Weekday,£",
        },
        {
            "id": "MonzoAPI-d20240302t081500-ap250000-id.tx_0001",
            "datetime": "2024-03-02 08:15:00",
            "amount": 2500.0,
            "currency": "GBP",
            "amount_cur": 2500.0,
            "description": "bank:MonzoAPI, other_fields: {category: income, description: Monthly salary payment from ITV}",
            "tags": "All,Money In,Monzo,Morning,Saturday,Source MonzoAPI,Weekend,£",
        },
        {
            "id": "TLHSBC-d20240303t103000-an120000-ihsbc_002",
            "datetime": "2024-03-03 10:30:00",
            "amount": -1200.0,
            "currency": "GBP",
            "amount_cur": -1200.0,
            "description": "bank:TLHSBC, Rent payment to Landlord other_fields:{'transaction_category': 'rent', 'transaction_type': 'Debit'}",
            "tags": "All,HSBC,Money Out,Morning,Source TrueLayerHSBC,Sunday,Weekend,£",
        },
        {
            "id": "MonzoAPI-d20240304t074500-an320-id.tx_0002",
            "datetime": "2024-03-04 07:45:00",
            "amount": -3.2,
            "currency": "GBP",
            "amount_cur": -3.2,
            "description": "bank:MonzoAPI, other_fields: {category: transport, merchant: TfL, description: TFL Travel - Zone 1 to 3}",
            "tags": "All,Commute,Monday,Money Out,Monzo,Night,Source MonzoAPI,Weekday,£",
        },
        {
            "id": "MonzoAPI-d20240305t182000-an4520-id.tx_0003",
            "datetime": "2024-03-05 18:20:00",
            "amount": -45.2,
            "currency": "GBP",
            "amount_cur": -45.2,
            "description": "bank:MonzoAPI, other_fields: {category: shopping, merchant: Tesco, description: Tesco Supermarket groceries}",
            "tags": "Afternoon,All,Money Out,Monzo,Source MonzoAPI,Tuesday,Weekday,£",
        },
        {
            "id": "TLHSBC-d20240306t071500-an4560-ihsbc_003",
            "datetime": "2024-03-06 07:15:00",
            "amount": -45.6,
            "currency": "GBP",
            "amount_cur": -45.6,
            "description": "bank:TLHSBC, TFL Travelcard renewal other_fields:{'transaction_category': 'transport', 'transaction_type': 'Debit'}",
            "tags": "All,Commute,HSBC,Money Out,Night,Source TrueLayerHSBC,Wednesday,Weekday,£",
        },
        {
            "id": "INVENG-d20240308t153000-an7550-id.1",
            "datetime": "2024-03-08 15:30:00",
            "amount": -75.5,
            "currency": "GBP",
            "amount_cur": -75.5,
            "description": "bank:INVENG, ETF purchase VWRL",
            "tags": "Afternoon,All,Friday,InvestEngine,Money Out,Source INVENG,Weekday,£",
        },
        {
            "id": "TRD212-d20240311t100500-an15000-i9001",
            "datetime": "2024-03-11 10:05:00",
            "amount": -150.0,
            "currency": "GBP",
            "amount_cur": -150.0,
            "description": "bank:Trading212API,  other_fields:{'action': 'Buy', 'currency_(total)': 'GBP', 'price': 150.0, 'quantity': 1, 'ticker': 'VUSA'}",
            "tags": "All,Monday,Money Out,Morning,Source Trading212API,Trading212,Weekday,£",
        },
        {
            "id": "INVENG-d20240319t101500-ap1230-id.2",
            "datetime": "2024-03-19 10:15:00",
            "amount": 12.3,
            "currency": "GBP",
            "amount_cur": 12.3,
            "description": "bank:INVENG, Dividend payout VWRL",
            "tags": "All,InvestEngine,Money In,Morning,Source INVENG,Tuesday,Weekday,£",
        },
        {
            "id": "TRD212-d20240325t073000-ap525-i9002",
            "datetime": "2024-03-25 07:30:00",
            "amount": 5.25,
            "currency": "GBP",
            "amount_cur": 5.25,
            "description": "bank:Trading212API,  other_fields:{'action': 'Dividend', 'currency_(total)': 'GBP', 'price': 5.25, 'quantity': 0, 'ticker': 'VUSA'}",
            "tags": "All,Monday,Money In,Night,Source Trading212API,Trading212,Weekday,£",
        },
    ])

    pd.testing.assert_frame_equal(
        actual_transactions.sort_values("id").reset_index(drop=True),
        expected_transactions.sort_values("id").reset_index(drop=True),
        check_dtype=False,
    )

    actual_tags_metadata = pd.read_csv(tags_metadata_path)
    assert "date_modified" in actual_tags_metadata.columns
    assert actual_tags_metadata["date_modified"].notna().all()

    actual_tags_summary = actual_tags_metadata.drop(columns=["date_modified"])
    expected_tags_summary = pd.DataFrame([
        {"name": "All", "count": 11, "total_money_in": 5768.05, "total_money_out": 1519.5},
        {"name": "£", "count": 11, "total_money_in": 5768.05, "total_money_out": 1519.5},
        {"name": "Weekday", "count": 9, "total_money_in": 3268.05, "total_money_out": 319.5},
        {"name": "Morning", "count": 6, "total_money_in": 5762.8, "total_money_out": 1350.0},
        {"name": "Money Out", "count": 6, "total_money_in": 0.0, "total_money_out": 1519.5},
        {"name": "Money In", "count": 5, "total_money_in": 5768.05, "total_money_out": 0.0},
        {"name": "Friday", "count": 3, "total_money_in": 3250.5, "total_money_out": 75.5},
        {"name": "HSBC", "count": 3, "total_money_in": 3000.5, "total_money_out": 1245.6},
        {"name": "Source TrueLayerHSBC", "count": 3, "total_money_in": 3000.5, "total_money_out": 1245.6},
        {"name": "InvestEngine", "count": 3, "total_money_in": 262.3, "total_money_out": 75.5},
        {"name": "Source INVENG", "count": 3, "total_money_in": 262.3, "total_money_out": 75.5},
        {"name": "Monzo", "count": 3, "total_money_in": 2500.0, "total_money_out": 48.4},
        {"name": "Source MonzoAPI", "count": 3, "total_money_in": 2500.0, "total_money_out": 48.4},
        {"name": "Monday", "count": 3, "total_money_in": 5.25, "total_money_out": 153.2},
        {"name": "Night", "count": 3, "total_money_in": 5.25, "total_money_out": 48.8},
        {"name": "Weekend", "count": 2, "total_money_in": 2500.0, "total_money_out": 1200.0},
        {"name": "Commute", "count": 2, "total_money_in": 0.0, "total_money_out": 48.8},
        {"name": "Afternoon", "count": 2, "total_money_in": 0.0, "total_money_out": 120.7},
        {"name": "Tuesday", "count": 2, "total_money_in": 12.3, "total_money_out": 45.2},
        {"name": "Source Trading212API", "count": 2, "total_money_in": 5.25, "total_money_out": 150.0},
        {"name": "Trading212", "count": 2, "total_money_in": 5.25, "total_money_out": 150.0},
        {"name": "Saturday", "count": 1, "total_money_in": 2500.0, "total_money_out": 0.0},
        {"name": "Sunday", "count": 1, "total_money_in": 0.0, "total_money_out": 1200.0},
        {"name": "Wednesday", "count": 1, "total_money_in": 0.0, "total_money_out": 45.6},
    ])

    pd.testing.assert_frame_equal(
        actual_tags_summary.sort_values("name").reset_index(drop=True),
        expected_tags_summary.sort_values("name").reset_index(drop=True),
        check_dtype=False,
        check_exact=False,
        atol=1e-6,
    )
