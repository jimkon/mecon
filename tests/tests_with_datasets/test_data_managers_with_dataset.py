import json
import re
import shutil
import unittest
from pathlib import Path

import pandas as pd
import pytest

from mecon.data.data_management import CachedFileDataManager
from mecon.etl.dataset import Dataset


_OTHER_FIELDS_PATTERN = re.compile(r"other_fields:\s*\{(?P<content>[^}]*)\}")


def _normalise_other_fields(description: str) -> str:
    match = _OTHER_FIELDS_PATTERN.search(description)
    if not match:
        return description

    inner = match.group("content")
    parts = [part.strip() for part in inner.split(",") if part.strip()]
    sorted_inner = ", ".join(sorted(parts))

    start, end = match.span()
    normalised = f"{description[:start]}other_fields: {{{sorted_inner}}}"
    if end < len(description):
        normalised += description[end:]

    return normalised


@pytest.fixture
def dataset_copy(tmp_path):
    source_dataset = (
        Path(__file__).resolve().parent
        / "datasets"
        / "test_statements_and_tags"
    )
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

    current_data = dataset_path / "data" / "current"
    transactions_path = current_data / "transactions.csv"
    tags_metadata_path = current_data / "tags_metadata.csv"

    _expected_current_data = dataset_path / "data" / "_expected_current"

    assert not transactions_path.exists()
    assert not tags_metadata_path.exists()

    manager.reset_transactions()
    assert transactions_path.exists()

    manager.reset_transaction_tags()
    assert tags_metadata_path.exists()

    actual_transactions = pd.read_csv(transactions_path)
    expected_transactions = pd.read_csv(_expected_current_data / "transactions.csv")

    actual_transactions["description"] = actual_transactions["description"].map(
        _normalise_other_fields
    )
    expected_transactions["description"] = expected_transactions["description"].map(
        _normalise_other_fields
    )

    pd.testing.assert_frame_equal(
        actual_transactions.sort_values("id").reset_index(drop=True),
        expected_transactions.sort_values("id").reset_index(drop=True),
        check_dtype=False,
    )

    actual_tags_metadata = pd.read_csv(tags_metadata_path)
    assert "date_modified" in actual_tags_metadata.columns
    assert actual_tags_metadata["date_modified"].notna().all()

    actual_tags_summary = actual_tags_metadata.drop(columns=["date_modified"])
    expected_tags_summary = pd.read_csv(_expected_current_data / "tags_metadata.csv").drop(columns=["date_modified"])


    pd.testing.assert_frame_equal(
        actual_tags_summary.sort_values("name").reset_index(drop=True),
        expected_tags_summary.sort_values("name").reset_index(drop=True),
        check_dtype=False,
        check_exact=False,
        atol=1e-6,
    )


if __name__ == '__main__':
    unittest.main()
