import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from mecon.data.data_management import CachedFileDataManager
from mecon.etl.dataset import Dataset
from mecon.tags import tagging

from services.edit_data import utils


@pytest.fixture()
def data_manager_with_dataset(tmp_path):
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
    manager = CachedFileDataManager(dataset)
    manager.reset_transactions()

    return manager, dataset


def test_save_tag_changes_appends_id_condition(data_manager_with_dataset):
    data_manager, dataset = data_manager_with_dataset
    original_tag = data_manager.get_tag("Commute")
    original_rule_count = len(original_tag.rule.rules)

    new_transaction_id = "MonzoAPI-d20240305t182000-an4520-id.tx_0003"
    changes_per_tag = {"Commute": [new_transaction_id]}

    utils.save_tag_changes(changes_per_tag, data_manager)

    updated_tag = data_manager.get_tag("Commute")
    assert len(updated_tag.rule.rules) == original_rule_count + 1

    id_conditions = [
        condition
        for condition in updated_tag.rule.rules[0].rules
        if isinstance(condition, tagging.Condition) and condition.field == "id"
    ]
    assert id_conditions, "An ID condition should be prepended to the tag rule"
    assert any(
        new_transaction_id in condition.value.split(",")
        for condition in id_conditions
    ), "The new transaction id should be included in the ID condition"

    tags_df = pd.read_csv(dataset.custom_tags_path)
    commute_row = tags_df.loc[tags_df["name"] == "Commute", "conditions_json"].iloc[0]
    commute_json = json.loads(commute_row)
    id_entries = [entry for entry in commute_json if "id" in entry]
    assert id_entries, "Serialized conditions should include the ID rule"
    assert any(
        new_transaction_id in entry["id"]["in_csv"].split(",")
        for entry in id_entries
    )
