import importlib
import json
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

sys.modules.setdefault("httpx", mock.MagicMock(Client=mock.MagicMock(), post=mock.MagicMock()))

sys.path.append(
    str(Path(__file__).resolve().parents[2] / "services" / "edit_data")
)

from mecon import config
from mecon.data.data_management import CachedFileDataManager
from mecon.etl.dataset import Dataset


@pytest.fixture
def dataset_manager(tmp_path, monkeypatch):
    datasets_source = (
        Path(__file__).resolve().parents[1]
        / "tests_with_datasets"
        / "datasets"
        / "test_statements_and_tags"
    )

    datasets_root = tmp_path / "datasets_root"
    target_dataset = datasets_root / "test_statements_and_tags"
    shutil.copytree(datasets_source, target_dataset)

    expected_current = datasets_source / "data" / "_expected_current"
    current_dir = target_dataset / "data" / "current"
    shutil.copyfile(expected_current / "transactions.csv", current_dir / "transactions.csv")
    shutil.copyfile(expected_current / "tags_metadata.csv", current_dir / "tags_metadata.csv")
    pd.DataFrame(
        columns=[
            "id",
            "datetime",
            "amount",
            "currency",
            "amount_cur",
            "description",
            "tags",
        ]
    ).to_csv(current_dir / "calc_monitoring.csv", index=False)
    pd.DataFrame(columns=["tag", "type", "in", "out", "alias"]).to_csv(
        current_dir / "op_monitoring.csv", index=False
    )

    (datasets_root / "settings.json").write_text(
        json.dumps({"CURRENT_DATASET": "test_statements_and_tags"})
    )

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
    (datasets_root / "credentials.json").write_text(json.dumps(credentials))

    monkeypatch.setenv("MECON_DATASETS_DIR", str(datasets_root))
    monkeypatch.setattr(config, "DEFAULT_DATASETS_DIR_PATH", datasets_root)

    dataset = Dataset.from_dirpath(target_dataset)
    manager = CachedFileDataManager(dataset)
    return dataset, manager, datasets_root


@pytest.fixture
def shiny_helpers():
    import services.edit_data.edit_tags as edit_tags_module
    import services.edit_data.manual_tagging_app as manual_tagging_module
    import services.edit_data.menu_tags as menu_tags_module
    import services.main_shiny.main_app as main_app_module

    return SimpleNamespace(
        edit_tags=importlib.reload(edit_tags_module),
        manual_tagging=importlib.reload(manual_tagging_module),
        menu_tags=importlib.reload(menu_tags_module),
        main_app=importlib.reload(main_app_module),
    )
