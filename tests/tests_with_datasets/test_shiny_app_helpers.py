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
    pd.DataFrame(columns=[
        "id",
        "datetime",
        "amount",
        "currency",
        "amount_cur",
        "description",
        "tags",
    ]).to_csv(current_dir / "calc_monitoring.csv", index=False)
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
def shiny_helpers(dataset_manager):
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


def test_create_markdown_menu_from_links(dataset_manager, shiny_helpers):
    dataset, _, _ = dataset_manager
    links = dataset.settings.get("links", {})
    markdown = shiny_helpers.main_app.create_markdown_menu_from_links(links)
    assert "### Comparisons" in markdown
    assert "### Reports" in markdown
    assert "### Tagging" in markdown
    assert "[Commute]" in markdown


def test_fetch_tag_from_manager(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    tag = shiny_helpers.edit_tags.fetch_tag_from_manager(manager, "Commute")
    assert tag.name == "Commute"


def test_calculate_transactions_for_tag(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    tag = shiny_helpers.edit_tags.fetch_tag_from_manager(manager, "Commute")
    tagged_transactions = shiny_helpers.edit_tags.calculate_transactions_for_tag(manager, tag)
    assert tagged_transactions.size() > 0
    assert any("Commute" in tags for tags in tagged_transactions.tags if tags)


def test_build_new_transactions_and_monitor(dataset_manager, shiny_helpers):
    dataset, manager, _ = dataset_manager
    all_tags = manager.all_tags()
    tag = shiny_helpers.edit_tags.fetch_tag_from_manager(manager, "Commute")
    tag_json = shiny_helpers.edit_tags.serialise_tag_to_json(tag)
    new_transactions, monitor = shiny_helpers.edit_tags.build_new_transactions_and_monitor(
        manager, dataset, all_tags, tag.name, tag_json
    )
    assert new_transactions.size() == manager.get_transactions().size()
    assert monitor is not None


def test_compute_transactions_diff(dataset_manager, shiny_helpers):
    dataset, manager, _ = dataset_manager
    tag = shiny_helpers.edit_tags.fetch_tag_from_manager(manager, "Commute")
    tag_json = shiny_helpers.edit_tags.serialise_tag_to_json(tag)
    new_transactions, _ = shiny_helpers.edit_tags.build_new_transactions_and_monitor(
        manager, dataset, manager.all_tags(), tag.name, tag_json
    )
    diff_transactions = shiny_helpers.edit_tags.compute_transactions_diff(
        manager.get_transactions(), new_transactions, tag.name
    )
    assert diff_transactions.size() == 0


def test_serialise_tag_to_json(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    tag = shiny_helpers.edit_tags.fetch_tag_from_manager(manager, "Commute")
    json_text = shiny_helpers.edit_tags.serialise_tag_to_json(tag)
    assert json.loads(json_text) == tag.rule.to_json()


def test_format_transaction_datetime(shiny_helpers):
    dt = pd.Timestamp("2024-03-01 08:30:00").to_pydatetime()
    formatted = shiny_helpers.edit_tags.format_transaction_datetime(dt)
    assert "2024" in formatted and "08:30:00" in formatted


def test_build_unsaved_warning_changes(shiny_helpers):
    warning = shiny_helpers.edit_tags.build_unsaved_warning("{}", "{\"a\": 1}")
    assert "Warning" in warning


def test_add_ids_to_tag(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    tag = shiny_helpers.edit_tags.fetch_tag_from_manager(manager, "Commute")
    updated_tag = shiny_helpers.edit_tags.add_ids_to_tag(tag, ["sample-id"])
    assert "sample-id" in json.dumps(updated_tag.rule.to_json())


def test_append_condition_to_tag(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    tag = shiny_helpers.edit_tags.fetch_tag_from_manager(manager, "Commute")
    updated_tag = shiny_helpers.edit_tags.append_condition_to_tag(
        tag,
        field="description",
        transformation_key="none",
        compare_key="contains_word",
        value="TFL",
    )
    assert any("TFL" in json.dumps(condition) for condition in updated_tag.rule.to_json())


def test_parse_tag_from_json(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    tag = shiny_helpers.edit_tags.fetch_tag_from_manager(manager, "Commute")
    parsed = shiny_helpers.edit_tags.parse_tag_from_json(
        tag.name, shiny_helpers.edit_tags.serialise_tag_to_json(tag)
    )
    assert parsed.rule.to_json() == tag.rule.to_json()


def test_parse_condition_value(shiny_helpers):
    assert shiny_helpers.edit_tags.parse_condition_value("10") == 10
    assert shiny_helpers.edit_tags.parse_condition_value("10.5") == "10.5"
    assert shiny_helpers.edit_tags.parse_condition_value("abc") == "abc"


# def test_build_tag_choices(dataset_manager, shiny_helpers):
#     _, manager, _ = dataset_manager
#     choices = shiny_helpers.manual_tagging.build_tag_choices(manager.all_tags())
#     assert "Commute" in choices
#
#
# def test_filter_transactions_by_selected_tags(dataset_manager, shiny_helpers):
#     _, manager, _ = dataset_manager
#     transactions = manager.get_transactions()
#     filtered = shiny_helpers.manual_tagging.filter_transactions_by_selected_tags(
#         transactions, ["Commute"]
#     )
#     assert filtered.size() > 0
#     assert all("Commute" in tags for tags in filtered.tags)
#
#
# def test_paginate_transactions_newest(dataset_manager, shiny_helpers):
#     _, manager, _ = dataset_manager
#     transactions = manager.get_transactions()
#     page = shiny_helpers.manual_tagging.paginate_transactions(
#         transactions, "Newest transactions", 0, 2
#     )
#     assert not page.empty
#     assert page.iloc[0]["datetime"] >= page.iloc[-1]["datetime"]
#
#
# def test_build_page_choices_least_tagged(dataset_manager, shiny_helpers):
#     _, manager, _ = dataset_manager
#     label, choices = shiny_helpers.manual_tagging.build_page_choices(
#         manager.get_transactions(), "Least tagged", page_size=2
#     )
#     assert "Choose page" in label
#     assert isinstance(choices, dict) and choices

def test_sanitize_tx_id(shiny_helpers):
    assert shiny_helpers.manual_tagging.sanitize_tx_id('test_id1') == 'test_id1'
    assert shiny_helpers.manual_tagging.sanitize_tx_id('test_id_with_-_in_it') == 'test_id_with__hyphen__in_it'
    assert shiny_helpers.manual_tagging.sanitize_tx_id('test_id_with_._in_it') == 'test_id_with__dot__in_it'

def test_desanitize_tx_id(shiny_helpers):
    assert shiny_helpers.manual_tagging.desanitize_tx_id('test_id1') == 'test_id1'
    assert shiny_helpers.manual_tagging.desanitize_tx_id('test_id_with__hyphen__in_it') == 'test_id_with_-_in_it'
    assert shiny_helpers.manual_tagging.desanitize_tx_id('test_id_with__dot__in_it') == 'test_id_with_._in_it'


def test_construct_amount_str(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    df = manager.get_transactions().dataframe()
    amount_strs = df.apply(shiny_helpers.manual_tagging.construct_amount_str, axis=1).tolist()
    expected_amount_strs = ['⮝ 3000.5 GBP', '⮝ 250.0 GBP', '⮝ 2500.0 GBP', '⮟ 1200.0 GBP', '⮟ 3.2 GBP', '⮟ 45.2 GBP', '⮟ 45.6 GBP', '⮟ 75.5 GBP', '⮟ 150.0 GBP', '⮝ 12.3 GBP', '⮝ 5.2 GBP']
    assert amount_strs == expected_amount_strs


def test_enhance_transactions_df(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    df = manager.get_transactions().dataframe()
    all_tags = {tag.name for tag in manager.all_tags()}
    df_enhanced = shiny_helpers.manual_tagging.enhance_transactions_df(df, all_tags)
    assert len(df_enhanced.columns) == 9
    assert df_enhanced.columns.tolist() == ['Tx_ID', 'amount', 'date', 'time', 'week_id', 'n_tags', 'current_tags', 'add_tags', 'short_desc']


def test_transform_tag_diffs(shiny_helpers):
    example_tag_diffs = [
        {'selectize_id': 'selectize_tx_id1', 'changes': ('tag1',)},
        {'selectize_id': 'selectize_tx_id2', 'changes': ('tag2',)},
        {'selectize_id': 'selectize_tx_id3', 'changes': ('tag3', 'tag4', 'tag1')}]


    expected_changes_per_tag = {
        'tag1': ['tx_id1', 'tx_id3'],
        'tag2': ['tx_id2'],
        'tag3': ['tx_id3'],
        'tag4': ['tx_id3'],
    }

    changes_per_tag = shiny_helpers.manual_tagging.transform_tag_diffs(example_tag_diffs)
    assert changes_per_tag == expected_changes_per_tag


def test_build_tags_table(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    tags_table = shiny_helpers.menu_tags.build_tags_table(manager)
    assert "Name" in tags_table.columns
    assert any(name == "Commute" for name in tags_table["Name"])


def test_create_tag_from_name(shiny_helpers):
    tag = shiny_helpers.menu_tags.create_tag_from_name("Sample")
    assert tag.name == "Sample"


def test_refresh_tag_reactives(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager

    class DummyReactive:
        def __init__(self):
            self.value = None

        def set(self, value):
            self.value = value

    all_tags_reactive = DummyReactive()
    tags_metadata_reactive = DummyReactive()

    shiny_helpers.menu_tags.refresh_tag_reactives(
        manager, all_tags_reactive, tags_metadata_reactive
    )

    assert [tag.name for tag in all_tags_reactive.value] == [
        tag.name for tag in manager.all_tags()
    ]
    pd.testing.assert_frame_equal(
        tags_metadata_reactive.value.reset_index(drop=True),
        manager.get_tags_metadata().reset_index(drop=True),
    )
