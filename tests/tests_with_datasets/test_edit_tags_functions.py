import json
import pandas as pd

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
