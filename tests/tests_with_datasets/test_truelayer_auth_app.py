import importlib
import unittest

import pytest


@pytest.fixture()
def truelayer_auth_app_module(dataset_manager):
    module = importlib.import_module("services.auth.truelayer_auth_app")
    module = importlib.reload(module)
    module._source_current_data_info_cache.clear()
    return module


def test_get_accounts_info_from_creds_returns_copy(truelayer_auth_app_module):
    module = truelayer_auth_app_module
    source = "ob-hsbc"

    accounts = module.get_accounts_info_from_creds(source)
    assert accounts, "Expected TrueLayer accounts data to be present"

    expected_len = len(module.creds["truelayer"]["sources"][source]["accounts"])
    accounts.append({"account_id": "dummy"})

    assert len(module.creds["truelayer"]["sources"][source]["accounts"]) == expected_len


def test_get_account_ids_for_source_matches_creds(truelayer_auth_app_module):
    module = truelayer_auth_app_module
    source = "ob-hsbc"

    expected_ids = [
        account["account_id"]
        for account in module.creds["truelayer"]["sources"][source]["accounts"]
    ]

    assert module.get_account_ids_for_source(source) == expected_ids


def test_source_current_data_info_uses_dataset_transactions(truelayer_auth_app_module):
    module = truelayer_auth_app_module
    source = "ob-hsbc"
    expected_account_id = "d4aa58643585c1e3a5f7d3e24cf5e829"

    info = module.source_current_data_info(source)

    assert expected_account_id in info
    hsbc_info = info[expected_account_id]

    assert hsbc_info["account_dir"] == "TrueLayerHSBC"
    assert hsbc_info["start_date"] == "2024-03-01"
    assert hsbc_info["end_date"] == "2024-03-06"
    assert hsbc_info["days_included"] == 5
    assert isinstance(hsbc_info["days_missing_from_today"], int)
    assert hsbc_info["days_missing_from_today"] >= 0


def test_source_current_data_info_cached_reuses_cache(monkeypatch, truelayer_auth_app_module):
    module = truelayer_auth_app_module
    module._source_current_data_info_cache.clear()

    calls = []

    def fake_source_info(source):
        calls.append(source)
        return {"dummy": {}}

    monkeypatch.setattr(module, "source_current_data_info", fake_source_info)

    first = module.source_current_data_info_cached("ob-hsbc")
    second = module.source_current_data_info_cached("ob-hsbc")

    assert first == {"dummy": {}}
    assert first is second
    assert calls == ["ob-hsbc"]


def test_format_helpers_include_account_data(truelayer_auth_app_module):
    module = truelayer_auth_app_module
    source = "ob-hsbc"

    accounts_html = module.format_accounts_info(
        module.creds["truelayer"]["sources"][source]["accounts"]
    )
    assert "d4aa58643585c1e3a5f7d3e24cf5e829" in accounts_html

    module._source_current_data_info_cache.clear()
    data_html = module.format_data_info(source)
    assert "TrueLayerHSBC" in data_html
    assert "2024-03-01" in data_html
    assert "2024-03-06" in data_html


if __name__ == '__main__':
    unittest.main()