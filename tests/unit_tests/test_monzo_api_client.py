import json
import time
from unittest.mock import MagicMock

import pytest

from mecon.settings import DictFile
from mecon.etl.monzo_api_client import MonzoClient, MonzoCredentialsError
from oauthlib.oauth2.rfc6749.errors import InvalidClientIdError
from monzo.authentication import Authentication


def _make_creds(tmp_path):
    creds_path = tmp_path / "creds.json"
    creds_path.write_text(json.dumps({
        "monzo-api": {
            "client_id": "id",
            "client_secret": "secret",
            "redirect_url": "http://localhost",
            "token": {
                "access_token": "token",
                "expiry": time.time() + 3600,
                "refresh_token": "refresh",
            },
        }
    }))
    return DictFile(creds_path)


def test_refresh_token_failure_raises_credentials_error(tmp_path, monkeypatch):
    creds = _make_creds(tmp_path)
    client = MonzoClient(creds)

    def boom(self):
        raise Exception("boom")

    monkeypatch.setattr(Authentication, "refresh_access", boom)

    with pytest.raises(MonzoCredentialsError):
        client.refresh_token()


def test_download_history_invalid_client_raises_credentials_error(tmp_path, monkeypatch):
    creds = _make_creds(tmp_path)
    client = MonzoClient(creds)

    mock_monzo = MagicMock()
    mock_monzo.get_transactions.side_effect = [InvalidClientIdError("bad"), InvalidClientIdError("bad")]

    monkeypatch.setattr(client, "_new_monzo_client", lambda: mock_monzo)
    monkeypatch.setattr(client, "refresh_token", lambda: None)

    with pytest.raises(MonzoCredentialsError):
        client.download_accounts_transaction_history("acc")
