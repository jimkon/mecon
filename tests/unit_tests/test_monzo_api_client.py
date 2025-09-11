import json
import tempfile
import time
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from mecon.settings import DictFile
from mecon.etl.monzo_api_client import MonzoClient, MonzoCredentialsError
from mecon.etl.account_statements import MonzoAPIStatements
from oauthlib.oauth2.rfc6749.errors import InvalidClientIdError
from monzo.authentication import Authentication


def _make_creds(tmp_dir):
    creds_path = Path(tmp_dir) / "creds.json"
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


class TestMonzoApiClient(unittest.TestCase):
    def test_refresh_token_failure_raises_credentials_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            creds = _make_creds(tmp_dir)
            client = MonzoClient(creds)

            with patch.object(Authentication, "refresh_access", side_effect=Exception("boom")):
                with self.assertRaises(MonzoCredentialsError):
                    client.refresh_token()

    def test_download_history_invalid_client_raises_credentials_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            creds = _make_creds(tmp_dir)
            client = MonzoClient(creds)

            mock_monzo = MagicMock()
            mock_monzo.get_transactions.side_effect = [InvalidClientIdError("bad"), InvalidClientIdError("bad")]

            with patch.object(client, "_new_monzo_client", return_value=mock_monzo):
                with patch.object(client, "refresh_token", return_value=None):
                    with self.assertRaises(MonzoCredentialsError):
                        client.download_accounts_transaction_history("acc")


class TestMonzoAPIStatements(unittest.TestCase):
    def test_fetch_propagates_credentials_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            mock_client = MagicMock()
            mock_client.download_full_history.side_effect = MonzoCredentialsError("bad")
            source = MonzoAPIStatements(
                working_dir=tmp_dir,
                trans_transformer=MagicMock(),
                api_handler=mock_client,
            )

            with self.assertRaises(MonzoCredentialsError):
                source.fetch()

            self.assertEqual(list(Path(tmp_dir).glob("*.csv")), [])


if __name__ == '__main__':
    unittest.main()
