import datetime as dt
import unittest
from unittest import mock

from mecon.etl.true_layer_client_by_o3 import TrueLayerClient


class DummyDictFile(dict):
    """In-memory DictFile replacement tracking save calls."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.saved = 0

    def save(self):  # pragma: no cover - trivial setter
        self.saved += 1


class TrueLayerClientTests(unittest.TestCase):
    @staticmethod
    def _patch_datetime(fixed_now: dt.datetime):
        class FixedDateTime(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    return fixed_now.replace(tzinfo=None)
                return fixed_now.astimezone(tz)

        return mock.patch("mecon.etl.true_layer_client_by_o3.dt.datetime", FixedDateTime)

    def _base_creds(self, token: dict) -> DummyDictFile:
        return DummyDictFile(
            {
                "truelayer": {
                    "client_id": "client",
                    "client_secret": "secret",
                    "redirect_uri": "https://callback",
                    "sources": {
                        "ob-hsbc": {"token": token},
                    },
                }
            }
        )

    @mock.patch("mecon.etl.true_layer_client_by_o3.httpx.Client")
    @mock.patch("mecon.etl.true_layer_client_by_o3.httpx.post")
    @mock.patch("mecon.etl.true_layer_client_by_o3._utc_now")
    def test_ensure_token_refreshes_when_expired(self, mock_now, mock_post, mock_httpx_client):
        """Expired tokens are refreshed and persisted back to credentials."""

        fixed_now = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
        expired_token = {
            "access_token": "old",
            "refresh_token": "refresh",
            "expires_at": (fixed_now - dt.timedelta(minutes=5)).isoformat(),
            "fetched_at": (fixed_now - dt.timedelta(days=1)).isoformat(),
            "refreshed_at": None,
        }
        creds = self._base_creds(expired_token)

        mock_post.return_value = mock.Mock(
            status_code=200,
            json=mock.Mock(
                return_value={
                    "access_token": "new",
                    "refresh_token": "refresh-new",
                    "expires_in": 3600,
                }
            ),
            text="OK",
        )
        mock_now.return_value = fixed_now
        mock_httpx_client.return_value = mock.Mock()

        with self._patch_datetime(fixed_now):
            client = TrueLayerClient(creds)
            token = client._ensure_token("ob-hsbc")

        mock_post.assert_called_once()
        self.assertEqual(token.access_token, "new")
        saved_token = creds["truelayer"]["sources"]["ob-hsbc"]["token"]
        self.assertEqual(saved_token["access_token"], "new")
        self.assertEqual(saved_token["refresh_token"], "refresh-new")
        self.assertEqual(creds.saved, 1)

    @mock.patch("mecon.etl.true_layer_client_by_o3.httpx.Client")
    def test_get_transactions_passes_filters_and_auth_header(self, mock_httpx_client):
        """The data client should send ISO filters and bearer auth to the API."""

        fixed_now = dt.datetime(2023, 5, 1, tzinfo=dt.timezone.utc)
        fresh_token = {
            "access_token": "token",
            "refresh_token": "refresh",
            "expires_at": (fixed_now + dt.timedelta(hours=1)).isoformat(),
            "fetched_at": (fixed_now - dt.timedelta(minutes=5)).isoformat(),
            "refreshed_at": None,
        }
        creds = self._base_creds(fresh_token)

        http_response = mock.Mock()
        http_response.is_error = False
        http_response.json.return_value = {"results": [{"transaction_id": "t1"}]}
        http_client = mock_httpx_client.return_value
        http_client.request.return_value = http_response

        with self._patch_datetime(fixed_now):
            client = TrueLayerClient(creds)
            rows = client.get_transactions(
                "ob-hsbc",
                "account",
                from_date=dt.date(2023, 4, 1),
                to_date=dt.date(2023, 4, 30),
            )

        self.assertEqual(rows, [{"transaction_id": "t1"}])
        http_client.request.assert_called_once()
        call_args = http_client.request.call_args
        self.assertEqual(call_args.args, ("GET", "/accounts/account/transactions"))
        self.assertEqual(
            call_args.kwargs["params"],
            {"from": "2023-04-01", "to": "2023-04-30"},
        )
        self.assertEqual(
            call_args.kwargs["headers"]["Authorization"],
            "Bearer token",
        )
        self.assertEqual(
            call_args.kwargs["headers"]["accept"],
            "application/json; charset=UTF-8",
        )


if __name__ == "__main__":
    unittest.main()
