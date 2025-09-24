import datetime as dt
from datetime import timezone
from io import BytesIO
import unittest
from unittest import mock
import zipfile

import pandas as pd

from mecon.etl.trading212_client_by_o3 import Trading212Client


class DummyDictFile(dict):
    def __init__(self):
        super().__init__({"trading212": {"api_key": "token", "mode": "live"}})

    def save(self):  # pragma: no cover - not used in tests
        pass


def _zip_bytes(csv_text: str) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("export.csv", csv_text)
    return buf.getvalue()


class Trading212ClientTests(unittest.TestCase):
    def _build_client(self, mock_httpx_client):
        api_client = mock.MagicMock()
        dl_client = mock.MagicMock()
        mock_httpx_client.side_effect = [api_client, dl_client]
        client = Trading212Client(DummyDictFile())
        client._client = api_client
        client._dl = dl_client
        return client, api_client, dl_client

    @staticmethod
    def _patch_now(target_date):
        class FixedDateTime(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    return target_date.replace(tzinfo=None)
                return target_date.astimezone(tz)

        return mock.patch("mecon.etl.trading212_client_by_o3.dt.datetime", FixedDateTime)

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_fetch_history_dataframe_reuses_completed_export(self, mock_httpx_client):
        """Reuse an already completed export and avoid requesting new reports."""
        client, _, dl_client = self._build_client(mock_httpx_client)
        client.request_csv_export = mock.MagicMock()

        fixed_now = dt.datetime(2023, 6, 1, tzinfo=timezone.utc)
        existing_export = {
            "status": "Completed",
            "downloadLink": "https://example.com/export.zip",
            "reportId": 99,
            "timeFrom": "2023-01-01T00:00:00Z",
            "timeTo": "2023-06-01T00:00:00Z",
            "dataIncluded": {
                "includeTransactions": True,
                "includeOrders": True,
                "includeDividends": True,
                "includeInterest": True,
            },
        }
        client.get_exports = mock.MagicMock(return_value=[existing_export])

        response = mock.MagicMock()
        response.content = _zip_bytes("Time,Value\n2023-01-02T00:00:00Z,10\n")
        dl_client.get.return_value = response

        with self._patch_now(fixed_now):
            df = client.fetch_history_dataframe(
                since=dt.datetime(2023, 1, 1, tzinfo=timezone.utc)
            )

        client.request_csv_export.assert_not_called()
        dl_client.get.assert_called_once_with("https://example.com/export.zip", timeout=60)
        pd.testing.assert_frame_equal(
            df[["Time", "Value", "_reportId"]],
            pd.DataFrame(
                {
                    "Time": ["2023-01-02T00:00:00Z"],
                    "Value": [10],
                    "_reportId": [99],
                }
            ),
        )

    @mock.patch("mecon.etl.trading212_client_by_o3.time.sleep", return_value=None)
    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_fetch_history_dataframe_requests_missing_exports(self, mock_httpx_client, _sleep):
        """Request a fresh export, poll until ready, and download the new report."""
        client, _, dl_client = self._build_client(mock_httpx_client)
        client.request_csv_export = mock.MagicMock(return_value=321)

        fixed_now = dt.datetime(2023, 6, 1, tzinfo=timezone.utc)
        completed_export = {
            "status": "Completed",
            "downloadLink": "https://example.com/new_export.zip",
            "reportId": 321,
            "timeFrom": "2023-01-01T00:00:00Z",
            "timeTo": "2023-06-01T00:00:00Z",
            "dataIncluded": {
                "includeTransactions": True,
                "includeOrders": True,
                "includeDividends": True,
                "includeInterest": True,
            },
        }
        client.get_exports = mock.MagicMock(
            side_effect=[[], [completed_export], [completed_export]]
        )

        response = mock.MagicMock()
        response.content = _zip_bytes("Time,Value\n2023-05-01T00:00:00Z,5\n")
        dl_client.get.return_value = response

        with self._patch_now(fixed_now):
            df = client.fetch_history_dataframe(
                since=dt.datetime(2023, 1, 1, tzinfo=timezone.utc)
            )

        client.request_csv_export.assert_called_once()
        dl_client.get.assert_called_once_with("https://example.com/new_export.zip", timeout=60)
        pd.testing.assert_frame_equal(
            df[["Time", "Value", "_reportId"]],
            pd.DataFrame(
                {
                    "Time": ["2023-05-01T00:00:00Z"],
                    "Value": [5],
                    "_reportId": [321],
                }
            ),
        )


if __name__ == "__main__":
    unittest.main()
