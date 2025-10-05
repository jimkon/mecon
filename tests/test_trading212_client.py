import datetime as dt
from datetime import timezone
from io import BytesIO
import unittest
from unittest import mock
import sys
import zipfile

import pandas as pd

sys.modules.setdefault("httpx", mock.MagicMock(Client=mock.MagicMock()))

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

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_normalize_since_adds_timezone(self, mock_httpx_client):
        client, _, _ = self._build_client(mock_httpx_client)
        naive = dt.datetime(2023, 1, 1)

        with self.assertLogs(level="INFO") as cm:
            result = client._normalize_since(naive)

        self.assertEqual(result.tzinfo, timezone.utc)
        self.assertIn("Normalizing 'since' parameter", " ".join(cm.output))

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_resolve_skip_ids_force_request(self, mock_httpx_client):
        client, _, _ = self._build_client(mock_httpx_client)

        with self.assertLogs(level="INFO") as cm:
            skip_ids = client._resolve_skip_ids(["1", "2"], force_request=True)

        self.assertEqual(skip_ids, set())
        self.assertIn("force_request=True", " ".join(cm.output))

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_build_chunks_creates_expected_ranges(self, mock_httpx_client):
        client, _, _ = self._build_client(mock_httpx_client)

        since = dt.datetime(2022, 6, 1, tzinfo=timezone.utc)
        now = dt.datetime(2023, 6, 1, tzinfo=timezone.utc)

        chunks = client._build_chunks(since, now)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0][0], since)
        self.assertEqual(chunks[0][1].date(), dt.date(2022, 12, 31))
        self.assertEqual(chunks[1][0], dt.datetime(2023, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(chunks[1][1], now)

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_collect_completed_exports_filters_mismatched(self, mock_httpx_client):
        client, _, _ = self._build_client(mock_httpx_client)

        matching_export = {
            "status": "Completed",
            "downloadLink": "https://example.com/export.zip",
            "reportId": 10,
            "timeFrom": "2023-01-01T00:00:00Z",
            "timeTo": "2023-03-01T00:00:00Z",
            "dataIncluded": {
                "includeTransactions": True,
                "includeOrders": True,
                "includeDividends": True,
                "includeInterest": True,
            },
        }
        mismatched_export = {
            "status": "Completed",
            "downloadLink": "https://example.com/export2.zip",
            "reportId": 11,
            "timeFrom": "2023-01-01T00:00:00Z",
            "timeTo": "2023-02-01T00:00:00Z",
            "dataIncluded": {"includeTransactions": True},
        }
        client.get_exports = mock.MagicMock(return_value=[matching_export, mismatched_export])

        exports = client._collect_completed_exports(
            {
                "includeTransactions": True,
                "includeOrders": True,
                "includeDividends": True,
                "includeInterest": True,
            }
        )

        self.assertEqual(len(exports), 1)
        self.assertEqual(exports[0]["reportId"], 10)

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_plan_exports_reuses_and_requests(self, mock_httpx_client):
        client, _, _ = self._build_client(mock_httpx_client)

        frm = dt.datetime(2023, 1, 1, tzinfo=timezone.utc)
        to = dt.datetime(2023, 6, 1, tzinfo=timezone.utc)
        chunks = [(frm, to)]
        completed = [
            {
                "reportId": "42",
                "downloadLink": "https://example.com/reuse.zip",
                "_from": frm,
                "_to": to,
            }
        ]

        links, need_to_create, reused, skipped = client._plan_exports(
            chunks,
            completed,
            skip_ids=set(),
            today_utc=to.date(),
            force_request=False,
        )

        self.assertEqual(reused, 1)
        self.assertEqual(skipped, 0)
        self.assertEqual(need_to_create, [])
        self.assertEqual(len(links), 1)

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_create_missing_exports_requests_and_appends(self, mock_httpx_client):
        client, _, _ = self._build_client(mock_httpx_client)
        client.request_csv_export = mock.MagicMock(return_value=77)

        frm = dt.datetime(2023, 1, 1, tzinfo=timezone.utc)
        to = dt.datetime(2023, 2, 1, tzinfo=timezone.utc)
        links: list[tuple[int | None, str | None, dt.datetime, dt.datetime]] = []

        created = client._create_missing_exports(
            [(frm, to)],
            {
                "includeTransactions": True,
                "includeOrders": True,
                "includeDividends": True,
                "includeInterest": True,
            },
            post_gap_sec=0,
            links_or_ids=links,
        )

        self.assertEqual(created, [77])
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0][0], 77)

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_wait_for_exports_updates_links(self, mock_httpx_client):
        client, _, _ = self._build_client(mock_httpx_client)

        links = [(123, None, dt.datetime(2023, 1, 1, tzinfo=timezone.utc), dt.datetime(2023, 2, 1, tzinfo=timezone.utc))]
        client.get_exports = mock.MagicMock(return_value=[{"reportId": 123, "status": "Completed", "downloadLink": "https://example.com/file.zip"}])

        with mock.patch("mecon.etl.trading212_client_by_o3._sleep", return_value=None):
            client._wait_for_exports(links, [123], poll_interval_sec=0, timeout_sec=1)

        self.assertEqual(links[0][1], "https://example.com/file.zip")

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_download_export_frames_uses_read_export(self, mock_httpx_client):
        client, _, _ = self._build_client(mock_httpx_client)
        sample_df = pd.DataFrame({"A": [1]})
        client._read_export = mock.MagicMock(return_value=[sample_df])

        frames = client._download_export_frames(
            [(1, "https://example.com/file.csv", dt.datetime(2023, 1, 1, tzinfo=timezone.utc), dt.datetime(2023, 2, 1, tzinfo=timezone.utc))],
            reused_existing=1,
            created_count=0,
            skipped_existing=0,
        )

        self.assertEqual(frames, [sample_df])

    @mock.patch("mecon.etl.trading212_client_by_o3.httpx.Client")
    def test_read_export_handles_zip_payload(self, mock_httpx_client):
        client, _, dl_client = self._build_client(mock_httpx_client)
        response = mock.MagicMock()
        response.raise_for_status = mock.MagicMock()
        response.content = _zip_bytes("Time,Value\n2023-01-02T00:00:00Z,10\n")
        dl_client.get.return_value = response

        frames = client._read_export(
            "https://example.com/export.zip",
            99,
            dt.datetime(2023, 1, 1, tzinfo=timezone.utc),
            dt.datetime(2023, 2, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(len(frames), 1)
        self.assertIn("_file_name", frames[0].columns)



if __name__ == "__main__":
    unittest.main()
