import datetime as dt
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pandas as pd

from mecon.etl.account_statements import (
    APIAccountStatementsSource,
    TrueLayerStatements,
    Trading212APIStatements,
    MonzoAPIStatements,
)


class DummyTrueLayer(TrueLayerStatements):
    """Minimal TrueLayer source for testing."""

    id = "DUMMYTL"
    dir_name = "DUMMYTL"
    bank = "ob-test"
    account_id = "acc"

    def __init__(self, working_dir: Path, api_handler):
        super().__init__(
            working_dir=working_dir,
            trans_transformer=mock.MagicMock(),
            api_handler=api_handler,
        )


class DummyAPIAccount(APIAccountStatementsSource):
    """Simple source to test fetch_if_needed_and_transform."""

    id = "DUMMY"
    dir_name = "DUMMY"

    @classmethod
    def from_path_and_creds(cls, working_dir: Path, creds):
        return cls(working_dir, mock.MagicMock(), mock.MagicMock())

    def fetch(self, since: dt.datetime | None = None):  # pragma: no cover - mocked
        pass


class FetchImplementationTests(unittest.TestCase):
    def test_truelayer_fetch_passes_from_date(self):
        """Ensure TrueLayer fetch forwards the since date as a date object."""
        api_handler = mock.MagicMock()
        with TemporaryDirectory() as tmpdir:
            source = DummyTrueLayer(Path(tmpdir), api_handler)
            since = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
            with mock.patch(
                "mecon.etl.account_statements.json_to_csv", return_value=pd.DataFrame()
            ):
                source.fetch(since=since)
        api_handler.get_transactions.assert_called_once_with(
            "ob-test", "acc", from_date=since.date()
        )

    def test_trading212_fetch_passes_since_datetime(self):
        """Verify Trading212 fetch passes the datetime and empty skip list through."""
        api_handler = mock.MagicMock()
        api_handler.fetch_history_dataframe.return_value = pd.DataFrame()
        with TemporaryDirectory() as tmpdir:
            source = Trading212APIStatements(
                working_dir=Path(tmpdir),
                trans_transformer=mock.MagicMock(),
                api_handler=api_handler,
            )
            since = dt.datetime(2021, 5, 4, tzinfo=dt.timezone.utc)
            source.fetch(since=since)
        api_handler.fetch_history_dataframe.assert_called_once()
        _, kwargs = api_handler.fetch_history_dataframe.call_args
        self.assertEqual(kwargs["since"], since)
        self.assertEqual(kwargs["request_ids_to_skip"], [])

    def test_trading212_fetch_collects_existing_report_ids(self):
        """Confirm fetch gathers cached report IDs from disk before calling the API."""
        df = pd.DataFrame({"foo": [1]})
        api_handler = mock.MagicMock()
        api_handler.fetch_history_dataframe.return_value = df
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pd.DataFrame({"_reportId": ["111", None]}).to_csv(
                tmp_path / "existing_upper.csv", index=False
            )
            pd.DataFrame({"_reportid": ["222"]}).to_csv(
                tmp_path / "existing_lower.csv", index=False
            )
            source = Trading212APIStatements(
                working_dir=tmp_path,
                trans_transformer=mock.MagicMock(),
                api_handler=api_handler,
            )
            since = dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc)
            source.fetch(since=since)

        api_handler.fetch_history_dataframe.assert_called_once()
        _, kwargs = api_handler.fetch_history_dataframe.call_args
        self.assertEqual(kwargs["since"], since)
        self.assertEqual(kwargs["request_ids_to_skip"], ["111", "222"])

    def test_trading212_fetch_defaults_since_from_cached_chunk(self):
        """Ensure cached chunk metadata advances the implicit since datetime."""
        api_handler = mock.MagicMock()
        api_handler.fetch_history_dataframe.return_value = pd.DataFrame({"foo": [1]})
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pd.DataFrame(
                {
                    "_reportId": ["555"],
                    "_chunk_to": ["2020-12-31 23:59:59+00:00"],
                }
            ).to_csv(tmp_path / "existing.csv", index=False)
            source = Trading212APIStatements(
                working_dir=tmp_path,
                trans_transformer=mock.MagicMock(),
                api_handler=api_handler,
            )
            source.fetch()

        api_handler.fetch_history_dataframe.assert_called_once()
        _, kwargs = api_handler.fetch_history_dataframe.call_args
        expected_since = dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc)
        self.assertEqual(kwargs["since"], expected_since)
        self.assertEqual(kwargs["request_ids_to_skip"], ["555"])

    def test_trading212_collect_cached_metadata(self):
        """Collect cached report IDs and chunk metadata from existing files."""
        api_handler = mock.MagicMock()
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pd.DataFrame({"_reportId": ["111", " 222 "]}).to_csv(
                tmp_path / "existing_upper.csv", index=False
            )
            pd.DataFrame({"_reportid": [None, "333"], "_chunk_to": ["2021-01-01T00:00:00Z", None]}).to_csv(
                tmp_path / "existing_lower.csv", index=False
            )
            source = Trading212APIStatements(
                working_dir=tmp_path,
                trans_transformer=mock.MagicMock(),
                api_handler=api_handler,
            )

            with self.assertLogs(level="INFO") as captured_logs:
                existing_report_ids, cached_chunk_tos = source._collect_cached_metadata()

        self.assertSetEqual(existing_report_ids, {"111", "222", "333"})
        self.assertEqual(len(cached_chunk_tos), 1)
        self.assertIn("Collecting cached metadata", "".join(captured_logs.output))

    def test_trading212_log_cached_report_ids(self):
        """Log a summary of the cached report IDs when present."""
        source = Trading212APIStatements(
            working_dir=Path("/tmp"),
            trans_transformer=mock.MagicMock(),
            api_handler=mock.MagicMock(),
        )
        with self.assertLogs(level="INFO") as captured_logs:
            source._log_cached_report_ids({"111", "222"})

        log_output = "".join(captured_logs.output)
        self.assertIn("Logging cached report IDs", log_output)
        self.assertIn("detected 2 cached export id(s)", log_output)

    def test_trading212_determine_since(self):
        """Derive an appropriate since datetime based on cached metadata."""
        source = Trading212APIStatements(
            working_dir=Path("/tmp"),
            trans_transformer=mock.MagicMock(),
            api_handler=mock.MagicMock(),
        )
        cached_chunk_to = dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc)
        with self.assertLogs(level="INFO") as captured_logs:
            result = source._determine_since(None, [cached_chunk_to])

        expected = cached_chunk_to + dt.timedelta(seconds=1)
        self.assertEqual(result, expected)
        self.assertIn("Determining since parameter", "".join(captured_logs.output))

    def test_trading212_determine_since_returns_none_when_up_to_date(self):
        """Return None when cached metadata already covers the current time."""
        source = Trading212APIStatements(
            working_dir=Path("/tmp"),
            trans_transformer=mock.MagicMock(),
            api_handler=mock.MagicMock(),
        )
        latest_chunk_to = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
        now_value = latest_chunk_to + dt.timedelta(seconds=1)
        with mock.patch("mecon.etl.account_statements.dt.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = now_value
            mocked_datetime.side_effect = lambda *args, **kwargs: dt.datetime(*args, **kwargs)
            mocked_datetime.timezone = dt.timezone
            mocked_datetime.timedelta = dt.timedelta
            with self.assertLogs(level="INFO") as captured_logs:
                result = source._determine_since(None, [latest_chunk_to])

        self.assertIsNone(result)
        self.assertIn("nothing to fetch", "".join(captured_logs.output))

    def test_monzo_fetch_passes_since_string(self):
        """Check Monzo fetch converts the datetime to the expected ISO string."""
        api_handler = mock.MagicMock(return_value=pd.DataFrame())
        with TemporaryDirectory() as tmpdir:
            source = MonzoAPIStatements(
                working_dir=Path(tmpdir),
                trans_transformer=mock.MagicMock(),
                api_handler=api_handler,
            )
            since = dt.datetime(2022, 2, 3, tzinfo=dt.timezone.utc)
            source.fetch(since=since)
        api_handler.download_full_history.assert_called_once_with(
            since="2022-02-03T00:00:00Z"
        )


class FetchIfNeededTests(unittest.TestCase):
    def test_fetch_if_needed_and_transform_fetches_missing_days(self):
        """Fetch when data is stale and re-run transformation to include new rows."""
        with TemporaryDirectory() as tmpdir:
            source = DummyAPIAccount(
                working_dir=Path(tmpdir),
                trans_transformer=mock.MagicMock(),
                api_handler=mock.MagicMock(),
            )
            last_date = dt.date.today() - dt.timedelta(days=2)
            existing = mock.Mock()
            existing.date_range.return_value = (None, last_date)
            final = mock.Mock()
            source.to_transactions = mock.MagicMock(side_effect=[existing, final])
            source.fetch = mock.MagicMock()

            result, fetched = source.fetch_if_needed_and_transform()

        expected_since = dt.datetime.combine(
            last_date + dt.timedelta(days=1),
            dt.datetime.min.time(),
        ).replace(tzinfo=dt.timezone.utc)
        source.fetch.assert_called_once_with(since=expected_since)
        self.assertEqual(result, final)
        self.assertTrue(fetched)
        self.assertEqual(source.to_transactions.call_count, 2)

    def test_fetch_if_needed_and_transform_skips_when_up_to_date(self):
        """Skip fetching when latest transactions already cover today's date."""
        with TemporaryDirectory() as tmpdir:
            source = DummyAPIAccount(
                working_dir=Path(tmpdir),
                trans_transformer=mock.MagicMock(),
                api_handler=mock.MagicMock(),
            )
            last_date = dt.date.today()
            existing = mock.Mock()
            existing.date_range.return_value = (None, last_date)
            source.to_transactions = mock.MagicMock(return_value=existing)
            source.fetch = mock.MagicMock()

            result, fetched = source.fetch_if_needed_and_transform()

        source.fetch.assert_not_called()
        self.assertFalse(fetched)
        self.assertEqual(result, existing)
        source.to_transactions.assert_called_once()


if __name__ == "__main__":
    unittest.main()
