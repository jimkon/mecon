import datetime as dt
import json
import shutil
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pandas as pd

from mecon.etl.account_statements import (
    MonzoAPIStatements,
    Trading212APIStatements,
    TrueLayerHSBCStatements,
    TrueLayerStatements,
)
from mecon.etl.dataset import Dataset


class _QueueTrading212Client:
    """Simple FIFO client stub returning preconfigured dataframes."""

    def __init__(self, frames: list[pd.DataFrame]):
        self._frames = list(frames)
        self.calls: list[dict[str, object]] = []

    def fetch_history_dataframe(self, *, since, request_ids_to_skip):
        self.calls.append(
            {
                "since": since,
                "request_ids_to_skip": list(request_ids_to_skip),
            }
        )
        if not self._frames:
            raise AssertionError("No more responses configured for Trading212 client stub.")
        return self._frames.pop(0)


class _QueueMonzoClient:
    """FIFO Monzo client stub that records the requested since window."""

    def __init__(self, frames: list[pd.DataFrame]):
        self._frames = list(frames)
        self.calls: list[dict[str, object]] = []

    def download_full_history(self, *, since):
        self.calls.append({"since": since})
        if not self._frames:
            raise AssertionError("No more responses configured for Monzo client stub.")
        return self._frames.pop(0)


class _QueueTrueLayerClient:
    """FIFO TrueLayer client stub capturing requested filters."""

    def __init__(self, payloads: list[list[dict[str, object]]]):
        self._payloads = list(payloads)
        self.calls: list[dict[str, object]] = []

    def get_transactions(self, bank, account_id, *, from_date=None, to_date=None):
        self.calls.append(
            {
                "bank": bank,
                "account_id": account_id,
                "from_date": from_date,
                "to_date": to_date,
            }
        )
        if not self._payloads:
            raise AssertionError("No more responses configured for TrueLayer client stub.")
        return self._payloads.pop(0)


class Trading212DatasetFlowTests(unittest.TestCase):
    def test_fetch_flow_appends_files_and_reuses_cached_exports(self):
        """Run dataset flow to cover append, incremental fetch, and cache-only runs."""

        first_chunk_to = dt.datetime(2020, 1, 31, 23, 59, 59, tzinfo=dt.timezone.utc)
        second_chunk_to = dt.datetime(2020, 2, 29, 23, 59, 59, tzinfo=dt.timezone.utc)

        responses = [
            pd.DataFrame(
                {
                    "_reportId": ["alpha"],
                    "_chunk_to": [first_chunk_to.isoformat()],
                    "value": [1],
                }
            ),
            pd.DataFrame(
                {
                    "_reportId": ["beta"],
                    "_chunk_to": [second_chunk_to.isoformat()],
                    "value": [2],
                }
            ),
            pd.DataFrame(columns=["_reportId", "_chunk_to", "value"]),
            pd.DataFrame(columns=["_reportId", "_chunk_to", "value"]),
        ]

        api_client = _QueueTrading212Client(responses)

        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dataset_template = (
                Path(__file__).resolve().parent
                / "datasets"
                / "test_apis_providers_and_fetch"
            )
            dataset_path = tmp_path / "test_apis_providers_and_fetch"
            shutil.copytree(dataset_template, dataset_path)

            creds_path = tmp_path / "credentials.json"
            creds_path.write_text(
                json.dumps({"trading212": {"api_key": "dummy", "mode": "demo"}})
            )

            dataset = Dataset.from_dirpath(dataset_path)
            working_dir = dataset.statements / "Trading212API"

            source = Trading212APIStatements(
                working_dir=working_dir,
                trans_transformer=mock.MagicMock(),
                api_handler=api_client,
            )

            since_initial = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
            source.fetch(since=since_initial)
            files_after_first = list(working_dir.glob("*.csv"))
            self.assertEqual(len(files_after_first), 1)
            first_written = pd.read_csv(files_after_first[0])
            self.assertIn("alpha", first_written["_reportId"].tolist())

            since_recent = dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc)
            source.fetch(since=since_recent)
            files_after_second = list(working_dir.glob("*.csv"))
            self.assertEqual(len(files_after_second), 2)
            combined_reports = pd.concat(
                (pd.read_csv(path) for path in files_after_second), ignore_index=True
            )
            self.assertCountEqual(
                combined_reports["_reportId"].dropna().tolist(), ["alpha", "beta"]
            )

            since_future = dt.datetime(2020, 3, 5, tzinfo=dt.timezone.utc)
            source.fetch(since=since_future)
            self.assertEqual(len(list(working_dir.glob("*.csv"))), 2)

            source.fetch()
            self.assertEqual(len(list(working_dir.glob("*.csv"))), 2)

        expected_calls = [
            {
                "since": since_initial,
                "request_ids_to_skip": [],
            },
            {
                "since": since_recent,
                "request_ids_to_skip": ["alpha"],
            },
            {
                "since": since_future,
                "request_ids_to_skip": ["alpha", "beta"],
            },
            {
                "since": second_chunk_to + dt.timedelta(seconds=1),
                "request_ids_to_skip": ["alpha", "beta"],
            },
        ]

        self.assertEqual(len(api_client.calls), len(expected_calls))
        for recorded, expected in zip(api_client.calls, expected_calls):
            self.assertEqual(recorded["since"], expected["since"])
            self.assertEqual(recorded["request_ids_to_skip"], expected["request_ids_to_skip"])


class MonzoDatasetFlowTests(unittest.TestCase):
    def test_fetch_flow_creates_incremental_exports(self):
        """Ensure Monzo fetch writes incremental exports and reuses cached directory."""

        responses = [
            pd.DataFrame({"id": [1], "amount": [10]}),
            pd.DataFrame({"id": [2], "amount": [20]}),
            pd.DataFrame(),
        ]
        api_client = _QueueMonzoClient(responses)

        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            working_dir = tmp_path / "MonzoAPI"
            working_dir.mkdir(parents=True, exist_ok=True)

            source = MonzoAPIStatements(
                working_dir=working_dir,
                trans_transformer=mock.MagicMock(),
                api_handler=api_client,
            )

            since_initial = dt.datetime(2021, 5, 1, tzinfo=dt.timezone.utc)
            source.fetch(since=since_initial)
            files_after_first = sorted(working_dir.glob("*.csv"))
            self.assertEqual(len(files_after_first), 1)
            first_df = pd.read_csv(files_after_first[0])
            self.assertIn(1, first_df["id"].tolist())

            since_next = dt.datetime(2021, 6, 1, tzinfo=dt.timezone.utc)
            source.fetch(since=since_next)
            files_after_second = sorted(working_dir.glob("*.csv"))
            self.assertEqual(len(files_after_second), 2)
            combined_ids = pd.concat(
                (pd.read_csv(path)["id"] for path in files_after_second),
                ignore_index=True,
            )
            self.assertCountEqual(combined_ids.tolist(), [1, 2])

            source.fetch()
            self.assertEqual(len(list(working_dir.glob("*.csv"))), 2)

        self.assertEqual(
            [call["since"] for call in api_client.calls],
            ["2021-05-01T00:00:00Z", "2021-06-01T00:00:00Z", "2019-01-01T00:00:00Z"],
        )


class TrueLayerDatasetFlowTests(unittest.TestCase):
    def test_fetch_flow_appends_new_batches(self):
        """Verify TrueLayer fetch writes CSVs for new batches and stops on empty payloads."""

        payloads = [
            [{"transaction_id": "t1", "amount": 1}],
            [{"transaction_id": "t2", "amount": 2}],
            [],
        ]
        api_client = _QueueTrueLayerClient(payloads)

        with TemporaryDirectory() as tmpdir, mock.patch(
            "mecon.etl.account_statements.json_to_csv",
            side_effect=lambda payload: pd.DataFrame(payload),
        ):
            tmp_path = Path(tmpdir)
            working_dir = tmp_path / "TrueLayerHSBC"
            working_dir.mkdir(parents=True, exist_ok=True)

            source = TrueLayerHSBCStatements(
                working_dir=working_dir,
                trans_transformer=mock.MagicMock(),
                api_handler=api_client,
            )

            since_initial = dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc)
            source.fetch(since=since_initial)
            files_after_first = sorted(working_dir.glob("*.csv"))
            self.assertEqual(len(files_after_first), 1)
            first_df = pd.read_csv(files_after_first[0])
            self.assertIn("t1", first_df["transaction_id"].tolist())

            since_next = dt.datetime(2021, 2, 1, tzinfo=dt.timezone.utc)
            source.fetch(since=since_next)
            files_after_second = sorted(working_dir.glob("*.csv"))
            self.assertEqual(len(files_after_second), 2)
            combined_ids = pd.concat(
                (pd.read_csv(path)["transaction_id"] for path in files_after_second),
                ignore_index=True,
            )
            self.assertCountEqual(combined_ids.tolist(), ["t1", "t2"])

            source.fetch()
            self.assertEqual(len(list(working_dir.glob("*.csv"))), 2)

        self.assertEqual(
            api_client.calls,
            [
                {
                    "bank": "ob-hsbc",
                    "account_id": "d4aa58643585c1e3a5f7d3e24cf5e829",
                    "from_date": dt.date(2021, 1, 1),
                    "to_date": None,
                },
                {
                    "bank": "ob-hsbc",
                    "account_id": "d4aa58643585c1e3a5f7d3e24cf5e829",
                    "from_date": dt.date(2021, 2, 1),
                    "to_date": None,
                },
                {
                    "bank": "ob-hsbc",
                    "account_id": "d4aa58643585c1e3a5f7d3e24cf5e829",
                    "from_date": None,
                    "to_date": None,
                },
            ],
        )

    def test_tl_from_id_and_creds(self):
        dataset_path = (
                Path(__file__).resolve().parent
                / "datasets"
                / "test_statements_and_tags"
        )
        dataset = Dataset.from_dirpath(dataset_path)

        tl_hsbc = TrueLayerStatements.from_account_id(dataset, account_id='d4aa58643585c1e3a5f7d3e24cf5e829')
        self.assertIsNotNone(tl_hsbc)

        tl_hsbc_saver = TrueLayerStatements.from_account_id(dataset, account_id='875dba485407b435dfddccc5a91e772b')
        self.assertIsNotNone(tl_hsbc_saver)

        tl_invalid_id = TrueLayerStatements.from_account_id(dataset, account_id='a random id')
        self.assertIsNone(tl_invalid_id)

        tl_invalid_id = TrueLayerStatements.from_account_id(dataset,
                                                            account_id='fa5ddbfc7431ffd009445263b4259094') # valid but not existing dir
        self.assertIsNone(tl_invalid_id)


if __name__ == "__main__":
    unittest.main()
