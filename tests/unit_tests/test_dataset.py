import pathlib
import shutil
import tempfile
import unittest
from pathlib import Path
from datetime import datetime
from unittest import mock

import pandas as pd
import pytest

from mecon.etl import dataset as fs


def create_temporary_folder():
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir)

    # Create subfolders
    subfolder1 = temp_path / "subfolder1"
    subfolder1.mkdir()
    subfolder2 = temp_path / "subfolder2"
    subfolder2.mkdir()

    # Create CSV files in subfolder1
    csv_file1 = subfolder1 / "file1.csv"
    csv_file1.touch()
    csv_file2 = subfolder1 / "file2.csv"
    csv_file2.touch()

    # Create CSV files in subfolder2
    csv_file3 = subfolder2 / "file3.csv"
    csv_file3.touch()
    csv_file4 = subfolder2 / "file4.csv"
    csv_file4.touch()
    csv_file5 = subfolder2 / "file5.csv"
    csv_file5.touch()

    return temp_path


class SubfolderCSVTest(unittest.TestCase):
    def setUp(self):
        self.path = create_temporary_folder()
        self.subfolder_csv_files = fs._subfolder_csvs(self.path)

    def tearDown(self):
        shutil.rmtree(self.path)

    def test_subfolder_csv_files_existence(self):
        self.assertIsNotNone(self.subfolder_csv_files, "Subfolder CSV files dictionary should not be None")

    def test_subfolder_csv_files_type(self):
        self.assertIsInstance(self.subfolder_csv_files, dict, "Subfolder CSV files should be a dictionary")

    def test_subfolder_csv_files_content(self):
        expected_content = {
            "subfolder1": [
                self.path / "subfolder1/file1.csv",
                self.path / "subfolder1/file2.csv"
            ],
            "subfolder2": [
                self.path / "subfolder2/file3.csv",
                self.path / "subfolder2/file4.csv",
                self.path / "subfolder2/file5.csv",
            ]
            # Add more subfolder and CSV file paths as needed
        }
        self.assertEqual(self.subfolder_csv_files, expected_content, "Subfolder CSV files content mismatch")


class DatasetV1TestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        self.dataset = fs.DatasetV1.from_dirpath(self.temp_path)

    def tearDown(self):
        shutil.rmtree(self.temp_path)

    def test_db_path(self):
        self.assertEqual(self.dataset.db, self.temp_path / 'data/db/sqlite3')

    def test_statements_path(self):
        self.assertEqual(self.dataset.statements, self.temp_path / 'data/statements')

    def test_statement_files(self):
        self.assertEqual(self.dataset.statement_files(), {})

        temp_fp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        temp_fp.write(b'data')
        temp_fp.close()

        self.dataset.add_statement('test_bank', temp_fp.name)
        statement_files = self.dataset.statement_files()
        self.assertEqual(list(statement_files.keys()), ['test_bank'])
        self.assertEqual(len(statement_files['test_bank']), 1)
        csv_file = pathlib.Path(temp_fp.name)
        self.assertEqual(statement_files['test_bank'][0].name, str(csv_file.name))

        csv_file.unlink()

    # TODO remove
    @unittest.skip('Not used')
    def test_add_df_statement(self):
        # Initial assertion
        self.assertEqual(self.dataset.statement_files(), {})

        # Create a DataFrame for testing
        data = {'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']}
        df = pd.DataFrame(data)

        # Add DataFrame statement
        filename = 'test_df_statement.csv'
        bank_name = 'test_bank'
        self.dataset.add_df_statement(bank_name, df, filename)

        # Verify statement files
        statement_files = self.dataset.statement_files()
        self.assertEqual(list(statement_files.keys()), [bank_name])
        self.assertEqual(len(statement_files[bank_name]), 1)
        expected_path = self.dataset.statements / bank_name / filename
        self.assertEqual(statement_files[bank_name][0].name, str(expected_path.name))

        # Clean up temporary files
        expected_path.unlink()


class DatasetV2TestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        self.dataset = fs.DatasetV2(self.temp_path)

    def tearDown(self):
        shutil.rmtree(self.temp_path)

    def test_data_path(self):
        self.assertEqual(self.dataset.data, self.temp_path / 'data')

    def test_current_data_path(self):
        self.assertEqual(self.dataset.current_data, self.temp_path / 'data/current')

    def test_statements_path(self):
        self.assertEqual(self.dataset.statements, self.temp_path / 'data/statements')

    def test_db_path(self):
        with self.assertRaises(DeprecationWarning):
            self.dataset.db

    def test_file_structure(self):
        self.assertTrue(self.dataset.data.exists())
        self.assertTrue(self.dataset.current_data.exists())
        self.assertTrue(self.dataset.statements.exists())
        self.assertIsNotNone(self.dataset.settings)

    def test_statement_files(self):
        self.assertEqual(self.dataset.statement_files(), {})

        temp_fp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        temp_fp.write(b'data')
        temp_fp.close()

        bank_name, statement_path = 'test_bank', temp_fp.name
        statement_path = Path(statement_path)
        filename = statement_path.name
        new_statement_path = self.dataset.statements / bank_name / filename
        new_statement_path.parent.mkdir(parents=True, exist_ok=True)
        new_statement_path.write_bytes(statement_path.read_bytes())

        statement_files = self.dataset.statement_files()
        self.assertEqual(list(statement_files.keys()), ['test_bank'])
        self.assertEqual(len(statement_files['test_bank']), 1)
        csv_file = pathlib.Path(temp_fp.name)
        self.assertEqual(statement_files['test_bank'][0].name, str(csv_file.name))

        statement_files_info = self.dataset.statement_files_info()
        self.assertEqual(len(statement_files_info), 1)
        self.assertTrue('test_bank' in statement_files_info)

        statement_files_info_df = self.dataset.statement_files_info_df()
        self.assertEqual(statement_files_info_df.shape, (1, 4))
        self.assertEqual(statement_files_info_df['source'].tolist(), ['test_bank'])
        self.assertEqual(statement_files_info_df['rows'].tolist(), [0])

        csv_file.unlink()

class DateRollingDatasetTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.datasets_root = Path(self.temp_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.datasets_root)

    def _create_dataset(self, dataset_id: str) -> Path:
        dataset_path = self.datasets_root / dataset_id
        dataset_path.mkdir()
        fs.Dataset.from_dirpath(dataset_path)
        return dataset_path

    def test_find_datasets_ignores_invalid_entries(self):
        today_id = datetime.today().strftime("%Y%m%d")
        self._create_dataset(today_id)
        self._create_dataset("20240101")
        (self.datasets_root / "not_a_dataset").mkdir()
        (self.datasets_root / "2024010").mkdir()
        (self.datasets_root / "README.txt").write_text("not a dataset")

        with mock.patch("mecon.etl.dataset.datetime") as mock_datetime:
            mock_datetime.today.return_value.strftime.return_value = today_id
            dataset_dir = fs.DateRollingDataset(self.datasets_root, max_number_of_datasets=5)

        expected_dataset_names = {today_id, "20240101"}
        self.assertEqual(set(dataset_dir.dataset_names()), expected_dataset_names)

    @pytest.mark.skip("DateRollingDataset is disabled")
    def test_rollover_creates_copy_and_limits_history(self):
        dataset_ids = ["20240101", "20240102"]
        for dataset_id in dataset_ids:
            self._create_dataset(dataset_id)

        latest_dataset_path = self.datasets_root / "20240102"
        file_to_copy = latest_dataset_path / "data" / "current" / "existing.txt"
        file_to_copy.parent.mkdir(parents=True, exist_ok=True)
        file_to_copy.write_text("content")

        new_dataset_id = "20240103"
        with mock.patch("mecon.etl.dataset.datetime") as mock_datetime:
            mock_datetime.today.return_value.strftime.return_value = new_dataset_id
            dataset_dir = fs.DateRollingDataset(self.datasets_root, max_number_of_datasets=2)

        self.assertTrue((self.datasets_root / new_dataset_id).exists())
        copied_file = self.datasets_root / new_dataset_id / "data" / "current" / "existing.txt"
        self.assertTrue(copied_file.exists())
        self.assertEqual(copied_file.read_text(), "content")

        self.assertFalse((self.datasets_root / "20240101").exists())
        self.assertEqual(set(dataset_dir.dataset_names()), {"20240102", new_dataset_id})
        self.assertEqual(dataset_dir.get_last_dataset().name, new_dataset_id)

    def test_rollover_skips_when_today_dataset_already_exists(self):
        existing_dataset_id = "20240101"
        today_id = "20240102"

        self._create_dataset(existing_dataset_id)
        today_dataset_path = self._create_dataset(today_id)
        sentinel_file = today_dataset_path / "data" / "current" / "sentinel.txt"
        sentinel_file.parent.mkdir(parents=True, exist_ok=True)
        sentinel_file.write_text("original")

        with mock.patch("mecon.etl.dataset.datetime") as mock_datetime:
            mock_datetime.today.return_value.strftime.return_value = today_id
            dataset_dir = fs.DateRollingDataset(self.datasets_root, max_number_of_datasets=5)

        self.assertEqual(dataset_dir.get_last_dataset().name, today_id)
        self.assertEqual(set(dataset_dir.dataset_names()), {existing_dataset_id, today_id})
        self.assertEqual(sentinel_file.read_text(), "original")

if __name__ == '__main__':
    unittest.main()
