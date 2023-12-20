import pathlib
import unittest
import tempfile
import shutil
from pathlib import Path

import pandas as pd

from mecon.import_data import file_system as fs


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
                "file1.csv",
                "file2.csv"
            ],
            "subfolder2": [
                "file3.csv",
                "file4.csv",
                "file5.csv",
            ]
            # Add more subfolder and CSV file paths as needed
        }
        self.assertEqual(self.subfolder_csv_files, expected_content, "Subfolder CSV files content mismatch")


class DatasetTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        self.dataset = fs.Dataset(self.temp_path)

    def tearDown(self):
        shutil.rmtree(self.temp_path)

    def test_data_path(self):
        self.assertEqual(self.dataset._data, self.temp_path / 'data')

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
        self.assertEqual(statement_files['test_bank'][0], str(csv_file.name))

        csv_file.unlink()

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
        self.assertEqual(statement_files[bank_name][0], str(expected_path.name))

        # Clean up temporary files
        expected_path.unlink()


if __name__ == '__main__':
    unittest.main()
