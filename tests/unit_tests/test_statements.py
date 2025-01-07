import pathlib
import unittest
from io import StringIO
from unittest.mock import Mock, call, patch

import pandas as pd
from pandas import Timestamp

from mecon.etl.statements import HSBCStatementCSV, MonzoStatementCSV, RevoStatementCSV, StatementCSV, StatementDFMergeStrategies


class TestMerge(unittest.TestCase):
    def test_merge_statement_dataframes_non_overlapping(self):
        # Sample dataframes
        df1 = pd.DataFrame({'date_col': pd.date_range(start='2022-01-01', end='2022-01-03'),
                            'amount': [100, 200, 300]})
        df2 = pd.DataFrame({'date_col': pd.date_range(start='2022-01-09', end='2022-01-13'),
                            'amount': [700, 800, 900, 1000, 1100]})
        df3 = pd.DataFrame({'date_col': pd.date_range(start='2022-01-05', end='2022-01-07'),
                            'amount': [400, 500, 600]})

        # Merge dataframes
        merged_df = StatementDFMergeStrategies.merge_last_first([df1, df2, df3], 'date_col')

        # Assert merged dataframe has the correct length
        self.assertEqual(len(merged_df), 11)
        self.assertEqual(merged_df['date_col'].tolist(), [Timestamp('2022-01-01 00:00:00'),
                                                          Timestamp('2022-01-02 00:00:00'),
                                                          Timestamp('2022-01-03 00:00:00'),
                                                          Timestamp('2022-01-05 00:00:00'),
                                                          Timestamp('2022-01-06 00:00:00'),
                                                          Timestamp('2022-01-07 00:00:00'),
                                                          Timestamp('2022-01-09 00:00:00'),
                                                          Timestamp('2022-01-10 00:00:00'),
                                                          Timestamp('2022-01-11 00:00:00'),
                                                          Timestamp('2022-01-12 00:00:00'),
                                                          Timestamp('2022-01-13 00:00:00')])
        self.assertEqual(merged_df['amount'].tolist(), [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100])

    def test_merge_statement_dataframes_overlapping_1(self):
        # Sample dataframes
        df1 = pd.DataFrame({'date': pd.date_range(start='2022-01-02', end='2022-01-04'),
                            'amount': [100, 200, 250]})
        df2 = pd.DataFrame({'date': pd.date_range(start='2022-01-03', end='2022-01-07'),
                            'amount': [400, 500, 450, 600, 550]})
        df3 = pd.DataFrame({'date': pd.date_range(start='2022-01-01', end='2022-01-05'),
                            'amount': [100, 200, 150, 300, 250]})

        # Merge dataframes
        merged_df = StatementDFMergeStrategies.merge_last_first([df1, df2, df3], 'date')

        # Assert merged dataframe has the correct length
        self.assertEqual(len(merged_df), 7)
        self.assertEqual(merged_df['date'].tolist(), pd.date_range(start='2022-01-01', end='2022-01-07').tolist())
        self.assertEqual(merged_df['amount'].tolist(), [100, 200, 400, 500, 450, 600, 550])

    def test_merge_statement_dataframes_overlapping_2(self):
        # Sample dataframes
        df1 = pd.DataFrame({
            'date': [Timestamp('2020-12-20 00:00:00'), Timestamp('2020-12-22 00:00:00'),
                     Timestamp('2020-12-23 00:00:00')],
            'description': ['desc1', 'desc2', 'desc3'],
            'amount': [0.0, 2.0, 3.0],
        })
        df2 = pd.DataFrame({
            'date': [Timestamp('2020-12-21 00:00:00'), Timestamp('2020-12-23 00:00:00'),
                     Timestamp('2020-12-25 00:00:00')],
            'description': ['desc4', 'desc5', 'desc6'],
            'amount': [-1.0, -3.0, -5.0],
        })

        # Merge dataframes
        merged_df = StatementDFMergeStrategies.merge_last_first([df1, df2], 'date')

        pd.testing.assert_frame_equal(merged_df,
                                      pd.DataFrame(data={
                                          'date': [
                                              Timestamp('2020-12-20 00:00:00'),
                                              Timestamp('2020-12-21 00:00:00'),
                                              Timestamp('2020-12-23 00:00:00'),
                                              Timestamp('2020-12-25 00:00:00')
                                          ],
                                          'description': ['desc1', 'desc4', 'desc5', 'desc6'],
                                          'amount': [0., -1., -3., -5]
                                      }))


class TestStatementCSV(unittest.TestCase):
    def test_dataframe(self):
        statement = StatementCSV(None)
        statement.transform = Mock(return_value=pd.DataFrame({'col2': [4, 5, 6]}))

        df = statement.dataframe()

        statement.transform.assert_called_once()
        self.assertTrue(df.equals(pd.DataFrame({'col2': [4, 5, 6]})))

    def test_load_many_from_dir(self):
        with patch.object(pathlib.Path, 'glob') as mck_path_glob, \
                patch.object(StatementCSV, 'from_path') as mck_from_path:
            mck_path_glob.return_value = [
                pathlib.Path('example_dir/prefix1_file1.csv'),
                pathlib.Path('example_dir/prefix1_file2.csv'),
                pathlib.Path('example_dir/prefix1_file3.not_csv'),
                pathlib.Path('example_dir/prefix2_file4.csv'),
            ]

            StatementCSV.load_many_from_dir(
                dir_path='it is mocked anyways',
                filename_condition_f=lambda name: name.startswith('prefix1')
            )

            mck_from_path.assert_has_calls([
                call(pathlib.Path('example_dir/prefix1_file1.csv')),
                call(pathlib.Path('example_dir/prefix1_file2.csv'))
            ])

    def test_split_and_store_files_based_on_year(self):
        res_dfs = []

        class ExampleStatementCSV(StatementCSV):
            def __init__(self, df):
                res_dfs.append(df)
                super().__init__(df)

        example_statement = ExampleStatementCSV(pd.DataFrame({
            'date': [
                Timestamp('2020-12-24 00:00:00'),
                Timestamp('2020-12-22 00:00:00'),
                Timestamp('2021-12-22 00:00:00'),
                Timestamp('2023-12-22 00:00:00'),
                Timestamp('2023-12-22 00:00:00'),
            ],
            'description': ['desc1', 'desc2', 'desc3', 'desc4', 'desc5'],
            'amount': [-100., -200., -300., -400., -500.]
        }))

        with patch.object(ExampleStatementCSV, 'to_path') as mck_to_path:
            example_statement.split_and_store_files_based_on_year(
                dir_path=pathlib.Path('a path'),
                date_col_name='date',
                filename_prefix='prefix'
            )

            pd.testing.assert_frame_equal(res_dfs[0].reset_index(drop=True), example_statement.dataframe())

            pd.testing.assert_frame_equal(res_dfs[1].reset_index(drop=True), pd.DataFrame({
                'date': [
                    Timestamp('2020-12-24 00:00:00'),
                    Timestamp('2020-12-22 00:00:00'),
                ],
                'description': ['desc1', 'desc2'],
                'amount': [-100., -200.]
            }))

            pd.testing.assert_frame_equal(res_dfs[2].reset_index(drop=True), pd.DataFrame({
                'date': [
                    Timestamp('2021-12-22 00:00:00'),
                ],
                'description': ['desc3'],
                'amount': [-300.]
            }))

            pd.testing.assert_frame_equal(res_dfs[3].reset_index(drop=True), pd.DataFrame({
                'date': [
                    Timestamp('2023-12-22 00:00:00'),
                    Timestamp('2023-12-22 00:00:00'),
                ],
                'description': ['desc4', 'desc5'],
                'amount': [-400., -500.]
            }))

            mck_to_path.assert_has_calls([
                call(pathlib.Path('a path/prefix_FROM2020-12-22-TO2020-12-24')),
                call(pathlib.Path('a path/prefix_FROM2021-12-22-TO2021-12-22')),
                call(pathlib.Path('a path/prefix_FROM2023-12-22-TO2023-12-22'))
            ])

    def test_from_all_paths_in_dir(self):
        with patch.object(StatementCSV, 'load_many_from_dir') as mock_load_stats:
            mock_load_stats.return_value = [
                StatementCSV(pd.DataFrame(data={
                    'date': [
                        Timestamp('2020-12-20 00:00:00'),
                        Timestamp('2020-12-22 00:00:00'),
                        Timestamp('2020-12-23 00:00:00'),
                    ],
                    'description': ['desc1', 'desc2', 'desc3'],
                    'amount': [0., 2., 3.]
                })),
                StatementCSV(pd.DataFrame(data={
                    'date': [
                        Timestamp('2020-12-21 00:00:00'),
                        Timestamp('2020-12-23 00:00:00'),
                        Timestamp('2020-12-25 00:00:00')
                    ],
                    'description': ['desc4', 'desc5', 'desc6'],
                    'amount': [-1., -3., -5]
                }))
            ]
            df = StatementCSV.from_all_paths_in_dir(dir_path='example_path',
                                                    file_prefix='does not matter here',
                                                    date_col_name='date').dataframe()
            pd.testing.assert_frame_equal(
                df,
                pd.DataFrame(data={
                    'date': [
                        Timestamp('2020-12-20 00:00:00'),
                        Timestamp('2020-12-21 00:00:00'),
                        Timestamp('2020-12-23 00:00:00'),
                        Timestamp('2020-12-25 00:00:00')
                    ],
                    'description': ['desc1', 'desc4', 'desc5', 'desc6'],
                    'amount': [0., -1., -3., -5]
                }))


class TestHSBCStatementCSV(unittest.TestCase):
    def test_from_path(self):
        mock_csv_file = StringIO("""24/12/2020,desc1,-100.00\r\n22/12/2020,desc2,100.00""")
        statement = HSBCStatementCSV.from_path(mock_csv_file)
        self.assertEqual(statement._df.shape, (2, 3))
        pd.testing.assert_frame_equal(statement._df,
                                      pd.DataFrame(data={
                                          'date': [Timestamp('2020-12-24 00:00:00'), Timestamp('2020-12-22 00:00:00')],
                                          'description': ['desc1', 'desc2'],
                                          'amount': [-100., 100.]
                                      }))

    def test_to_path(self):
        statement = HSBCStatementCSV(pd.DataFrame(data={
            'date': [Timestamp('2020-12-24 00:00:00'), Timestamp('2020-12-22 00:00:00')],
            'description': ['desc1', 'desc2'],
            'amount': [-100., 100.]
        }))
        mock_csv_file = StringIO()
        statement.to_path(mock_csv_file)
        mock_csv_file.seek(0)
        csv_file_text = mock_csv_file.read()
        self.assertEqual(csv_file_text.split(), """24/12/2020,desc1,-100.0\n22/12/2020,desc2,100.0\n""".split())

    def test_compatibility_of_from_path_and_to_path(self):
        input_csv_file_text = """24/12/2020,desc1,-100.0\n22/12/2020,desc2,100.0\n"""
        mock_csv_file = StringIO(input_csv_file_text)
        statement = HSBCStatementCSV.from_path(mock_csv_file)
        mock_csv_file = StringIO()
        statement.to_path(mock_csv_file)
        mock_csv_file.seek(0)
        csv_file_text = mock_csv_file.read()
        self.assertEqual(csv_file_text.split(), input_csv_file_text.split())

    def test_from_all_paths_in_dir(self):
        with patch.object(HSBCStatementCSV, 'load_many_from_dir') as mock_load_stats:
            mock_load_stats.return_value = [
                HSBCStatementCSV.from_path(
                    StringIO("""20/12/2020,desc1,0.00\n22/12/2020,desc2,2.00\n23/12/2020,desc3,3.00\n""")),
                HSBCStatementCSV.from_path(
                    StringIO("""21/12/2020,desc4,-1.00\n23/12/2020,desc5,-3.00\n25/12/2020,desc6,-5.00\n"""))
            ]
            df = HSBCStatementCSV.from_all_paths_in_dir('example_path').dataframe()
            pd.testing.assert_frame_equal(
                df,
                pd.DataFrame(data={
                    'date': [
                        Timestamp('2020-12-20 00:00:00'),
                        Timestamp('2020-12-21 00:00:00'),
                        Timestamp('2020-12-23 00:00:00'),
                        Timestamp('2020-12-25 00:00:00')
                    ],
                    'description': ['desc1', 'desc4', 'desc5', 'desc6'],
                    'amount': [0., -1., -3., -5]
                }))


class TestMonzoStatementCSV(unittest.TestCase):
    def test_transform(self):
        statement_df = pd.DataFrame({
            "Transaction ID": [1, 2, 3],
            "Date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "Amount": [10.0, 20.0, 30.0],
            "Time": ["time"] * 3,
            "Type": ["transaction_type"] * 3,
            "Name": ["name"] * 3,
            "Emoji": ["emoji"] * 3,
            "Category": ["category"] * 3,
            "Currency": ["currency"] * 3,
            "Local amount": ["local_amount"] * 3,
            "Local currency": ["local_currency"] * 3,
            "Notes and #tags": ["notes_tags"] * 3,
            "Address": ["address"] * 3,
            "Receipt": ["receipt"] * 3,
            "Description": ["description"] * 3,
            "Category split": ["category_split"] * 3,
            "Money Out": ["money_out"] * 3,
            "Money In": ["money_in"] * 3,
        })

        statement = MonzoStatementCSV(statement_df)
        transformed_df = statement.transform()

        mapping = {
            # "Transaction ID": "id",  removed
            "Date": "date",
            "Time": "time",
            "Type": "transaction_type",
            "Name": "name",
            "Emoji": "emoji",
            "Category": "category",
            "Amount": "amount",
            "Currency": "currency",
            "Local amount": "local_amount",
            "Local currency": "local_currency",
            "Notes and #tags": "notes_tags",
            "Address": "address",
            "Receipt": "receipt",
            "Description": "description",
            "Category split": "category_split",
            "Money Out": "money_out",
            "Money In": "money_in",
        }
        for value in mapping.values():
            # Implement assertions for the transformed DataFrame
            self.assertIn(value, transformed_df.columns)
        # ...


class TestRevoStatementCSV(unittest.TestCase):
    def setUp(self):
        self.path = 'test_revo.csv'

    def test_transform(self):
        statement_df = pd.DataFrame({
            "Type": ["type"],
            "Product": ["product"],
            "Started Date": ["start_date"],
            "Completed Date": ["completed_date"],
            "Description": ["description"],
            "Amount": ["amount"],
            "Fee": ["fee"],
            "Currency": ["currency"],
            "State": ["state"],
            "Balance": ["balance"],
        })

        statement = RevoStatementCSV(statement_df)
        transformed_df = statement.transform()

        mapping = {
            "Type": "type",
            "Product": "product",
            "Started Date": "start_date",
            "Completed Date": "completed_date",
            "Description": "description",
            "Amount": "amount",
            "Fee": "fee",
            "Currency": "currency",
            "State": "state",
            "Balance": "balance",
        }
        for value in mapping.values():
            # Implement assertions for the transformed DataFrame
            self.assertIn(value, transformed_df.columns)


class TestDatasetStatements(unittest.TestCase):
    from mecon import config
    statements_dir = pathlib.Path(config.DEFAULT_DATASETS_DIR_PATH) / 'test/data/mock_statements_source_dir'
    if not statements_dir.exists():
        raise Exception(f"For this tests to run 'test' dataset has to be present.")

    def test_hsbc(self):
        dfs = HSBCStatementCSV.from_all_paths_in_dir(self.statements_dir)._df

        self.assertEqual(len(dfs), 58)
        self.assertEqual(dfs.shape[1], 3)
        self.assertEqual(dfs['date'].min(), Timestamp('2020-12-03 00:00:00'))
        self.assertEqual(dfs['date'].max(), Timestamp('2024-01-30 00:00:00'))
        self.assertEqual(int(dfs['amount'].sum()), 13430)

    def test_monzo(self):
        dfs = MonzoStatementCSV.from_all_paths_in_dir(self.statements_dir)._df

        self.assertEqual(len(dfs), 138)
        self.assertEqual(dfs.shape[1], 18)
        self.assertEqual(dfs['Date'].min(), '2019-05-15')
        self.assertEqual(dfs['Date'].max(), '2024-02-18')
        self.assertEqual(int(dfs['Amount'].sum()), -6247)

    def test_revo(self):
        dfs = RevoStatementCSV.from_all_paths_in_dir(self.statements_dir)._df
        self.assertEqual(len(dfs), 374)
        self.assertEqual(dfs.shape[1], 10)
        self.assertEqual(dfs['Started Date'].min(), '2019-05-11 16:57:56')
        self.assertEqual(dfs['Started Date'].max(), '2024-02-10 16:02:38')
        self.assertEqual(int(dfs['Amount'].sum()), 1317)


if __name__ == '__main__':
    unittest.main()
