import pathlib
import unittest
from unittest.mock import patch, mock_open

import pandas as pd

from mecon2.monitoring import log_data
from mecon2.monitoring import logs


class TestLogs(unittest.TestCase):
    def test_read_logs_string_as_df(self):
        csv_string = """2023-09-26 00:08:27,592,root,INFO,logs,print_logs_info,"#""Logs are stored in logs (filenames logs_raw.csv)""#"
2023-09-26 00:08:27,598,root,INFO,file_system,__init__,"#""New datasets directory in path PycharmProjects\mecon\datasets #info#filesystem""#"
2023-09-26 00:08:27,599,root,INFO,file_system,__init__,"#""New dataset in path PycharmProjects\mecon\datasets\mydata #info#filesystem""#"
 * Running on http://127.0.0.1:5000"
"""

        expected_df = pd.DataFrame({'datetime': ['2023-09-26 00:08:27', '2023-09-26 00:08:27', '2023-09-26 00:08:27'],
                                    'msecs': [592, 598, 599], 'logger': ['root', 'root', 'root'],
                                    'level': ['INFO', 'INFO', 'INFO'], 'module': ['logs', 'file_system', 'file_system'],
                                    'funcName': ['print_logs_info', '__init__', '__init__'],
                                    'message': ['#"Logs are stored in logs (filenames logs_raw.csv)"#',
                                                '#"New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem"#',
                                                '#"New dataset in path PycharmProjects\\mecon\\datasets\\mydata #info#filesystem"#']})

        result_df = logs.read_logs_string_as_df(csv_string)
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch("mecon2.monitoring.logs.read_logs_string_as_df")
    def test_read_logs_as_df(self, mock_read_str_logs):
        expected_df = pd.DataFrame({'datetime': ['2023-09-26 00:08:27', '2023-09-26 00:08:27', '2023-09-26 00:08:27'],
                                    'msecs': [592, 598, 599], 'logger': ['root', 'root', 'root'],
                                    'level': ['INFO', 'INFO', 'INFO'], 'module': ['logs', 'file_system', 'file_system'],
                                    'funcName': ['print_logs_info', '__init__', '__init__'],
                                    'message': ['#"Logs are stored in logs (filenames logs_raw.csv)"#',
                                                '#"New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem"#',
                                                '#"New dataset in path PycharmProjects\\mecon\\datasets\\mydata #info#filesystem"#']})

        mock_read_str_logs.return_value = expected_df
        with patch('pathlib.Path.__new__') as mock_new_path:
            with patch('pathlib.Path.read_text') as mock_path_read_text:
                result_df = logs.read_logs_as_df()
                pd.testing.assert_frame_equal(result_df, expected_df)

    @patch("mecon2.monitoring.logs.read_logs_string_as_df")
    def test_read_logs_as_df_historic_logs(self, mock_read_str_logs):
        mock_read_str_logs.return_value = pd.DataFrame({'datetime': ['2023-09-26 00:08:27', '2023-09-26 00:08:28'],
                                                        'msecs': [592, 598], 'logger': ['root', 'root'],
                                                        'level': ['INFO', 'INFO'], 'module': ['logs', 'file_system', ],
                                                        'funcName': ['print_logs_info', '__init__'],
                                                        'message': [
                                                            '#"Logs are stored in logs (filenames logs_raw.csv)"#',
                                                            '#"New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem"#']})

        expected_df = pd.DataFrame(
            {'datetime': ['2023-09-26 00:08:27', '2023-09-26 00:08:27', '2023-09-26 00:08:28', '2023-09-26 00:08:28'],
             'msecs': [592, 592, 598, 598], 'logger': ['root', 'root', 'root', 'root'],
             'level': ['INFO', 'INFO', 'INFO', 'INFO'], 'module': ['logs', 'logs', 'file_system', 'file_system'],
             'funcName': ['print_logs_info', 'print_logs_info', '__init__', '__init__'],
             'message': ['#"Logs are stored in logs (filenames logs_raw.csv)"#',
                         '#"Logs are stored in logs (filenames logs_raw.csv)"#',
                         '#"New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem"#',
                         '#"New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem"#']})

        with patch('pathlib.Path.__new__') as mock_new_path:
            with patch('pathlib.Path.read_text') as mock_path_read_text:
                with patch('pathlib.Path.glob') as mock_path_glob:
                    mock_path_glob.return_value = [pathlib.Path('file1')]
                    result_df = logs.read_logs_as_df(historic_logs=True)
                    pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                                  expected_df.reset_index(drop=True))


class TestLogData(unittest.TestCase):
    def test_extract_tags(self):
        self.assertEqual(log_data._extract_tags('example test #tag1'), 'tag1')
        self.assertEqual(log_data._extract_tags('example test #tag1#tag2'), 'tag1,tag2')
        self.assertEqual(log_data._extract_tags('example test #tag1 #tag2'), 'tag1,tag2')
        self.assertEqual(log_data._extract_tags('example test no tags'), '')
        self.assertEqual(log_data._extract_tags(''), '')

    def test_transform_raw_dataframe(self):
        test_df_raw = pd.DataFrame({'datetime': ['2023-09-26 00:08:27', '2023-09-26 00:08:28'],
                                    'msecs': [592, 598], 'logger': ['root', 'root'],
                                    'level': ['INFO', 'INFO'],
                                    'module': ['logs', 'file_system', ],
                                    'funcName': ['print_logs_info', '__init__'],
                                    'message': [
                                        '#"Logs are stored in logs (filenames logs_raw.csv)"#',
                                        '#"New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem"#']})

        expected_df_transformed = pd.DataFrame({
            'datetime': [pd.Timestamp('2023-09-26 00:08:27.592000'), pd.Timestamp('2023-09-26 00:08:28.598000')],
            'level': ['INFO', 'INFO'],
            'module': ['logs', 'file_system', ],
            'funcName': ['print_logs_info', '__init__'],
            'description': ["Logs are stored in logs (filenames logs_raw.csv)",
                            "New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem"],
            'tags': ['', 'info,filesystem']
        })

        result_df_transformed = log_data.transform_raw_dataframe(test_df_raw)

        pd.testing.assert_frame_equal(result_df_transformed, expected_df_transformed)


if __name__ == '__main__':
    unittest.main()
