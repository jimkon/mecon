import pathlib
import unittest
from io import StringIO
from unittest.mock import patch

import pandas as pd
from pandas import Timestamp

from mecon.monitoring import logs, log_data


class TestLogs(unittest.TestCase):
    def test_read_logs_string_as_df(self):
        csv_string = """2023-09-26 00:08:27,592,root,INFO,logs,print_logs_info,~Logs are stored in logs (filenames logs_raw.csv)~
2023-09-26 00:08:27,598,root,INFO,file_system,__init__,~New datasets directory in path PycharmProjects\mecon\datasets #info#filesystem~
2023-09-26 00:08:27,599,root,INFO,file_system,__init__,~New dataset in path PycharmProjects\mecon\datasets\mydata #info#filesystem~
 * Running on http://127.0.0.1:5000
2023-09-28 16:42:08,583,werkzeug,INFO,_internal,_log,~ * Detected change in 'mecon\\monitoring\\log_data.py', reloading~
2023-09-29 17:25:50,000,werkzeug,INFO,_internal,_log,~127.0.0.1 - - [29/Sep/2023 17:25:50] "GET /reports/graph/amount_freq/dates:2019-09-07_2023-08-17,tags:Accomodation,group:month HTTP/1.1" 200 -~
"""

        expected_df = pd.DataFrame({'datetime': ['2023-09-26 00:08:27',
                                                 '2023-09-26 00:08:27',
                                                 '2023-09-26 00:08:27',
                                                 '2023-09-28 16:42:08',
                                                 '2023-09-29 17:25:50'],
                                    'msecs': [592, 598, 599, 583, 000],
                                    'logger': ['root', 'root', 'root', 'werkzeug', 'werkzeug'],
                                    'level': ['INFO', 'INFO', 'INFO', 'INFO', 'INFO'],
                                    'module': ['logs', 'file_system', 'file_system', '_internal', '_internal'],
                                    'funcName': ['print_logs_info', '__init__', '__init__', '_log', '_log'],
                                    'message': ['Logs are stored in logs (filenames logs_raw.csv)',
                                                'New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem',
                                                'New dataset in path PycharmProjects\\mecon\\datasets\\mydata #info#filesystem',
                                                " * Detected change in 'mecon\\monitoring\\log_data.py', reloading",
                                                '127.0.0.1 - - [29/Sep/2023 17:25:50] "GET /reports/graph/amount_freq/dates:2019-09-07_2023-08-17,tags:Accomodation,group:month HTTP/1.1" 200 -']})

        result_df = logs.read_logs_string_as_df(csv_string)
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch("mecon.monitoring.logs.read_logs_string_as_df")
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
                result_df = logs.read_logs_as_df([pathlib.Path('mocked')])
                pd.testing.assert_frame_equal(result_df, expected_df)

    @patch("mecon.monitoring.logs.read_logs_string_as_df")
    def test_read_logs_as_df_historical_logs(self, mock_read_str_logs):
        example_df = pd.DataFrame({'datetime': ['2023-09-26 00:08:27', '2023-09-26 00:08:28'],
                                   'msecs': [592, 598], 'logger': ['root', 'root'],
                                   'level': ['INFO', 'INFO'], 'module': ['logs', 'file_system', ],
                                   'funcName': ['print_logs_info', '__init__'],
                                   'message': [
                                       '#"Logs are stored in logs (filenames logs_raw.csv)"#',
                                       '#"New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem"#']})
        mock_read_str_logs.return_value = example_df

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
                result_df = logs.read_logs_as_df([pathlib.Path('mocked_1'), pathlib.Path('mocked_2')])
                self.assertEqual(len(result_df), 2 * len(example_df))
                pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                              expected_df.reset_index(drop=True))


class HistoricalPerformanceDataTestCase(unittest.TestCase):
    example_csv_file = """2024-01-10 18:02:17.617993947,2.006053924560547,"DataframeWrapper.apply_negated_rule,data,transactions,tags"
2024-01-10 18:02:17.617993947,2.006053924560547,"Tagger.filter_df_with_negated_rule,data,tags"
2024-01-10 18:02:17.617994113,1.0058879852294922,"Tagger.get_index_for_rule,data,tags"
2024-01-10 18:02:17.624000456,2.999544143676758,"DataframeWrapper.apply_rule,data,transactions,tags"
2024-01-10 18:02:17.625000000,0.0,"Tagger.get_index_for_rule,data,tags"
2024-01-10 18:02:17.625000384,1.9996166229248047,"Tagger.filter_df_with_rule,data,tags"
2024-01-10 18:02:17.639993159,1.0068416595458984,"Grouping.group,data,transactions,process"
2024-01-10 18:02:17.645000000,0.0,"Grouping.group,data,transactions,process"
"""
    def setUp(self) -> None:
        logs.HistoricalPerformanceData._filename = StringIO(self.example_csv_file)
        self.perf_data = logs.HistoricalPerformanceData.load_historical_data()

    def test_load(self):
        self.assertListEqual(self.perf_data.dataframe()['datetime'].to_list(), [Timestamp('2024-01-10 18:02:17.617993947'),
                                                                           Timestamp('2024-01-10 18:02:17.617993947'),
                                                                           Timestamp('2024-01-10 18:02:17.617994113'),
                                                                           Timestamp('2024-01-10 18:02:17.624000456'),
                                                                           Timestamp('2024-01-10 18:02:17.625000'),
                                                                           Timestamp('2024-01-10 18:02:17.625000384'),
                                                                           Timestamp('2024-01-10 18:02:17.639993159'),
                                                                           Timestamp('2024-01-10 18:02:17.645000')])

        self.assertListEqual(self.perf_data.dataframe()['execution_time'].to_list(), [2.006053924560547,
                                                                                 2.006053924560547,
                                                                                 1.0058879852294922,
                                                                                 2.999544143676758,
                                                                                 0.0,
                                                                                 1.999616622924805,
                                                                                 1.0068416595458984,
                                                                                 0.0])

        self.assertListEqual(self.perf_data.dataframe()['tags'].to_list(),
                             ['DataframeWrapper.apply_negated_rule,data,transactions,tags',
                              'Tagger.filter_df_with_negated_rule,data,tags',
                              'Tagger.get_index_for_rule,data,tags',
                              'DataframeWrapper.apply_rule,data,transactions,tags',
                              'Tagger.get_index_for_rule,data,tags',
                              'Tagger.filter_df_with_rule,data,tags',
                              'Grouping.group,data,transactions,process',
                              'Grouping.group,data,transactions,process'])

    def test_append_log_data(self):
        perf_data = self.perf_data.copy()  # avoid appending to self.perf_data and cause issues to other tests
        logs_data_obj = log_data.LogData(pd.DataFrame({
            'datetime': [Timestamp('2023-09-29 18:11:58.298000'), Timestamp('2023-09-29 18:11:58.299000')],
            'level': ['DEBUG', 'INFO'],  # same for all
            'module': ['logs', 'logs'],  # same for all
            'funcName': ['wrapper', 'wrapper'],
            'description': [
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished.  (exec_dur=0.001 seconds) #codeflow#end#Transactions.__init__#data#transactions#process'],
            'tags': [  # only 'start' and 'end' tags will be different
                'codeflow,start,Transactions.__init__,data,transactions,process',
                'codeflow,end,Transactions.__init__,data,transactions,process']
        }))

        perf_data.append_log_data(logs_data_obj)
        self.assertEqual(len(perf_data.dataframe()), len(self.perf_data.dataframe())+1)
        self.assertDictEqual(perf_data.dataframe().iloc[0].to_dict(), {'datetime': Timestamp('2023-09-29 18:11:58.298000'), 'execution_time': 1.0, 'tags': 'Transactions.__init__,data,transactions,process'})
