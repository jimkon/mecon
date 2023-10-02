import pathlib
import unittest
from unittest.mock import patch

import pandas as pd
from pandas import Timestamp

from mecon2.monitoring import log_data
from mecon2.monitoring import logs


class TestLogs(unittest.TestCase):
    def test_read_logs_string_as_df(self):
        csv_string = """2023-09-26 00:08:27,592,root,INFO,logs,print_logs_info,~Logs are stored in logs (filenames logs_raw.csv)~
2023-09-26 00:08:27,598,root,INFO,file_system,__init__,~New datasets directory in path PycharmProjects\mecon\datasets #info#filesystem~
2023-09-26 00:08:27,599,root,INFO,file_system,__init__,~New dataset in path PycharmProjects\mecon\datasets\mydata #info#filesystem~
 * Running on http://127.0.0.1:5000
2023-09-28 16:42:08,583,werkzeug,INFO,_internal,_log,~ * Detected change in 'mecon2\\monitoring\\log_data.py', reloading~
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
                                                " * Detected change in 'mecon2\\monitoring\\log_data.py', reloading",
                                                '127.0.0.1 - - [29/Sep/2023 17:25:50] "GET /reports/graph/amount_freq/dates:2019-09-07_2023-08-17,tags:Accomodation,group:month HTTP/1.1" 200 -']})

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
                result_df = logs.read_logs_as_df([pathlib.Path('mocked')])
                pd.testing.assert_frame_equal(result_df, expected_df)

    @patch("mecon2.monitoring.logs.read_logs_string_as_df")
    def test_read_logs_as_df_historic_logs(self, mock_read_str_logs):
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
                                        'Logs are stored in logs (filenames logs_raw.csv)',
                                        'New datasets directory in path PycharmProjects\\mecon\\datasets #info#filesystem']})

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


class TestPerformanceDataAggregator(unittest.TestCase):
    def test_aggregation(self):
        logs_data_obj = log_data.LogData(pd.DataFrame({
            'datetime': [Timestamp('2023-09-29 18:11:58.298000'), Timestamp('2023-09-29 18:11:58.298000'),
                         Timestamp('2023-09-29 18:11:58.328000'), Timestamp('2023-09-29 18:11:58.329000'),
                         Timestamp('2023-09-29 18:11:58.330000'), Timestamp('2023-09-29 18:11:58.430000'),
                         Timestamp('2023-09-29 18:11:58.431000'), Timestamp('2023-09-29 18:11:58.432000'),
                         Timestamp('2023-09-29 18:11:58.433000'), Timestamp('2023-09-29 18:11:58.433000'),
                         Timestamp('2023-09-29 18:11:58.438000'), Timestamp('2023-09-29 18:11:58.439000'),
                         Timestamp('2023-09-29 18:11:58.539000')],
            'level': ['INFO', 'INFO', 'INFO', 'INFO', 'INFO', 'INFO', 'INFO', 'INFO', 'INFO', 'INFO', 'INFO', 'INFO',
                      'INFO'],  # same for all
            'module': ['logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs',
                       'logs'],  # same for all
            'funcName': ['wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper',  # same for all
                         'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper'],
            'description': [
                'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process'],
            'tags': [  # only 'start' and 'end' tags will be different
                'codeflow,start,Transactions.__init__,data,transactions,process',
                'codeflow,end,Transactions.__init__,data,transactions,process',
                'codeflow,start,Transactions.__init__,data,transactions,process',
                'codeflow,end,Transactions.__init__,data,transactions,process',
                'codeflow,start,Transactions.__init__,data,transactions,process',
                'codeflow,end,Transactions.__init__,data,transactions,process',
                'codeflow,start,Transactions.__init__,data,transactions,process',
                'codeflow,start,Transactions.__init__,data,transactions,process',
                'codeflow,end,Transactions.__init__,data,transactions,process',
                'codeflow,start,Transactions.__init__,data,transactions,process',
                'codeflow,start,Transactions.__init__,data,transactions,process',
                'codeflow,start,Transactions.__init__,data,transactions,process',
                'codeflow,end,Transactions.__init__,data,transactions,process']
        }))
        aggregator = log_data.PerformanceDataAggregator()
        perf_df = aggregator.aggregation(logs_data_obj).dataframe()

        expected_df = pd.DataFrame({'datetime': [Timestamp('2023-09-29 18:11:58.298000'),
                                                 Timestamp('2023-09-29 18:11:58.328000'),
                                                 Timestamp('2023-09-29 18:11:58.330000'),
                                                 Timestamp('2023-09-29 18:11:58.431000'),
                                                 Timestamp('2023-09-29 18:11:58.432000'),
                                                 Timestamp('2023-09-29 18:11:58.433000'),
                                                 Timestamp('2023-09-29 18:11:58.438000'),
                                                 Timestamp('2023-09-29 18:11:58.439000')],
                                    'execution_time': [-0.0, 1.0, 100.0, None, 1.0, None, None,
                                                       100.0],
                                    'tags': ['Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process']})

        pd.testing.assert_frame_equal(perf_df, expected_df)


class TestPerformanceData(unittest.TestCase):
    def test_from_log_data(self):
        #         csv_string = """
        # 2023-09-29 18:11:58,001,root,INFO,logs,wrapper,~a random log #notcodeflow#start#balance_graph#api~
        # 2023-09-29 18:11:58,002,root,INFO,logs,wrapper,~mecon2.blueprints.reports.reports_bp.balance_graph started... #codeflow#start#balance_graph#api~
        # 2023-09-29 18:11:58,003,root,INFO,logs,wrapper,~mecon2.blueprints.reports.reports_bp.get_filtered_transactions started... #codeflow#start#get_filtered_transactions#data#transactions#process~
        # 2023-09-29 18:11:58,004,root,INFO,logs,wrapper,~mecon2.blueprints.reports.reports_bp.get_transactions started... #codeflow#start#get_transactions#data#transactions#load~
        # 2023-09-29 18:11:58,005,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,006,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,007,root,INFO,logs,wrapper,~mecon2.blueprints.reports.reports_bp.get_transactions finished... #codeflow#end#get_transactions#data#transactions#load~
        # 2023-09-29 18:11:58,008,root,INFO,logs,wrapper,~mecon2.datafields.DataframeWrapper.apply_rule started... #codeflow#start#DataframeWrapper.apply_rule#data#transactions#tags~
        # 2023-09-29 18:11:58,009,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,010,root,INFO,logs,wrapper,~a random log #notcodeflow#start#balance_graph#api~
        # 2023-09-29 18:11:58,011,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,012,root,INFO,logs,wrapper,~mecon2.datafields.DataframeWrapper.apply_rule finished... #codeflow#end#DataframeWrapper.apply_rule#data#transactions#tags~
        # 2023-09-29 18:11:58,013,root,INFO,logs,wrapper,~mecon2.datafields.DataframeWrapper.apply_rule started... #codeflow#start#DataframeWrapper.apply_rule#data#transactions#tags~
        # 2023-09-29 18:11:58,014,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,015,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,016,root,INFO,logs,wrapper,~mecon2.datafields.DataframeWrapper.apply_rule finished... #codeflow#end#DataframeWrapper.apply_rule#data#transactions#tags~
        # 2023-09-29 18:11:58,017,root,INFO,logs,wrapper,~mecon2.datafields.DataframeWrapper.groupagg started... #codeflow#start#DataframeWrapper.groupagg#data#transactions#groupagg~
        # 2023-09-29 18:11:58,018,root,INFO,logs,wrapper,~a random log #notcodeflow#start#balance_graph#api~
        # 2023-09-29 18:11:58,019,root,INFO,logs,wrapper,~mecon2.datafields.Grouping.group started... #codeflow#start#Grouping.group#data#transactions#process~
        # 2023-09-29 18:11:58,021,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,022,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,023,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,024,root,INFO,logs,wrapper,~mecon2.datafields.Aggregator.aggregate started... #codeflow#start#Aggregator.aggregate#data#transactions#process~
        # 2023-09-29 18:11:58,025,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,026,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,027,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,028,root,INFO,logs,wrapper,~mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process~
        # 2023-09-29 18:11:58,029,root,INFO,logs,wrapper,~mecon2.datafields.Aggregator.aggregate finished... #codeflow#end#Aggregator.aggregate#data#transactions#process~
        # 2023-09-29 18:11:58,030,root,INFO,logs,wrapper,~mecon2.datafields.DataframeWrapper.groupagg finished... #codeflow#end#DataframeWrapper.groupagg#data#transactions#groupagg~
        # 2023-09-29 18:11:58,031,root,INFO,logs,wrapper,~mecon2.blueprints.reports.reports_bp.get_filtered_transactions finished... #codeflow#end#get_filtered_transactions#data#transactions#process~
        # 2023-09-29 18:11:58,032,root,INFO,logs,wrapper,~mecon2.blueprints.reports.reports_bp.balance_graph finished... #codeflow#end#balance_graph#api~
        # """
        #        log_data_obj = log_data.LogData.from_raw_logs(logs.read_logs_string_as_df(csv_string))

        log_data_df = pd.DataFrame.from_dict({
            0: {'datetime': Timestamp('2023-09-29 18:11:58.00100000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
                'tags': 'notcodeflow,start,balance_graph,api'},
            1: {'datetime': Timestamp('2023-09-29 18:11:58.00200000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon2.blueprints.reports.reports_bp.balance_graph started... #codeflow#start#balance_graph#api',
                'tags': 'codeflow,start,balance_graph,api'},
            2: {'datetime': Timestamp('2023-09-29 18:11:58.00300000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon2.blueprints.reports.reports_bp.get_filtered_transactions started... #codeflow#start#get_filtered_transactions#data#transactions#process',
                'tags': 'codeflow,start,get_filtered_transactions,data,transactions,process'},
            3: {'datetime': Timestamp('2023-09-29 18:11:58.00400000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon2.blueprints.reports.reports_bp.get_transactions started... #codeflow#start#get_transactions#data#transactions#load',
                'tags': 'codeflow,start,get_transactions,data,transactions,load'},
            4: {'datetime': Timestamp('2023-09-29 18:11:58.00500000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            5: {'datetime': Timestamp('2023-09-29 18:11:58.00600000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            6: {'datetime': Timestamp('2023-09-29 18:11:58.00700000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon2.blueprints.reports.reports_bp.get_transactions finished... #codeflow#end#get_transactions#data#transactions#load',
                'tags': 'codeflow,end,get_transactions,data,transactions,load'},
            7: {'datetime': Timestamp('2023-09-29 18:11:58.00800000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon2.datafields.DataframeWrapper.apply_rule started... #codeflow#start#DataframeWrapper.apply_rule#data#transactions#tags',
                'tags': 'codeflow,start,DataframeWrapper.apply_rule,data,transactions,tags'},
            8: {'datetime': Timestamp('2023-09-29 18:11:58.00900000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            9: {'datetime': Timestamp('2023-09-29 18:11:58.0100000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
                'tags': 'notcodeflow,start,balance_graph,api'},
            10: {'datetime': Timestamp('2023-09-29 18:11:58.0110000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            11: {'datetime': Timestamp('2023-09-29 18:11:58.0120000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.datafields.DataframeWrapper.apply_rule finished... #codeflow#end#DataframeWrapper.apply_rule#data#transactions#tags',
                 'tags': 'codeflow,end,DataframeWrapper.apply_rule,data,transactions,tags'},
            12: {'datetime': Timestamp('2023-09-29 18:11:58.0130000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.datafields.DataframeWrapper.apply_rule started... #codeflow#start#DataframeWrapper.apply_rule#data#transactions#tags',
                 'tags': 'codeflow,start,DataframeWrapper.apply_rule,data,transactions,tags'},
            13: {'datetime': Timestamp('2023-09-29 18:11:58.0140000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            14: {'datetime': Timestamp('2023-09-29 18:11:58.0150000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            15: {'datetime': Timestamp('2023-09-29 18:11:58.0160000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.datafields.DataframeWrapper.apply_rule finished... #codeflow#end#DataframeWrapper.apply_rule#data#transactions#tags',
                 'tags': 'codeflow,end,DataframeWrapper.apply_rule,data,transactions,tags'},
            16: {'datetime': Timestamp('2023-09-29 18:11:58.0170000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.datafields.DataframeWrapper.groupagg started... #codeflow#start#DataframeWrapper.groupagg#data#transactions#groupagg',
                 'tags': 'codeflow,start,DataframeWrapper.groupagg,data,transactions,groupagg'},
            17: {'datetime': Timestamp('2023-09-29 18:11:58.0180000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
                 'tags': 'notcodeflow,start,balance_graph,api'},
            18: {'datetime': Timestamp('2023-09-29 18:11:58.0190000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.datafields.Grouping.group started... #codeflow#start#Grouping.group#data#transactions#process',
                 'tags': 'codeflow,start,Grouping.group,data,transactions,process'},
            19: {'datetime': Timestamp('2023-09-29 18:11:58.0190000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
                 'tags': 'notcodeflow,start,balance_graph,api'},
            20: {'datetime': Timestamp('2023-09-29 18:11:58.0210000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            21: {'datetime': Timestamp('2023-09-29 18:11:58.0220000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            22: {'datetime': Timestamp('2023-09-29 18:11:58.0230000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            23: {'datetime': Timestamp('2023-09-29 18:11:58.0240000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.datafields.Aggregator.aggregate started... #codeflow#start#Aggregator.aggregate#data#transactions#process',
                 'tags': 'codeflow,start,Aggregator.aggregate,data,transactions,process'},
            24: {'datetime': Timestamp('2023-09-29 18:11:58.0250000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            25: {'datetime': Timestamp('2023-09-29 18:11:58.0260000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            26: {'datetime': Timestamp('2023-09-29 18:11:58.0270000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            27: {'datetime': Timestamp('2023-09-29 18:11:58.0280000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            28: {'datetime': Timestamp('2023-09-29 18:11:58.0290000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.datafields.Aggregator.aggregate finished... #codeflow#end#Aggregator.aggregate#data#transactions#process',
                 'tags': 'codeflow,end,Aggregator.aggregate,data,transactions,process'},
            29: {'datetime': Timestamp('2023-09-29 18:11:58.0300000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.datafields.DataframeWrapper.groupagg finished... #codeflow#end#DataframeWrapper.groupagg#data#transactions#groupagg',
                 'tags': 'codeflow,end,DataframeWrapper.groupagg,data,transactions,groupagg'},
            30: {'datetime': Timestamp('2023-09-29 18:11:58.0310000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.blueprints.reports.reports_bp.get_filtered_transactions finished... #codeflow#end#get_filtered_transactions#data#transactions#process',
                 'tags': 'codeflow,end,get_filtered_transactions,data,transactions,process'},
            31: {'datetime': Timestamp('2023-09-29 18:11:58.0320000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon2.blueprints.reports.reports_bp.balance_graph finished... #codeflow#end#balance_graph#api',
                 'tags': 'codeflow,end,balance_graph,api'}
        }, orient='index')
        log_data_obj = log_data.LogData(log_data_df)

        expected_performance_df = pd.DataFrame.from_dict({
            1: {'datetime': Timestamp('2023-09-29 18:11:58.00200000'),
                'execution_time': 30,
                'tags': 'balance_graph,api'},
            2: {'datetime': Timestamp('2023-09-29 18:11:58.00300000'),
                'execution_time': 28,
                'tags': 'get_filtered_transactions,data,transactions,process'},
            3: {'datetime': Timestamp('2023-09-29 18:11:58.00400000'),
                'execution_time': 3,
                'tags': 'get_transactions,data,transactions,load'},
            4: {'datetime': Timestamp('2023-09-29 18:11:58.00500000'),
                'execution_time': 1,
                'tags': 'Transactions.__init__,data,transactions,process'},
            7: {'datetime': Timestamp('2023-09-29 18:11:58.00800000'),
                'execution_time': 4,
                'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
            8: {'datetime': Timestamp('2023-09-29 18:11:58.00900000'),
                'execution_time': 2,
                'tags': 'Transactions.__init__,data,transactions,process'},
            12: {'datetime': Timestamp('2023-09-29 18:11:58.0130000'),
                 'execution_time': 3,
                 'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
            13: {'datetime': Timestamp('2023-09-29 18:11:58.0140000'),
                 'execution_time': 1,
                 'tags': 'Transactions.__init__,data,transactions,process'},
            16: {'datetime': Timestamp('2023-09-29 18:11:58.0170000'),
                 'execution_time': 13,
                 'tags': 'DataframeWrapper.groupagg,data,transactions,groupagg'},
            18: {'datetime': Timestamp('2023-09-29 18:11:58.0190000'),
                 'execution_time': None,
                 'tags': 'Grouping.group,data,transactions,process'},
            20: {'datetime': Timestamp('2023-09-29 18:11:58.0210000'),
                 'execution_time': None,
                 'tags': 'Transactions.__init__,data,transactions,process'},
            21: {'datetime': Timestamp('2023-09-29 18:11:58.0220000'),
                 'execution_time': 1,
                 'tags': 'Transactions.__init__,data,transactions,process'},
            23: {'datetime': Timestamp('2023-09-29 18:11:58.0240000'),
                 'execution_time': 5,
                 'tags': 'Aggregator.aggregate,data,transactions,process'},
            24: {'datetime': Timestamp('2023-09-29 18:11:58.0250000'),
                 'execution_time': None,
                 'tags': 'Transactions.__init__,data,transactions,process'},
            25: {'datetime': Timestamp('2023-09-29 18:11:58.0260000'),
                 'execution_time': None,
                 'tags': 'Transactions.__init__,data,transactions,process'},
            26: {'datetime': Timestamp('2023-09-29 18:11:58.0270000'),
                 'execution_time': 1,
                 'tags': 'Transactions.__init__,data,transactions,process'},
        }, orient='index')

        result_performance_df = log_data.PerformanceData.from_log_data(log_data_obj).dataframe()

        pd.testing.assert_frame_equal(result_performance_df.reset_index(drop=True),
                                      expected_performance_df.reset_index(drop=True))


if __name__ == '__main__':
    unittest.main()
