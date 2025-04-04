import unittest

import pandas as pd
from pandas import Timestamp

from mecon.monitoring import log_data


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


class TestPerformanceDataAggregator(unittest.TestCase):  # TODO test for legacy code
    def test_aggregation(self):
        logs_data_obj = log_data.LogData(pd.DataFrame({
            'datetime': [Timestamp('2023-09-29 18:11:58.298000'), Timestamp('2023-09-29 18:11:58.298000'),
                         Timestamp('2023-09-29 18:11:58.328000'), Timestamp('2023-09-29 18:11:58.329000'),
                         Timestamp('2023-09-29 18:11:58.330000'), Timestamp('2023-09-29 18:11:58.430000'),
                         Timestamp('2023-09-29 18:11:58.431000'), Timestamp('2023-09-29 18:11:58.432000'),
                         Timestamp('2023-09-29 18:11:58.433000'), Timestamp('2023-09-29 18:11:58.433000'),
                         Timestamp('2023-09-29 18:11:58.438000'), Timestamp('2023-09-29 18:11:58.439000'),
                         Timestamp('2023-09-29 18:11:58.539000')],
            'level': ['DEBUG', 'INFO', 'DEBUG', 'INFO', 'DEBUG', 'INFO', 'DEBUG', 'DEBUG', 'INFO', 'DEBUG', 'DEBUG',
                      'DEBUG',
                      'INFO'],  # same for all
            'module': ['logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs',
                       'logs'],  # same for all
            'funcName': ['wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper',  # same for all
                         'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper'],
            'description': [
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process'],
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
                                    'execution_time': [0, 1, 100, -1, 1, -1, -1, 100],
                                    'tags': ['Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process']})

        pd.testing.assert_frame_equal(perf_df, expected_df)


class TestPerformanceDataAggregatorV2(unittest.TestCase):
    def test_aggregation(self):
        logs_data_obj = log_data.LogData(pd.DataFrame({
            'datetime': [Timestamp('2023-09-29 18:11:58.298000'), Timestamp('2023-09-29 18:11:58.298000'),
                         Timestamp('2023-09-29 18:11:58.328000'), Timestamp('2023-09-29 18:11:58.329000'),
                         Timestamp('2023-09-29 18:11:58.330000'), Timestamp('2023-09-29 18:11:58.430000'),
                         Timestamp('2023-09-29 18:11:58.431000'), Timestamp('2023-09-29 18:11:58.432000'),
                         Timestamp('2023-09-29 18:11:58.433000'), Timestamp('2023-09-29 18:11:58.433000'),
                         Timestamp('2023-09-29 18:11:58.438000'), Timestamp('2023-09-29 18:11:58.439000'),
                         Timestamp('2023-09-29 18:11:58.539000')],
            'level': ['DEBUG', 'INFO', 'DEBUG', 'INFO', 'DEBUG', 'INFO', 'DEBUG', 'DEBUG', 'INFO', 'DEBUG', 'DEBUG',
                      'DEBUG',
                      'INFO'],  # same for all
            'module': ['logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs', 'logs',
                       'logs'],  # same for all
            'funcName': ['wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper',  # same for all
                         'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper', 'wrapper'],
            'description': [
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished.  (exec_dur=0.0005 seconds) #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished.  (exec_dur=0.0015 seconds) #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished.  (exec_dur=0.1005 seconds) #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished.  (exec_dur=0.0015 seconds) #codeflow#end#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'mecon.transactions.Transactions.__init__ finished.  (exec_dur=0.1005 seconds) #codeflow#end#Transactions.__init__#data#transactions#process'],
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
        aggregator = log_data.PerformanceDataAggregatorV2()
        perf_df = aggregator.aggregation(logs_data_obj).dataframe()

        expected_df = pd.DataFrame({'datetime': [Timestamp('2023-09-29 18:11:58.297500'),
                                                 Timestamp('2023-09-29 18:11:58.327500'),
                                                 Timestamp('2023-09-29 18:11:58.329500'),
                                                 Timestamp('2023-09-29 18:11:58.431500'),
                                                 Timestamp('2023-09-29 18:11:58.438500')],
                                    'execution_time': [.5, 1.5, 100.5, 1.5, 100.5],
                                    'tags': ['Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process',
                                             'Transactions.__init__,data,transactions,process']})

        pd.testing.assert_frame_equal(perf_df, expected_df)


# class TestPerformanceData(unittest.TestCase):  # TODO test for legacy code
#     def setUp(self) -> None:
#         self.example_perf_data = log_data.PerformanceData(pd.DataFrame.from_dict({
#             1: {'datetime': Timestamp('2023-09-29 18:11:58.00200000'),
#                 'execution_time': 30,
#                 'tags': 'balance_graph,api'},
#             2: {'datetime': Timestamp('2023-09-29 18:11:58.00300000'),
#                 'execution_time': 28,
#                 'tags': 'get_filtered_transactions,data,transactions,process'},
#             3: {'datetime': Timestamp('2023-09-29 18:11:58.00400000'),
#                 'execution_time': 3,
#                 'tags': 'get_transactions,data,transactions,load'},
#             4: {'datetime': Timestamp('2023-09-29 18:11:58.00500000'),
#                 'execution_time': 1,
#                 'tags': 'Transactions.__init__,data,transactions,process'},
#             7: {'datetime': Timestamp('2023-09-29 18:11:58.00800000'),
#                 'execution_time': 4,
#                 'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
#             8: {'datetime': Timestamp('2023-09-29 18:11:58.00900000'),
#                 'execution_time': 2,
#                 'tags': 'Transactions.__init__,data,transactions,process'},
#             12: {'datetime': Timestamp('2023-09-29 18:11:58.0130000'),
#                  'execution_time': 3,
#                  'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
#             13: {'datetime': Timestamp('2023-09-29 18:11:58.0140000'),
#                  'execution_time': 1,
#                  'tags': 'Transactions.__init__,data,transactions,process'},
#             16: {'datetime': Timestamp('2023-09-29 18:11:58.0170000'),
#                  'execution_time': 13,
#                  'tags': 'DataframeWrapper.groupagg,data,transactions,groupagg'},
#             18: {'datetime': Timestamp('2023-09-29 18:11:58.0190000'),
#                  'execution_time': -1,
#                  'tags': 'Grouping.group,data,transactions,process'},
#             20: {'datetime': Timestamp('2023-09-29 18:11:58.0210000'),
#                  'execution_time': -1,
#                  'tags': 'Transactions.__init__,data,transactions,process'},
#             21: {'datetime': Timestamp('2023-09-29 18:11:58.0220000'),
#                  'execution_time': 1,
#                  'tags': 'Transactions.__init__,data,transactions,process'},
#             23: {'datetime': Timestamp('2023-09-29 18:11:58.0240000'),
#                  'execution_time': 5,
#                  'tags': 'Aggregator.aggregate,data,transactions,process'},
#             24: {'datetime': Timestamp('2023-09-29 18:11:58.0250000'),
#                  'execution_time': -1,
#                  'tags': 'Transactions.__init__,data,transactions,process'},
#             25: {'datetime': Timestamp('2023-09-29 18:11:58.0260000'),
#                  'execution_time': -1,
#                  'tags': 'Transactions.__init__,data,transactions,process'},
#             26: {'datetime': Timestamp('2023-09-29 18:11:58.0270000'),
#                  'execution_time': 1,
#                  'tags': 'Transactions.__init__,data,transactions,process'},
#         }, orient='index')
#         )
#
#     def test_from_log_data(self):
#         log_data_df = pd.DataFrame.from_dict({
#             0: {'datetime': Timestamp('2023-09-29 18:11:58.00100000'), 'level': 'INFO', 'module': 'logs',
#                 'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
#                 'tags': 'notcodeflow,start,balance_graph,api'},
#             1: {'datetime': Timestamp('2023-09-29 18:11:58.00200000'), 'level': 'DEBUG', 'module': 'logs',
#                 'funcName': 'wrapper',
#                 'description': 'mecon.blueprints.reports.reports_bp.balance_graph started... #codeflow#start#balance_graph#api',
#                 'tags': 'codeflow,start,balance_graph,api'},
#             2: {'datetime': Timestamp('2023-09-29 18:11:58.00300000'), 'level': 'DEBUG', 'module': 'logs',
#                 'funcName': 'wrapper',
#                 'description': 'mecon.blueprints.reports.reports_bp.get_filtered_transactions started... #codeflow#start#get_filtered_transactions#data#transactions#process',
#                 'tags': 'codeflow,start,get_filtered_transactions,data,transactions,process'},
#             3: {'datetime': Timestamp('2023-09-29 18:11:58.00400000'), 'level': 'DEBUG', 'module': 'logs',
#                 'funcName': 'wrapper',
#                 'description': 'mecon.blueprints.reports.reports_bp.get_transactions started... #codeflow#start#get_transactions#data#transactions#load',
#                 'tags': 'codeflow,start,get_transactions,data,transactions,load'},
#             4: {'datetime': Timestamp('2023-09-29 18:11:58.00500000'), 'level': 'DEBUG', 'module': 'logs',
#                 'funcName': 'wrapper',
#                 'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
#                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
#             5: {'datetime': Timestamp('2023-09-29 18:11:58.00600000'), 'level': 'INFO', 'module': 'logs',
#                 'funcName': 'wrapper',
#                 'description': 'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
#                 'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
#             6: {'datetime': Timestamp('2023-09-29 18:11:58.00700000'), 'level': 'INFO', 'module': 'logs',
#                 'funcName': 'wrapper',
#                 'description': 'mecon.blueprints.reports.reports_bp.get_transactions finished... #codeflow#end#get_transactions#data#transactions#load',
#                 'tags': 'codeflow,end,get_transactions,data,transactions,load'},
#             7: {'datetime': Timestamp('2023-09-29 18:11:58.00800000'), 'level': 'DEBUG', 'module': 'logs',
#                 'funcName': 'wrapper',
#                 'description': 'mecon.datafields.DataframeWrapper.apply_rule started... #codeflow#start#DataframeWrapper.apply_rule#data#transactions#tags',
#                 'tags': 'codeflow,start,DataframeWrapper.apply_rule,data,transactions,tags'},
#             8: {'datetime': Timestamp('2023-09-29 18:11:58.00900000'), 'level': 'DEBUG', 'module': 'logs',
#                 'funcName': 'wrapper',
#                 'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
#                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
#             9: {'datetime': Timestamp('2023-09-29 18:11:58.0100000'), 'level': 'INFO', 'module': 'logs',
#                 'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
#                 'tags': 'notcodeflow,start,balance_graph,api'},
#             10: {'datetime': Timestamp('2023-09-29 18:11:58.0110000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
#             11: {'datetime': Timestamp('2023-09-29 18:11:58.0120000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.datafields.DataframeWrapper.apply_rule finished... #codeflow#end#DataframeWrapper.apply_rule#data#transactions#tags',
#                  'tags': 'codeflow,end,DataframeWrapper.apply_rule,data,transactions,tags'},
#             12: {'datetime': Timestamp('2023-09-29 18:11:58.0130000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.datafields.DataframeWrapper.apply_rule started... #codeflow#start#DataframeWrapper.apply_rule#data#transactions#tags',
#                  'tags': 'codeflow,start,DataframeWrapper.apply_rule,data,transactions,tags'},
#             13: {'datetime': Timestamp('2023-09-29 18:11:58.0140000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
#             14: {'datetime': Timestamp('2023-09-29 18:11:58.0150000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
#             15: {'datetime': Timestamp('2023-09-29 18:11:58.0160000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.datafields.DataframeWrapper.apply_rule finished... #codeflow#end#DataframeWrapper.apply_rule#data#transactions#tags',
#                  'tags': 'codeflow,end,DataframeWrapper.apply_rule,data,transactions,tags'},
#             16: {'datetime': Timestamp('2023-09-29 18:11:58.0170000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.datafields.DataframeWrapper.groupagg started... #codeflow#start#DataframeWrapper.groupagg#data#transactions#groupagg',
#                  'tags': 'codeflow,start,DataframeWrapper.groupagg,data,transactions,groupagg'},
#             17: {'datetime': Timestamp('2023-09-29 18:11:58.0180000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
#                  'tags': 'notcodeflow,start,balance_graph,api'},
#             18: {'datetime': Timestamp('2023-09-29 18:11:58.0190000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.datafields.Grouping.group started... #codeflow#start#Grouping.group#data#transactions#process',
#                  'tags': 'codeflow,start,Grouping.group,data,transactions,process'},
#             19: {'datetime': Timestamp('2023-09-29 18:11:58.0190000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
#                  'tags': 'notcodeflow,start,balance_graph,api'},
#             20: {'datetime': Timestamp('2023-09-29 18:11:58.0210000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
#             21: {'datetime': Timestamp('2023-09-29 18:11:58.0220000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
#             22: {'datetime': Timestamp('2023-09-29 18:11:58.0230000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
#             23: {'datetime': Timestamp('2023-09-29 18:11:58.0240000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.datafields.Aggregator.aggregate started... #codeflow#start#Aggregator.aggregate#data#transactions#process',
#                  'tags': 'codeflow,start,Aggregator.aggregate,data,transactions,process'},
#             24: {'datetime': Timestamp('2023-09-29 18:11:58.0250000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
#             25: {'datetime': Timestamp('2023-09-29 18:11:58.0260000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
#             26: {'datetime': Timestamp('2023-09-29 18:11:58.0270000'), 'level': 'DEBUG', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
#             27: {'datetime': Timestamp('2023-09-29 18:11:58.0280000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.transactions.Transactions.__init__ finished... #codeflow#end#Transactions.__init__#data#transactions#process',
#                  'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
#             28: {'datetime': Timestamp('2023-09-29 18:11:58.0290000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.datafields.Aggregator.aggregate finished... #codeflow#end#Aggregator.aggregate#data#transactions#process',
#                  'tags': 'codeflow,end,Aggregator.aggregate,data,transactions,process'},
#             29: {'datetime': Timestamp('2023-09-29 18:11:58.0300000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.datafields.DataframeWrapper.groupagg finished... #codeflow#end#DataframeWrapper.groupagg#data#transactions#groupagg',
#                  'tags': 'codeflow,end,DataframeWrapper.groupagg,data,transactions,groupagg'},
#             30: {'datetime': Timestamp('2023-09-29 18:11:58.0310000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.blueprints.reports.reports_bp.get_filtered_transactions finished... #codeflow#end#get_filtered_transactions#data#transactions#process',
#                  'tags': 'codeflow,end,get_filtered_transactions,data,transactions,process'},
#             31: {'datetime': Timestamp('2023-09-29 18:11:58.0320000'), 'level': 'INFO', 'module': 'logs',
#                  'funcName': 'wrapper',
#                  'description': 'mecon.blueprints.reports.reports_bp.balance_graph finished... #codeflow#end#balance_graph#api',
#                  'tags': 'codeflow,end,balance_graph,api'}
#         }, orient='index')
#         log_data_obj = log_data.LogData(log_data_df)
#
#         expected_performance_df = self.example_perf_data.dataframe()
#
#         result_performance_df = log_data.PerformanceData.from_log_data(log_data_obj).dataframe()
#
#         pd.testing.assert_frame_equal(result_performance_df.reset_index(drop=True),
#                                       expected_performance_df.reset_index(drop=True))
#
#     def test_execution_time(self):
#         self.assertListEqual(self.example_perf_data.execution_time.to_list(),
#                              [30, 28, 3, 1, 4, 2, 3, 1, 13, -1, -1, 1, 5, -1, -1, 1])
#
#     def test_start_datetime(self):
#         self.assertListEqual(self.example_perf_data.start_datetime.to_list(),
#                              [Timestamp('2023-09-29 18:11:58.002000'), Timestamp('2023-09-29 18:11:58.003000'),
#                               Timestamp('2023-09-29 18:11:58.004000'), Timestamp('2023-09-29 18:11:58.005000'),
#                               Timestamp('2023-09-29 18:11:58.008000'), Timestamp('2023-09-29 18:11:58.009000'),
#                               Timestamp('2023-09-29 18:11:58.013000'), Timestamp('2023-09-29 18:11:58.014000'),
#                               Timestamp('2023-09-29 18:11:58.017000'), Timestamp('2023-09-29 18:11:58.019000'),
#                               Timestamp('2023-09-29 18:11:58.021000'), Timestamp('2023-09-29 18:11:58.022000'),
#                               Timestamp('2023-09-29 18:11:58.024000'), Timestamp('2023-09-29 18:11:58.025000'),
#                               Timestamp('2023-09-29 18:11:58.026000'), Timestamp('2023-09-29 18:11:58.027000')])
#
#     def test_end_datetime(self):
#         self.assertListEqual(self.example_perf_data.end_datetime.to_list(),
#                              [Timestamp('2023-09-29 18:11:58.032000'), Timestamp('2023-09-29 18:11:58.031000'),
#                               Timestamp('2023-09-29 18:11:58.007000'), Timestamp('2023-09-29 18:11:58.006000'),
#                               Timestamp('2023-09-29 18:11:58.012000'), Timestamp('2023-09-29 18:11:58.011000'),
#                               Timestamp('2023-09-29 18:11:58.016000'), Timestamp('2023-09-29 18:11:58.015000'),
#                               Timestamp('2023-09-29 18:11:58.030000'), Timestamp('2023-09-29 18:11:58.018000'),
#                               Timestamp('2023-09-29 18:11:58.020000'), Timestamp('2023-09-29 18:11:58.023000'),
#                               Timestamp('2023-09-29 18:11:58.029000'), Timestamp('2023-09-29 18:11:58.024000'),
#                               Timestamp('2023-09-29 18:11:58.025000'), Timestamp('2023-09-29 18:11:58.028000')])
#
#     def test_is_finished(self):
#         self.assertListEqual(self.example_perf_data.is_finished.to_list(),
#                              [True, True, True, True, True, True, True, True, True, False, False, True, True, False,
#                               False, True])
#
#     def test_finished(self):
#         expected_df = pd.DataFrame.from_dict({
#             1: {'datetime': Timestamp('2023-09-29 18:11:58.00200000'),
#                 'execution_time': 30,
#                 'tags': 'balance_graph,api'},
#             2: {'datetime': Timestamp('2023-09-29 18:11:58.00300000'),
#                 'execution_time': 28,
#                 'tags': 'get_filtered_transactions,data,transactions,process'},
#             3: {'datetime': Timestamp('2023-09-29 18:11:58.00400000'),
#                 'execution_time': 3,
#                 'tags': 'get_transactions,data,transactions,load'},
#             4: {'datetime': Timestamp('2023-09-29 18:11:58.00500000'),
#                 'execution_time': 1,
#                 'tags': 'Transactions.__init__,data,transactions,process'},
#             7: {'datetime': Timestamp('2023-09-29 18:11:58.00800000'),
#                 'execution_time': 4,
#                 'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
#             8: {'datetime': Timestamp('2023-09-29 18:11:58.00900000'),
#                 'execution_time': 2,
#                 'tags': 'Transactions.__init__,data,transactions,process'},
#             12: {'datetime': Timestamp('2023-09-29 18:11:58.0130000'),
#                  'execution_time': 3,
#                  'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
#             13: {'datetime': Timestamp('2023-09-29 18:11:58.0140000'),
#                  'execution_time': 1,
#                  'tags': 'Transactions.__init__,data,transactions,process'},
#             16: {'datetime': Timestamp('2023-09-29 18:11:58.0170000'),
#                  'execution_time': 13,
#                  'tags': 'DataframeWrapper.groupagg,data,transactions,groupagg'},
#             21: {'datetime': Timestamp('2023-09-29 18:11:58.0220000'),
#                  'execution_time': 1,
#                  'tags': 'Transactions.__init__,data,transactions,process'},
#             23: {'datetime': Timestamp('2023-09-29 18:11:58.0240000'),
#                  'execution_time': 5,
#                  'tags': 'Aggregator.aggregate,data,transactions,process'},
#             26: {'datetime': Timestamp('2023-09-29 18:11:58.0270000'),
#                  'execution_time': 1,
#                  'tags': 'Transactions.__init__,data,transactions,process'},
#         }, orient='index')
#
#         pd.testing.assert_frame_equal(self.example_perf_data.finished().dataframe().reset_index(drop=True),
#                                       expected_df.reset_index(drop=True))
#
#     def test_isolate_function_tags(self):
#         self.assertListEqual(log_data._isolate_function_tags(pd.Series(['a', '', 'a,b', 'b,c'], name='tags')),
#                              ['a', '', 'a', 'b'])
#
#     def test_functions(self):
#         self.assertListEqual(self.example_perf_data.functions.to_list(),
#                              ['balance_graph', 'get_filtered_transactions',
#                               'get_transactions',
#                               'Transactions.__init__',
#                               'DataframeWrapper.apply_rule',
#                               'Transactions.__init__',
#                               'DataframeWrapper.apply_rule',
#                               'Transactions.__init__',
#                               'DataframeWrapper.groupagg',
#                               'Grouping.group',
#                               'Transactions.__init__',
#                               'Transactions.__init__',
#                               'Aggregator.aggregate',
#                               'Transactions.__init__',
#                               'Transactions.__init__',
#                               'Transactions.__init__'])
#
#     def test_distinct_function_tags(self):
#         self.assertListEqual(log_data._distinct_function_tags(pd.Series(['a', '', 'a,b', 'b,c'], name='tags')),
#                              ['a', '', 'b'])
#
#     def test_logged_functions(self):
#         self.assertListEqual(self.example_perf_data.logged_functions(),
#                              ['balance_graph', 'get_filtered_transactions',
#                               'get_transactions',
#                               'Transactions.__init__',
#                               'DataframeWrapper.apply_rule',
#                               'DataframeWrapper.groupagg',
#                               'Grouping.group',
#                               'Aggregator.aggregate'])


class TestPerformanceDataV2(unittest.TestCase):
    def setUp(self) -> None:
        self.example_perf_data = log_data.PerformanceData(pd.DataFrame.from_dict({
            1: {'datetime': Timestamp('2023-09-29 18:11:58.00200000'),
                'execution_time': 30.,
                'tags': 'balance_graph,api'},
            2: {'datetime': Timestamp('2023-09-29 18:11:58.00300000'),
                'execution_time': 28.,
                'tags': 'get_filtered_transactions,data,transactions,process'},
            3: {'datetime': Timestamp('2023-09-29 18:11:58.00400000'),
                'execution_time': 3.,
                'tags': 'get_transactions,data,transactions,load'},
            4: {'datetime': Timestamp('2023-09-29 18:11:58.00500000'),
                'execution_time': 1.,
                'tags': 'Transactions.__init__,data,transactions,process'},
            7: {'datetime': Timestamp('2023-09-29 18:11:58.00800000'),
                'execution_time': 4.,
                'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
            8: {'datetime': Timestamp('2023-09-29 18:11:58.00900000'),
                'execution_time': 2.,
                'tags': 'Transactions.__init__,data,transactions,process'},
            12: {'datetime': Timestamp('2023-09-29 18:11:58.0130000'),
                 'execution_time': 3.,
                 'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
            13: {'datetime': Timestamp('2023-09-29 18:11:58.0140000'),
                 'execution_time': 1.,
                 'tags': 'Transactions.__init__,data,transactions,process'},
            16: {'datetime': Timestamp('2023-09-29 18:11:58.0170000'),
                 'execution_time': 13.,
                 'tags': 'DataframeWrapper.groupagg,data,transactions,groupagg'},
            21: {'datetime': Timestamp('2023-09-29 18:11:58.0220000'),
                 'execution_time': 1.,
                 'tags': 'Transactions.__init__,data,transactions,process'},
            23: {'datetime': Timestamp('2023-09-29 18:11:58.0240000'),
                 'execution_time': 5.,
                 'tags': 'Aggregator.aggregate,data,transactions,process'},
            26: {'datetime': Timestamp('2023-09-29 18:11:58.0270000'),
                 'execution_time': 1.,
                 'tags': 'Transactions.__init__,data,transactions,process'},
        }, orient='index')
        )

    def test_from_log_data(self):
        log_data_df = pd.DataFrame.from_dict({
            0: {'datetime': Timestamp('2023-09-29 18:11:58.00100000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
                'tags': 'notcodeflow,start,balance_graph,api'},
            1: {'datetime': Timestamp('2023-09-29 18:11:58.00200000'), 'level': 'DEBUG', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon.blueprints.reports.reports_bp.balance_graph started... #codeflow#start#balance_graph#api',
                'tags': 'codeflow,start,balance_graph,api'},
            2: {'datetime': Timestamp('2023-09-29 18:11:58.00300000'), 'level': 'DEBUG', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon.blueprints.reports.reports_bp.get_filtered_transactions started... #codeflow#start#get_filtered_transactions#data#transactions#process',
                'tags': 'codeflow,start,get_filtered_transactions,data,transactions,process'},
            3: {'datetime': Timestamp('2023-09-29 18:11:58.00400000'), 'level': 'DEBUG', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon.blueprints.reports.reports_bp.get_transactions started... #codeflow#start#get_transactions#data#transactions#load',
                'tags': 'codeflow,start,get_transactions,data,transactions,load'},
            4: {'datetime': Timestamp('2023-09-29 18:11:58.00500000'), 'level': 'DEBUG', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            5: {'datetime': Timestamp('2023-09-29 18:11:58.00600000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon.transactions.Transactions.__init__ finished. (exec_dur=0.001 seconds) #codeflow#end#Transactions.__init__#data#transactions#process',
                'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            6: {'datetime': Timestamp('2023-09-29 18:11:58.00700000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon.blueprints.reports.reports_bp.get_transactions finished. (exec_dur=0.003 seconds) #codeflow#end#get_transactions#data#transactions#load',
                'tags': 'codeflow,end,get_transactions,data,transactions,load'},
            7: {'datetime': Timestamp('2023-09-29 18:11:58.00800000'), 'level': 'DEBUG', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon.datafields.DataframeWrapper.apply_rule started... #codeflow#start#DataframeWrapper.apply_rule#data#transactions#tags',
                'tags': 'codeflow,start,DataframeWrapper.apply_rule,data,transactions,tags'},
            8: {'datetime': Timestamp('2023-09-29 18:11:58.00900000'), 'level': 'DEBUG', 'module': 'logs',
                'funcName': 'wrapper',
                'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            9: {'datetime': Timestamp('2023-09-29 18:11:58.0100000'), 'level': 'INFO', 'module': 'logs',
                'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
                'tags': 'notcodeflow,start,balance_graph,api'},
            10: {'datetime': Timestamp('2023-09-29 18:11:58.0110000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ finished. (exec_dur=0.002 seconds) #codeflow#end#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            11: {'datetime': Timestamp('2023-09-29 18:11:58.0120000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.datafields.DataframeWrapper.apply_rule finished. (exec_dur=0.004 seconds) #codeflow#end#DataframeWrapper.apply_rule#data#transactions#tags',
                 'tags': 'codeflow,end,DataframeWrapper.apply_rule,data,transactions,tags'},
            12: {'datetime': Timestamp('2023-09-29 18:11:58.0130000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.datafields.DataframeWrapper.apply_rule started... #codeflow#start#DataframeWrapper.apply_rule#data#transactions#tags',
                 'tags': 'codeflow,start,DataframeWrapper.apply_rule,data,transactions,tags'},
            13: {'datetime': Timestamp('2023-09-29 18:11:58.0140000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            14: {'datetime': Timestamp('2023-09-29 18:11:58.0150000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ finished. (exec_dur=0.001 seconds) #codeflow#end#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            15: {'datetime': Timestamp('2023-09-29 18:11:58.0160000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.datafields.DataframeWrapper.apply_rule finished. (exec_dur=0.003 seconds) #codeflow#end#DataframeWrapper.apply_rule#data#transactions#tags',
                 'tags': 'codeflow,end,DataframeWrapper.apply_rule,data,transactions,tags'},
            16: {'datetime': Timestamp('2023-09-29 18:11:58.0170000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.datafields.DataframeWrapper.groupagg started... #codeflow#start#DataframeWrapper.groupagg#data#transactions#groupagg',
                 'tags': 'codeflow,start,DataframeWrapper.groupagg,data,transactions,groupagg'},
            17: {'datetime': Timestamp('2023-09-29 18:11:58.0180000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
                 'tags': 'notcodeflow,start,balance_graph,api'},
            18: {'datetime': Timestamp('2023-09-29 18:11:58.0190000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.datafields.Grouping.group started... #codeflow#start#Grouping.group#data#transactions#process',
                 'tags': 'codeflow,start,Grouping.group,data,transactions,process'},
            19: {'datetime': Timestamp('2023-09-29 18:11:58.0190000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper', 'description': 'a random log #notcodeflow#start#balance_graph#api',
                 'tags': 'notcodeflow,start,balance_graph,api'},
            20: {'datetime': Timestamp('2023-09-29 18:11:58.0210000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            21: {'datetime': Timestamp('2023-09-29 18:11:58.0220000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            22: {'datetime': Timestamp('2023-09-29 18:11:58.0230000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ finished. (exec_dur=0.001 seconds) #codeflow#end#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            23: {'datetime': Timestamp('2023-09-29 18:11:58.0240000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.datafields.Aggregator.aggregate started... #codeflow#start#Aggregator.aggregate#data#transactions#process',
                 'tags': 'codeflow,start,Aggregator.aggregate,data,transactions,process'},
            24: {'datetime': Timestamp('2023-09-29 18:11:58.0250000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            25: {'datetime': Timestamp('2023-09-29 18:11:58.0260000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            26: {'datetime': Timestamp('2023-09-29 18:11:58.0270000'), 'level': 'DEBUG', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ started... #codeflow#start#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,start,Transactions.__init__,data,transactions,process'},
            27: {'datetime': Timestamp('2023-09-29 18:11:58.0280000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.transactions.Transactions.__init__ finished. (exec_dur=0.001 seconds) #codeflow#end#Transactions.__init__#data#transactions#process',
                 'tags': 'codeflow,end,Transactions.__init__,data,transactions,process'},
            28: {'datetime': Timestamp('2023-09-29 18:11:58.0290000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.datafields.Aggregator.aggregate finished. (exec_dur=0.005 seconds) #codeflow#end#Aggregator.aggregate#data#transactions#process',
                 'tags': 'codeflow,end,Aggregator.aggregate,data,transactions,process'},
            29: {'datetime': Timestamp('2023-09-29 18:11:58.0300000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.datafields.DataframeWrapper.groupagg finished. (exec_dur=0.013 seconds) #codeflow#end#DataframeWrapper.groupagg#data#transactions#groupagg',
                 'tags': 'codeflow,end,DataframeWrapper.groupagg,data,transactions,groupagg'},
            30: {'datetime': Timestamp('2023-09-29 18:11:58.0310000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.blueprints.reports.reports_bp.get_filtered_transactions finished. (exec_dur=0.028 seconds) #codeflow#end#get_filtered_transactions#data#transactions#process',
                 'tags': 'codeflow,end,get_filtered_transactions,data,transactions,process'},
            31: {'datetime': Timestamp('2023-09-29 18:11:58.0320000'), 'level': 'INFO', 'module': 'logs',
                 'funcName': 'wrapper',
                 'description': 'mecon.blueprints.reports.reports_bp.balance_graph finished. (exec_dur=0.030 seconds) #codeflow#end#balance_graph#api',
                 'tags': 'codeflow,end,balance_graph,api'}
        }, orient='index')
        log_data_obj = log_data.LogData(log_data_df)

        expected_performance_df = self.example_perf_data.dataframe()

        result_performance_df = log_data.PerformanceData.from_log_data(log_data_obj).dataframe()

        pd.testing.assert_frame_equal(result_performance_df.reset_index(drop=True),
                                      expected_performance_df.reset_index(drop=True))

    def test_execution_time(self):
        self.assertListEqual(self.example_perf_data.execution_time.to_list(),
                             [30., 28., 3., 1., 4., 2., 3., 1., 13., 1., 5., 1.])

    def test_start_datetime(self):
        self.assertListEqual(self.example_perf_data.start_datetime.to_list(),
                             [Timestamp('2023-09-29 18:11:58.002000'), Timestamp('2023-09-29 18:11:58.003000'),
                              Timestamp('2023-09-29 18:11:58.004000'), Timestamp('2023-09-29 18:11:58.005000'),
                              Timestamp('2023-09-29 18:11:58.008000'), Timestamp('2023-09-29 18:11:58.009000'),
                              Timestamp('2023-09-29 18:11:58.013000'), Timestamp('2023-09-29 18:11:58.014000'),
                              Timestamp('2023-09-29 18:11:58.017000'), Timestamp('2023-09-29 18:11:58.022000'),
                              Timestamp('2023-09-29 18:11:58.024000'), Timestamp('2023-09-29 18:11:58.027000')])

    def test_end_datetime(self):
        self.assertListEqual(self.example_perf_data.end_datetime.to_list(),
                             [Timestamp('2023-09-29 18:11:58.032000'), Timestamp('2023-09-29 18:11:58.031000'),
                              Timestamp('2023-09-29 18:11:58.007000'), Timestamp('2023-09-29 18:11:58.006000'),
                              Timestamp('2023-09-29 18:11:58.012000'), Timestamp('2023-09-29 18:11:58.011000'),
                              Timestamp('2023-09-29 18:11:58.016000'), Timestamp('2023-09-29 18:11:58.015000'),
                              Timestamp('2023-09-29 18:11:58.030000'), Timestamp('2023-09-29 18:11:58.023000'),
                              Timestamp('2023-09-29 18:11:58.029000'), Timestamp('2023-09-29 18:11:58.028000')])

    def test_is_finished(self):
        self.assertListEqual(self.example_perf_data.is_finished.to_list(),
                             [True, True, True, True, True, True, True, True, True, True, True, True])

    # def test_finished(self):  TODO is this redundant? this dict seems to be identical to the one on the setUp method
    #     expected_df = pd.DataFrame.from_dict({
    #         1: {'datetime': Timestamp('2023-09-29 18:11:58.00200000'),
    #             'execution_time': 30,
    #             'tags': 'balance_graph,api'},
    #         2: {'datetime': Timestamp('2023-09-29 18:11:58.00300000'),
    #             'execution_time': 28,
    #             'tags': 'get_filtered_transactions,data,transactions,process'},
    #         3: {'datetime': Timestamp('2023-09-29 18:11:58.00400000'),
    #             'execution_time': 3,
    #             'tags': 'get_transactions,data,transactions,load'},
    #         4: {'datetime': Timestamp('2023-09-29 18:11:58.00500000'),
    #             'execution_time': 1,
    #             'tags': 'Transactions.__init__,data,transactions,process'},
    #         7: {'datetime': Timestamp('2023-09-29 18:11:58.00800000'),
    #             'execution_time': 4,
    #             'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
    #         8: {'datetime': Timestamp('2023-09-29 18:11:58.00900000'),
    #             'execution_time': 2,
    #             'tags': 'Transactions.__init__,data,transactions,process'},
    #         12: {'datetime': Timestamp('2023-09-29 18:11:58.0130000'),
    #              'execution_time': 3,
    #              'tags': 'DataframeWrapper.apply_rule,data,transactions,tags'},
    #         13: {'datetime': Timestamp('2023-09-29 18:11:58.0140000'),
    #              'execution_time': 1,
    #              'tags': 'Transactions.__init__,data,transactions,process'},
    #         16: {'datetime': Timestamp('2023-09-29 18:11:58.0170000'),
    #              'execution_time': 13,
    #              'tags': 'DataframeWrapper.groupagg,data,transactions,groupagg'},
    #         21: {'datetime': Timestamp('2023-09-29 18:11:58.0220000'),
    #              'execution_time': 1,
    #              'tags': 'Transactions.__init__,data,transactions,process'},
    #         23: {'datetime': Timestamp('2023-09-29 18:11:58.0240000'),
    #              'execution_time': 5,
    #              'tags': 'Aggregator.aggregate,data,transactions,process'},
    #         26: {'datetime': Timestamp('2023-09-29 18:11:58.0270000'),
    #              'execution_time': 1,
    #              'tags': 'Transactions.__init__,data,transactions,process'},
    #     }, orient='index')
    #
    #     pd.testing.assert_frame_equal(self.example_perf_data.finished().dataframe().reset_index(drop=True),
    #                                   expected_df.reset_index(drop=True))

    def test_isolate_function_tags(self):
        self.assertListEqual(log_data._isolate_function_tags(pd.Series(['a', '', 'a,b', 'b,c'], name='tags')),
                             ['a', '', 'a', 'b'])

    def test_functions(self):
        self.assertListEqual(self.example_perf_data.functions.to_list(),
                             ['balance_graph', 'get_filtered_transactions',
                              'get_transactions',
                              'Transactions.__init__',
                              'DataframeWrapper.apply_rule',
                              'Transactions.__init__',
                              'DataframeWrapper.apply_rule',
                              'Transactions.__init__',
                              'DataframeWrapper.groupagg',
                              'Transactions.__init__',
                              'Aggregator.aggregate',
                              'Transactions.__init__'])

    def test_distinct_function_tags(self):
        self.assertListEqual(log_data._distinct_function_tags(pd.Series(['a', '', 'a,b', 'b,c'], name='tags')),
                             ['a', '', 'b'])

    def test_logged_functions(self):
        self.assertListEqual(self.example_perf_data.logged_functions(),
                             ['balance_graph', 'get_filtered_transactions',
                              'get_transactions',
                              'Transactions.__init__',
                              'DataframeWrapper.apply_rule',
                              'DataframeWrapper.groupagg',
                              'Aggregator.aggregate'])



if __name__ == '__main__':
    unittest.main()
