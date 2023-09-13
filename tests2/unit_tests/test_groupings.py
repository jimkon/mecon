import unittest
from datetime import datetime

import pandas as pd

from datafields import DataframeWrapper
from groupings import LabelGroupingABC
from mecon2 import groupings as gp
from mecon2 import datafields


# class TestUtilFunctions(unittest.TestCase):  TODO clean
#     def test_series_date_to_str(self):
#         test_series = pd.DataFrame({'datetime': [datetime(2021, 3, 1, 0, 0, 0),
#                                                  datetime(2021, 2, 2, 0, 0, 0),
#                                                  datetime(2021, 1, 3, 0, 0, 0),
#                                                  ]})
#         expected_date_str = pd.Series([
#             '20210301',
#             '20210202',
#             '20210103',
#         ])
#         self.assertEqual(gp._series_date_to_str(test_series).to_list(),
#                          expected_date_str.to_list())


class TestLabelGrouping(unittest.TestCase):
    def test_group(self):
        class CustomGrouping(LabelGroupingABC):
            def labels(self, df_wrapper: DataframeWrapper):
                return pd.Series(['a', 'b', 'b', 'c', 'b'])

        data = {'A': [1, 2, 3, 4, 5],
                'B': [6, 7, 8, 9, 10]}
        df = pd.DataFrame(data)
        wrapper = DataframeWrapper(df)
        grouper = CustomGrouping('temp_name')

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [1],
                                                    'B': [6]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [2, 3, 5],
                                                    'B': [7, 8, 10]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [4],
                                                    'B': [9]}))


class TestGrouping(unittest.TestCase):
    def test_day_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 1, 12, 30, 30),
                         datetime(2021, 1, 1, 23, 59, 59),
                         datetime(2021, 1, 2, 0, 0, 0),
                         datetime(2021, 1, 3, 0, 0, 0),
                         ],
            'B': [6, 7, 8, 9, 10]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.DAY

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 1, 12, 30, 30),
                                                                 datetime(2021, 1, 1, 23, 59, 59),
                                                                 ],
                                                    'B': [6, 7, 8]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 2, 0, 0, 0)],
                                                    'B': [9]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 3, 0, 0, 0)],
                                                    'B': [10]}))

    def test_week_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2021, 1, 8, 23, 59, 59),
                         datetime(2021, 1, 15, 0, 0, 0),
                         ],
            'B': [6, 7, 8, 9]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.WEEK

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 2, 12, 30, 30)],
                                                    'B': [6, 7]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 8, 23, 59, 59)],
                                                    'B': [8]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 15, 0, 0, 0)],
                                                    'B': [9]}))

    def test_month_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2021, 2, 8, 23, 59, 59),
                         datetime(2021, 3, 15, 0, 0, 0),
                         ],
            'B': [6, 7, 8, 9]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.MONTH

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 2, 12, 30, 30)],
                                                    'B': [6, 7]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 2, 8, 23, 59, 59)],
                                                    'B': [8]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 3, 15, 0, 0, 0)],
                                                    'B': [9]}))

    def test_year_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2022, 2, 8, 23, 59, 59),
                         datetime(2023, 3, 15, 0, 0, 0),
                         ],
            'B': [6, 7, 8, 9]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.YEAR

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 2, 12, 30, 30)],
                                                    'B': [6, 7]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2022, 2, 8, 23, 59, 59)],
                                                    'B': [8]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2023, 3, 15, 0, 0, 0)],
                                                    'B': [9]}))


if __name__ == '__main__':
    unittest.main()
