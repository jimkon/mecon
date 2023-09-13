import unittest

import pandas as pd

from mecon2.datafields import DataframeWrapper, Grouping, Aggregator


class TestDataframeWrapper(unittest.TestCase):
    def setUp(self):
        data = {'A': [1, 2, 3],
                'B': [4, 5, 6]}
        self.df = pd.DataFrame(data)
        self.wrapper = DataframeWrapper(self.df)

    def test_dataframe(self):
        self.assertEqual(self.wrapper.dataframe().equals(self.df), True)

    def test_copy(self):
        copy_wrapper = self.wrapper.copy()
        self.assertIsNot(copy_wrapper, self.wrapper)
        self.assertTrue(copy_wrapper.dataframe().equals(self.df))

    def test_merge(self):
        data2 = {'A': [3, 4],
                 'B': [6, 7]}
        df2 = pd.DataFrame(data2)
        df2_wrapper = DataframeWrapper(df2)
        merged_wrapper = self.wrapper.merge(df2_wrapper)
        expected_data = {'A': [1, 2, 3, 4],
                         'B': [4, 5, 6, 7]}
        expected_df = pd.DataFrame(expected_data).reset_index(drop=True)
        pd.testing.assert_frame_equal(merged_wrapper.dataframe().reset_index(drop=True), expected_df)

    def test_size(self):
        self.assertEqual(self.wrapper.size(), len(self.df))

    def test_select_by_index(self):
        index = [True, False, True]
        selected_wrapper = self.wrapper.select_by_index(index)
        expected_data = {'A': [1, 3],
                         'B': [4, 6]}
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(selected_wrapper.dataframe().reset_index(drop=True), expected_df)


class TestGrouping(unittest.TestCase):
    def test_group(self):
        class CustomGrouping(Grouping):

            def compute_group_indexes(self, df_wrapper: DataframeWrapper):
                return [pd.Series([False, True, False, True, False]), pd.Series([True, False, True, False, True])]

        data = {'A': [1, 2, 3, 4, 5],
                'B': [6, 7, 8, 9, 10]}
        df = pd.DataFrame(data)
        wrapper = DataframeWrapper(df)
        grouper = CustomGrouping()

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 2)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True), pd.DataFrame({'A': [2, 4],
                                                                                                            'B': [7,
                                                                                                                  9]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [1, 3, 5],
                                                    'B': [6, 8, 10]}))


class TestAggregator(unittest.TestCase):
    def test_aggregate(self):
        class CustomAggregator(Aggregator):
            def __init__(self):
                pass

            def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
                new_data = {'A': [df_wrapper.dataframe()['A'].min()], 'B': [df_wrapper.dataframe()['B'].min()]}
                return df_wrapper.factory(pd.DataFrame(new_data))

        df_wrapper1 = DataframeWrapper(pd.DataFrame({'A': [2, 4], 'B': [7, 9]}))
        df_wrapper2 = DataframeWrapper(pd.DataFrame({'A': [1, 3, 5], 'B': [6, 8, 10]}))

        aggregator = CustomAggregator()

        result_df_wrapper = aggregator.aggregate([df_wrapper1, df_wrapper2])

        self.assertEqual(result_df_wrapper.size(), 2)
        pd.testing.assert_frame_equal(result_df_wrapper.dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [2, 1], 'B': [7, 6]}))

    def test_aggregation(self):
        aggregator = Aggregator({'A': max, 'B': min})

        df_wrapper1 = DataframeWrapper(pd.DataFrame({'A': [2, 4], 'B': [7, 9]}))
        result_df_wrapper1 = aggregator.aggregation(df_wrapper1)
        pd.testing.assert_frame_equal(result_df_wrapper1.dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [4], 'B': [7]}))

        df_wrapper2 = DataframeWrapper(pd.DataFrame({'A': [1, 3, 5], 'B': [6, 8, 10]}))
        result_df_wrapper2 = aggregator.aggregation(df_wrapper2)
        pd.testing.assert_frame_equal(result_df_wrapper2.dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [5], 'B': [6]}))


if __name__ == '__main__':
    unittest.main()
