import unittest

import pandas as pd

from mecon2.datafields import DataframeWrapper, Grouping, Aggregator, LabelGrouping


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


class TestLabelGrouping(unittest.TestCase):
    def test_group(self):
        class CustomGrouping(LabelGrouping):
            def labels(self, df_wrapper: DataframeWrapper):
                return pd.Series(['a', 'b', 'b', 'c', 'b'])

        data = {'A': [1, 2, 3, 4, 5],
                'B': [6, 7, 8, 9, 10]}
        df = pd.DataFrame(data)
        wrapper = DataframeWrapper(df)
        grouper = CustomGrouping()

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


class TestAggregator(unittest.TestCase):
    def test_group(self):
        class CustomAggregator(Aggregator):
            def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
                return df_wrapper

        df_wrapper1 = DataframeWrapper(pd.DataFrame({'A': [2, 4], 'B': [7, 9]}))
        df_wrapper2 = DataframeWrapper(pd.DataFrame({'A': [1, 3, 5], 'B': [6, 8, 10]}))

        aggregator = CustomAggregator()

        result_df_wrapper = aggregator.aggregate([df_wrapper1, df_wrapper2])

        self.assertEqual(result_df_wrapper.size(), df_wrapper1.size() + df_wrapper2.size())
        pd.testing.assert_frame_equal(result_df_wrapper.dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [2, 4, 1, 3, 5], 'B': [7, 9, 6, 8, 10]}))


if __name__ == '__main__':
    unittest.main()
