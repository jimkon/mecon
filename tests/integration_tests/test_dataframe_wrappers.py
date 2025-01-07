import unittest
from datetime import datetime, date

import pandas as pd

from mecon.tags import tagging
from mecon.data.datafields import DataframeWrapper, Grouping, InTypeAggregator, DateFiller, DatedDataframeWrapper, \
    UnorderedDatedDataframeWrapper, AggregatorABC, InvalidInputToAggregator


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

    def test_apply_rule(self):
        rule = tagging.Condition.from_string_values('A', None, 'greater_equal', 2)
        res_wrapper = self.wrapper.apply_rule(rule)

        expected_df = pd.DataFrame({'A': [2, 3],
                                    'B': [5, 6]})
        pd.testing.assert_frame_equal(res_wrapper.dataframe().reset_index(drop=True),
                                      expected_df)

    def test_apply_negated_rule(self):
        rule = tagging.Condition.from_string_values('A', None, 'greater_equal', 2)
        res_wrapper = self.wrapper.apply_negated_rule(rule)

        expected_df = pd.DataFrame({'A': [1],
                                    'B': [4]})
        pd.testing.assert_frame_equal(res_wrapper.dataframe().reset_index(drop=True),
                                      expected_df)


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

    def test_group_empty_df_wrapper(self):
        class CustomGrouping(Grouping):
            def compute_group_indexes(self, df_wrapper: DataframeWrapper):
                return []

        wrapper = DataframeWrapper(pd.DataFrame({'A': [], 'B': []}))

        grouper = CustomGrouping()

        grouped_wrappers = grouper.group(wrapper)
        self.assertEqual(len(grouped_wrappers), 0)
        self.assertEqual(grouped_wrappers, [])


class TestAggregator(unittest.TestCase):
    def test_aggregate_results_df(self):
        class CustomAggregator(AggregatorABC):
            def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
                return df_wrapper  # no aggregation

        df_wrapper1 = DataframeWrapper(pd.DataFrame({'A': [2, 4], 'B': [7, 9]}))
        df_wrapper2 = DataframeWrapper(pd.DataFrame({'A': [1, 3, 5], 'B': [6, 8, 10]}))

        aggregator = CustomAggregator()

        df_agg = aggregator.aggregate_result_df([df_wrapper1, df_wrapper2])

        pd.testing.assert_frame_equal(df_agg.reset_index(drop=True),
                                      pd.DataFrame({'A': [2, 4, 1, 3, 5], 'B': [7, 9, 6, 8, 10]}))

    def test_aggregate_results_df__empty_input_list(self):
        class CustomAggregator(AggregatorABC):
            def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
                return df_wrapper  # no aggregation

        aggregator = CustomAggregator()

        with self.assertRaises(InvalidInputToAggregator):
            df_agg = aggregator.aggregate_result_df([])


class TestInTypeAggregator(unittest.TestCase):
    def test_aggregate(self):
        class CustomInTypeAggregator(InTypeAggregator):
            def __init__(self):
                super().__init__(None)

            def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
                new_data = {'A': [df_wrapper.dataframe()['A'].min()], 'B': [df_wrapper.dataframe()['B'].min()]}
                return df_wrapper.factory(pd.DataFrame(new_data))

        df_wrapper1 = DataframeWrapper(pd.DataFrame({'A': [2, 4], 'B': [7, 9]}))
        df_wrapper2 = DataframeWrapper(pd.DataFrame({'A': [1, 3, 5], 'B': [6, 8, 10]}))

        aggregator = CustomInTypeAggregator()

        result_df_wrapper = aggregator.aggregate([df_wrapper1, df_wrapper2])

        self.assertEqual(result_df_wrapper.size(), 2)
        pd.testing.assert_frame_equal(result_df_wrapper.dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [2, 1], 'B': [7, 6]}))

    def test_aggregation(self):
        aggregator = InTypeAggregator({'A': max, 'B': min})

        df_wrapper1 = DataframeWrapper(pd.DataFrame({'A': [2, 4], 'B': [7, 9]}))
        result_df_wrapper1 = aggregator.aggregation(df_wrapper1)
        pd.testing.assert_frame_equal(result_df_wrapper1.dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [4], 'B': [7]}))

        df_wrapper2 = DataframeWrapper(pd.DataFrame({'A': [1, 3, 5], 'B': [6, 8, 10]}))
        result_df_wrapper2 = aggregator.aggregation(df_wrapper2)
        pd.testing.assert_frame_equal(result_df_wrapper2.dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [5], 'B': [6]}))


class TestDatedDataframeWrapper(unittest.TestCase):
    def test_validation(self):
        # should work
        DatedDataframeWrapper(pd.DataFrame({
            'datetime': [datetime(2023, 9, 1, 0, 0, 0),
                         datetime(2023, 9, 3, 0, 0, 0),
                         datetime(2023, 9, 5, 0, 0, 0)],
        }))

        with self.assertRaises(UnorderedDatedDataframeWrapper):
            # should throw UnorderedDatedDataframeWrapper
            DatedDataframeWrapper(pd.DataFrame({
                'datetime': [datetime(2023, 9, 3, 0, 0, 0),
                             datetime(2023, 9, 1, 0, 0, 0),
                             datetime(2023, 9, 5, 0, 0, 0)],
            }))


class TestDateFiller(unittest.TestCase):
    def test_produce_fill_df_rows(self):
        filler = DateFiller('day', fill_values_dict={'a': 'alpha', 'b': 'beta'})

        result_df = filler.produce_fill_df_rows(
            datetime(2023, 9, 1, 12, 23, 34),
            datetime(2023, 9, 5, 12, 23, 34)
        )
        expected_df = pd.DataFrame({
            'datetime': [datetime(2023, 9, 1, 0, 0, 0), datetime(2023, 9, 2, 0, 0, 0),
                         datetime(2023, 9, 3, 0, 0, 0), datetime(2023, 9, 4, 0, 0, 0),
                         datetime(2023, 9, 5, 0, 0, 0)],
            'a': ['alpha', 'alpha', 'alpha', 'alpha', 'alpha'],
            'b': ['beta', 'beta', 'beta', 'beta', 'beta']
        })
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_produce_fill_df_rows_remove_dates(self):
        filler = DateFiller('day', fill_values_dict={'a': 'alpha', 'b': 'beta'})

        result_df = filler.produce_fill_df_rows(
            datetime(2023, 9, 1, 12, 23, 34),
            datetime(2023, 9, 5, 12, 23, 34),
            remove_dates=[date(2023, 9, 2), date(2023, 9, 4)]
        )
        expected_df = pd.DataFrame({
            'datetime': [datetime(2023, 9, 1, 0, 0, 0),
                         datetime(2023, 9, 3, 0, 0, 0),
                         datetime(2023, 9, 5, 0, 0, 0)],
            'a': ['alpha', 'alpha', 'alpha'],
            'b': ['beta', 'beta', 'beta']
        })
        pd.testing.assert_frame_equal(result_df, expected_df)


if __name__ == '__main__':
    unittest.main()
