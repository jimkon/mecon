import unittest
from datetime import datetime, date

import pandas as pd

from data import datafields


# TODO:v3 merge with test_dataframe_wrappers maybe


class ExampleDataframeWrapper(datafields.DataframeWrapper,
                              datafields.IdColumnMixin,
                              datafields.DateTimeColumnMixin,
                              datafields.AmountColumnMixin,
                              datafields.DescriptionColumnMixin,
                              datafields.TagsColumnMixin
                              ):
    def __init__(self, df):
        super().__init__(df=df)
        datafields.IdColumnMixin.__init__(self, df_wrapper=self)
        datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)
        datafields.AmountColumnMixin.__init__(self, df_wrapper=self)
        datafields.DescriptionColumnMixin.__init__(self, df_wrapper=self)
        datafields.TagsColumnMixin.__init__(self, df_wrapper=self)
        # super(datafields.DataframeWrapper, self).__init__(df)  why it doesn't work?
        # super(datafields.IdColumnMixin, self).__init__(self)
        # super(datafields.DateTimeColumnMixin, self).__init__(self)
        # super(datafields.AmountColumnMixin, self).__init__(self)
        # super(datafields.DescriptionColumnMixin, self).__init__(self)
        # super(datafields.TagsColumnMixin, self).__init__(df_wrapper=self)


class TestTagsColumnMixin(unittest.TestCase):
    def test_tags_set(self):
        result_set = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag1,tag2,tag3']
        })).all_tags()
        self.assertEqual(result_set, {'tag1': 3, 'tag2': 2, 'tag3': 1})

    def test_contains_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        self.assertListEqual(example_wrapper.contains_tag('tag1').to_list(),
                             [False, True, True, False])
        self.assertListEqual(example_wrapper.contains_tag('tag2').to_list(),
                             [False, False, True, False])
        self.assertListEqual(example_wrapper.contains_tag('tag3').to_list(),
                             [False, False, False, True])
        self.assertListEqual(example_wrapper.contains_tag(['tag1', 'tag2']).to_list(),
                             [False, False, True, False])

        self.assertListEqual(example_wrapper.contains_tag([]).to_list(),
                             [True, True, True, True])

    def test_containing_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))
        expected_wrapper_df = pd.DataFrame({
            'tags': ['tag1', 'tag1,tag2']
        })
        pd.testing.assert_frame_equal(example_wrapper.containing_tag('tag1').dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        pd.testing.assert_frame_equal(example_wrapper.containing_tag(None).dataframe().reset_index(drop=True),
                                      example_wrapper.dataframe().reset_index(drop=True))


class TestDateTimeColumnMixin(unittest.TestCase):
    def test_date_range(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        }))
        expected_date_range = (
            date(2019, 1, 1),
            date(2023, 1, 1),
        )
        date_range = example_wrapper.date_range()
        self.assertTupleEqual(date_range, expected_date_range)

    def test_date_range_empty_df(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
            ]
        }))
        self.assertTupleEqual(example_wrapper.date_range(), (None, None))

    def test_select_date_range(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        }))
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date=datetime(2020, 1, 1, 0, 0, 0),
            end_date=datetime(2022, 1, 1, 0, 0, 0)
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

    def test_select_date_range_str(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        }))
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date='2020-01-01 00:00:00',
            end_date='2022-01-01 00:00:00'
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

    def test_select_date_range_null_input_dates(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        }))

        # end_date = None
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date=datetime(2020, 1, 1, 0, 0, 0),
            end_date=None
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        # start_date = None
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date=None,
            end_date=datetime(2022, 1, 1, 0, 0, 0)
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        # start_date = None and end_date = None
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date=None,
            end_date=None
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

    def test_select_date_range_empty_transactions(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
            ]
        }))
        # expected_wrapper_df = pd.DataFrame({
        #     'datetime': [
        #     ]
        # })
        result_df = example_wrapper.select_date_range(
            start_date='2020-01-01 00:00:00',
            end_date='2022-01-01 00:00:00'
        ).dataframe()
        # uncomment fix TODO in datafields.select_date_range (if self._df_wrapper is empty, self._df_wrapper_obj.apply_rule returned object has no columns)
        # pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
        #                               expected_wrapper_df.reset_index(drop=True))
        self.assertEqual(len(result_df), 0)


class TestAmountColumnMixin(unittest.TestCase):
    def test_all_currencies(self):
        result_set = ExampleDataframeWrapper(pd.DataFrame({
            'amount': [1, 2, 3, 4],
            'amount_cur': [1, 2, 3, 4],
            'currency': ['GBP', 'EUR,GBP', 'EUR', 'RON'],
        })).all_currencies()
        self.assertEqual(result_set, '{"RON": 1, "GBP": 2, "EUR": 2}')


if __name__ == '__main__':
    unittest.main()
