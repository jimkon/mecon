import unittest
from datetime import datetime, date

import pandas as pd

from mecon.data import datafields


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
    def test_tags_stats(self):
        result_set = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag1,tag2,tag3']
        })).all_tag_counts()
        self.assertEqual(result_set, {'tag1': 3, 'tag2': 2, 'tag3': 1})

    def test_tags_set(self):
        result_set = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag1,tag2,tag3']
        })).all_tags()
        self.assertEqual(result_set, ['tag1', 'tag2', 'tag3'])

    def test_contains_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3', 'tag4 blabla']
        }))

        pd.testing.assert_series_equal(example_wrapper.contains_tags('tag1'),
                                       pd.Series([False, True, True, False, False]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags('tag2'),
                                       pd.Series([False, False, True, False, False]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags('tag3'),
                                       pd.Series([False, False, False, True, False]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags(['tag1', 'tag2']),
                                       pd.Series([False, False, True, False, False]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags([]),
                                       pd.Series([True, True, True, True, True]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags('tag4'),
                                       pd.Series([False, False, False, False, False]))


    def test_contains_tags_empty_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        pd.testing.assert_series_equal(example_wrapper.contains_tags([]),
                                       pd.Series([True, True, True, True]))

        pd.testing.assert_series_equal(example_wrapper.contains_tags([], empty_tags_strategy='all_true'),
                                       pd.Series([True, True, True, True]))

        pd.testing.assert_series_equal(example_wrapper.contains_tags([], empty_tags_strategy='all_false'),
                                       pd.Series([False, False, False, False]))

        with self.assertRaises(ValueError):
            example_wrapper.contains_tags([], empty_tags_strategy='raise')

        with self.assertRaises(ValueError):
            example_wrapper.contains_tags([], empty_tags_strategy='not_a_valid_value')

    def test_containing_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))
        expected_wrapper_df = pd.DataFrame({
            'tags': ['tag1', 'tag1,tag2']
        })
        pd.testing.assert_frame_equal(example_wrapper.containing_tags('tag1').dataframe(),
                                      expected_wrapper_df)

        pd.testing.assert_frame_equal(example_wrapper.containing_tags(None).dataframe(),
                                      example_wrapper.dataframe())

    def test_containing_tags_empty_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        self.assertEqual(example_wrapper.containing_tags(None).size(), 4)
        self.assertEqual(example_wrapper.containing_tags(None, empty_tags_strategy='all_true').size(), 4)
        self.assertEqual(example_wrapper.containing_tags(None, empty_tags_strategy='all_false').size(), 0)

        with self.assertRaises(ValueError):
            example_wrapper.containing_tags(None, empty_tags_strategy='raise')

    def test_not_contains_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags('tag1'),
                                       pd.Series([True, False, False, True]))
        pd.testing.assert_series_equal(example_wrapper.not_contains_tags('tag2'),
                                       pd.Series([True, True, False, True]))
        pd.testing.assert_series_equal(example_wrapper.not_contains_tags('tag3'),
                                       pd.Series([True, True, True, False]))
        pd.testing.assert_series_equal(example_wrapper.not_contains_tags(['tag1', 'tag2']),
                                       pd.Series([True, True, False, True]))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags([]),
                                       pd.Series([False, False, False, False]))

    def test_not_contains_tags_empty_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags([]),
                                       pd.Series([False, False, False, False]))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags([], empty_tags_strategy='all_true'),
                                       pd.Series([True, True, True, True]))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags([], empty_tags_strategy='all_false'),
                                       pd.Series([False, False, False, False]))

        with self.assertRaises(ValueError):
            example_wrapper.not_contains_tags([], empty_tags_strategy='raise')

        with self.assertRaises(ValueError):
            example_wrapper.not_contains_tags([], empty_tags_strategy='not_a_valid_value')

    def test_not_containing_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))
        expected_wrapper_df = pd.DataFrame({
            'tags': ['', 'tag3']
        })
        pd.testing.assert_frame_equal(example_wrapper.not_containing_tags('tag1').dataframe(),
                                      expected_wrapper_df)

        self.assertEqual(example_wrapper.not_containing_tags(None).size(), 0)

    def test_not_containing_tags_empty_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        self.assertEqual(example_wrapper.not_containing_tags(None).size(), 0)
        self.assertEqual(example_wrapper.not_containing_tags(None, empty_tags_strategy='all_false').size(), 0)
        self.assertEqual(example_wrapper.not_containing_tags(None, empty_tags_strategy='all_true').size(), 4)

        with self.assertRaises(ValueError):
            example_wrapper.not_containing_tags(None, empty_tags_strategy='raise')

    def test_invalid_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', False, 12321, -12.2]
        }))

        self.assertEqual(example_wrapper.invalid_tags().to_list(),
                         [False, False, False, True, True, True])



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

    def test_invalid_datetimes(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [pd.Timestamp(2019, 1, 1, 0, 0, 0),
                         datetime(2019, 1, 1, 0, 0, 0),
                         '2020-01-01 00:00:00', False, 12321, -12.2]
        }))

        self.assertEqual(example_wrapper.invalid_datetimes().to_list(),
                         [False, False, True, True, True, True])


class TestAmountColumnMixin(unittest.TestCase):
    def test_all_currencies(self):
        result_set = ExampleDataframeWrapper(pd.DataFrame({
            'amount': [1, 2, 3, 4],
            'amount_cur': [1, 2, 3, 4],
            'currency': ['GBP', 'EUR,GBP', 'EUR', 'RON'],
        })).all_currencies()
        self.assertEqual(result_set, '{"RON": 1, "GBP": 2, "EUR": 2}')

    def test_positive_amounts(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'amount': [-1, 2, -3, 4, 0],
            'amount_cur': [1, 2, 3, 4, 0],  # only looking 'amount' column to check positivity
            'currency': ['GBP', 'EUR,GBP', 'EUR', 'RON', 'EUR'],
        }))
        pos_amounts_wrapper = example_wrapper.positive_amounts(include_zero=True)
        expected_wrapper_df = pd.DataFrame({
            'amount': [2, 4, 0],
            'amount_cur': [2, 4, 0],
            'currency': ['EUR,GBP', 'RON', 'EUR'],
        })
        pd.testing.assert_frame_equal(pos_amounts_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        pos_nonzero_amounts_wrapper = example_wrapper.positive_amounts(include_zero=False)
        expected_wrapper_df = pd.DataFrame({
            'amount': [2, 4],
            'amount_cur': [2, 4],
            'currency': ['EUR,GBP', 'RON'],
        })
        pd.testing.assert_frame_equal(pos_nonzero_amounts_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

    def test_negative_amounts(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'amount': [-1, 2, -3, 4, 0],
            'amount_cur': [1, 2, 3, 4, 0],  # only looking 'amount' column to check negativity
            'currency': ['GBP', 'EUR,GBP', 'EUR', 'RON', 'EUR'],
        }))
        pos_amounts_wrapper = example_wrapper.negative_amounts(include_zero=True)
        expected_wrapper_df = pd.DataFrame({
            'amount': [-1, -3, 0],
            'amount_cur': [1, 3, 0],  # only looking 'amount' column to check positivity
            'currency': ['GBP', 'EUR', 'EUR'],
        })
        pd.testing.assert_frame_equal(pos_amounts_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        pos_nonzero_amounts_wrapper = example_wrapper.negative_amounts(include_zero=False)
        expected_wrapper_df = pd.DataFrame({
            'amount': [-1, -3],
            'amount_cur': [1, 3],  # only looking 'amount' column to check positivity
            'currency': ['GBP', 'EUR'],
        })
        pd.testing.assert_frame_equal(pos_nonzero_amounts_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))


    def test_invalid_amounts(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'amount': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_amounts().to_list(),
                         [False, False, False, False, True, False, True])

    def test_invalid_amount_curs(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'amount_cur': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_amount_curs().to_list(),
                         [False, False, False, False, True, False, True])

    def test_invalid_currencies(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'currency': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_currencies().to_list(),
                         [True, True, True, True, False, True, True])


class TestDescriptionColumnMixin(unittest.TestCase):
    def test_invalid_descriptions(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'description': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_descriptions().to_list(),
                         [True, True, True, True, False, True, True])

class TestIDColumnMixin(unittest.TestCase):
    def test_invalid_ids(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_ids().to_list(),
                         [True, True, True, True, False, True, True])


if __name__ == '__main__':
    unittest.main()
