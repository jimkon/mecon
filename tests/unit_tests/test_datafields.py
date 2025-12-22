import unittest
from datetime import datetime, date
from unittest.mock import patch

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


class TestColumnMixinValidation(unittest.TestCase):
    class _IdWrapper(datafields.DataframeWrapper, datafields.IdColumnMixin):
        def __init__(self, df):
            datafields.DataframeWrapper.__init__(self, df)
            datafields.IdColumnMixin.__init__(self, df_wrapper=self, validate=True)

    class _NoRequiredWrapper(datafields.DataframeWrapper, datafields.ColumnMixin):
        def __init__(self, df):
            datafields.DataframeWrapper.__init__(self, df)
            datafields.ColumnMixin.__init__(self, df_wrapper=self, validate=True)

    def test_missing_required_column_raises(self):
        with self.assertRaises(datafields.MissingRequiredColumnInDataframeWrapperError):
            self._IdWrapper(pd.DataFrame({'not_id': ['a']}))

    def test_present_required_column_passes(self):
        try:
            self._IdWrapper(pd.DataFrame({'id': ['a']}))
        except datafields.MissingRequiredColumnInDataframeWrapperError as e:
            self.fail(f"Unexpected exception raised: {e}")

    def test_no_required_columns_passes(self):
        try:
            self._NoRequiredWrapper(pd.DataFrame({'not_id': ['a']}))
        except datafields.MissingRequiredColumnInDataframeWrapperError as e:
            self.fail(f"Unexpected exception raised: {e}")

class TestTaggedRowsLookup(unittest.TestCase):

    def _wrapper_with_ids_and_tags(self):
        # Uses your existing ExampleDataframeWrapper pattern
        return ExampleDataframeWrapper(pd.DataFrame({
            "id":   ["id1", "id2", "id3", "id4", "id5"],
            "tags": ["", "tag1", "tag1,tag2", "tag2", "tag3"],
        }))

    def test_build_lookup_creates_expected_mapping(self):
        wrapper = self._wrapper_with_ids_and_tags()

        lookup = datafields.TaggedRowsLookup(wrapper, tags_set={"tag1", "tag2", "tag3"}).build_lookup()

        # The _lookup dict is "private" but for basic correctness tests it’s OK to assert on it.
        expected = {
            "tag1": {"id2", "id3"},
            "tag2": {"id3", "id4"},
            "tag3": {"id5"},
        }
        self.assertEqual(lookup._lookup, expected)

    def test_build_lookup_adds_boolean_columns_for_each_tag(self):
        wrapper = self._wrapper_with_ids_and_tags()

        lookup = datafields.TaggedRowsLookup(wrapper, tags_set={"tag1", "tag2"}).build_lookup()

        # Ensure those helper columns exist and have correct booleans
        df = lookup._df
        self.assertIn("tag1_col", df.columns)
        self.assertIn("tag2_col", df.columns)

        pd.testing.assert_series_equal(
            df["tag1_col"].reset_index(drop=True),
            pd.Series([False, True, True, False, False]),
            check_names=False,
        )
        pd.testing.assert_series_equal(
            df["tag2_col"].reset_index(drop=True),
            pd.Series([False, False, True, True, False]),
            check_names=False,
        )

    def test_lookup_with_single_tag_string(self):
        wrapper = self._wrapper_with_ids_and_tags()
        lookup = datafields.TaggedRowsLookup(wrapper).build_lookup()

        result = set(lookup.lookup("tag1"))
        self.assertEqual(result, {"id2", "id3"})

    def test_lookup_with_multiple_tags_iterable_unions_ids(self):
        wrapper = self._wrapper_with_ids_and_tags()
        lookup = datafields.TaggedRowsLookup(wrapper).build_lookup()

        result = set(lookup.lookup(["tag1", "tag2"]))
        # union of {"id2","id3"} and {"id3","id4"} => {"id2","id3","id4"}
        self.assertEqual(result, {"id2", "id3", "id4"})

    def test_build_lookup_matches_whole_tags_not_substrings(self):
        wrapper = ExampleDataframeWrapper(pd.DataFrame({
            "id": ["id1", "id2"],
            "tags": ["tag10", "tag1,tag2"],
        }))
        lookup = datafields.TaggedRowsLookup(wrapper, tags_set={"tag1"}).build_lookup()
        self.assertEqual(lookup._lookup["tag1"], {"id2"})

    def test_lookup_warns_for_unindexed_tags_and_raises_keyerror(self):
        wrapper = self._wrapper_with_ids_and_tags()
        lookup = datafields.TaggedRowsLookup(wrapper, tags_set={"tag1"}).build_lookup()

        # Your implementation warns, then still tries self._lookup[tag] which KeyErrors.
        with patch("mecon.data.datafields.logging.warning") as warn_mock:
            with self.assertRaises(KeyError):
                lookup.lookup(["tag1", "not_indexed"])

            warn_mock.assert_called()  # simple check that warning happened

    def test_lookup_empty_iterable_returns_empty_list(self):
        wrapper = self._wrapper_with_ids_and_tags()
        lookup = datafields.TaggedRowsLookup(wrapper).build_lookup()

        result = lookup.lookup([])
        self.assertEqual(result, [])


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

    def test_containing_tags_with_lookup(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4'],
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        })).build_tags_lookup()
        expected_wrapper_df = pd.DataFrame({
            'id': ['id2', 'id3'],
            'tags': ['tag1', 'tag1,tag2']
        })
        pd.testing.assert_frame_equal(example_wrapper.containing_tags('tag1').dataframe().reset_index(drop=True),
                                      expected_wrapper_df)

        pd.testing.assert_frame_equal(example_wrapper.containing_tags(None).dataframe().reset_index(drop=True),
                                      example_wrapper.dataframe())

    def test_containing_tags_empty_tags_with_lookup(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4'],
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        })).build_tags_lookup()

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

    def test_not_containing_tags_with_lookup(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4'],
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        })).build_tags_lookup()
        expected_wrapper_df = pd.DataFrame({
            'id': ['id1', 'id4'],
            'tags': ['', 'tag3']
        })
        pd.testing.assert_frame_equal(example_wrapper.not_containing_tags('tag1').dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        self.assertEqual(example_wrapper.not_containing_tags(None).size(), 0)

    def test_not_containing_tags_empty_tags_with_lookup(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4'],
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        })).build_tags_lookup()

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

    def test_tag_row_wise_equality(self):
        a_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3', 'tag4', 'tag5', 'tag6,tag7']
        }))
        b_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3', 'not_tag4', '', 'tag6']
        }))

        pd.testing.assert_series_equal(a_wrapper.tag_row_wise_equality(b_wrapper.tags),
                         pd.Series([True, True, True, True, False, False, False]))

        pd.testing.assert_series_equal(a_wrapper.tag_row_wise_equality(b_wrapper.tags, target_tags=['tag1', 'tag6']),
                                       pd.Series([True, True, True, True, True, True, True]))



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


    def test_select_by_ids(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4', 'id5'],
        }))
        selected_ids_wrapper = example_wrapper.select_by_ids(['id1', 'id4', 'non_existent_id'])
        expected_wrapper_df = pd.DataFrame({
            'id': ['id1', 'id4'],
        })
        pd.testing.assert_frame_equal(selected_ids_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))


if __name__ == '__main__':
    unittest.main()
