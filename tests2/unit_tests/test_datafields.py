import unittest

import pandas as pd

from mecon2 import datafields


# TODO merge with test_dataframe_wrappers maybe


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
    # def test_init_validation(self):  # TODO uncomment when validation is back in place
    #     ExampleDataframeWrapper(pd.DataFrame({
    #         'tags': []
    #     }))  # should work
    #     with self.assertRaises(datafields.TagsColumnDoesNotExistInDataframe):
    #         ExampleDataframeWrapper(pd.DataFrame({
    #             'no_tags': []
    #         }))  # should NOT work

    def test_tags_set(self):
        result_set = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag1,tag2,tag3']
        })).all_tags()
        self.assertEqual(result_set, {'tag1': 3, 'tag2': 2, 'tag3': 1})

    def test_contains_tag(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))
        expected_wrapper_df = pd.DataFrame({
            'tags': ['tag1', 'tag1,tag2']
        })
        pd.testing.assert_frame_equal(example_wrapper.contains_tag('tag1').dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))


class TestDateTimeColumnMixin(unittest.TestCase):
    def test_date_range(self):
        from datetime import date, datetime
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
        result_df = example_wrapper.date_range()
        self.assertTupleEqual(result_df, expected_date_range)

    def test_select_date_range(self):
        from datetime import datetime
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
        from datetime import datetime
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
            start_date='2020-01-01 00:00:00',
            end_date='2022-01-01 00:00:00'
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))


if __name__ == '__main__':
    unittest.main()
