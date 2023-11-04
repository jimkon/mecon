import unittest
from datetime import time

import pandas as pd

import mecon as utils


class TestFillDays(unittest.TestCase):
    def test_fill_days(self):
        test_df = pd.DataFrame({'date': pd.to_datetime([pd.Timestamp(2022, 1, 1), pd.Timestamp(2022, 1, 3), pd.Timestamp(2022, 1, 10)])})
        test_df['month_date'] = utils.date_to_month_date(test_df['date'])
        test_df['time'] = time(1, 2, 3)
        test_df['amount'] = -1
        test_df['currency'] = 'test_curr'
        test_df['amount_curr'] = 1
        test_df['description'] = 'test_desc'
        test_df['tags'] = [['test_tag'] for i in range(len(test_df))]

        df_filled = utils.fill_days(test_df)

        self.assertEqual(len(df_filled), 10)
        self.assertListEqual(df_filled['date'].to_list(),
                             pd.to_datetime([pd.Timestamp(2022, 1, 1), pd.Timestamp(2022, 1, 2),
                                             pd.Timestamp(2022, 1, 3), pd.Timestamp(2022, 1, 4),
                                             pd.Timestamp(2022, 1, 5), pd.Timestamp(2022, 1, 6),
                                             pd.Timestamp(2022, 1, 7), pd.Timestamp(2022, 1, 8),
                                             pd.Timestamp(2022, 1, 9), pd.Timestamp(2022, 1, 10)]).to_list())

        self.assertListEqual(df_filled['month_date'].to_list(),
                             ['2022-01', '2022-01', '2022-01', '2022-01', '2022-01',
                              '2022-01', '2022-01', '2022-01', '2022-01', '2022-01'])

        self.assertListEqual(df_filled['time'].to_list(),
                             [time(1, 2, 3), time(0, 0, 0), time(1, 2, 3), time(0, 0, 0), time(0, 0, 0),
                              time(0, 0, 0), time(0, 0, 0), time(0, 0, 0), time(0, 0, 0), time(1, 2, 3)])

        self.assertListEqual(df_filled['amount'].to_list(),
                             [-1, 0, -1, 0, 0, 0, 0, 0, 0, -1])

        self.assertListEqual(df_filled['currency'].to_list(),
                             ['test_curr', 'GBP', 'test_curr', 'GBP', 'GBP', 'GBP', 'GBP', 'GBP', 'GBP', 'test_curr'])

        self.assertListEqual(df_filled['amount_curr'].to_list(),
                             [1, 0, 1, 0, 0, 0, 0, 0, 0, 1])

        self.assertListEqual(df_filled['description'].to_list(),
                             ['test_desc', '', 'test_desc', '', '', '', '', '', '', 'test_desc'])

        self.assertListEqual(df_filled['tags'].to_list(),
                             [['test_tag'], ['FILLED'], ['test_tag'], ['FILLED'], ['FILLED'],
                              ['FILLED'], ['FILLED'], ['FILLED'], ['FILLED'], ['test_tag']])

    def test__get_fill_dates_init(self):
        utils._fill_days_df = None
        utils._get_fill_dates([pd.Timestamp(2022, 1, 1), pd.Timestamp(2022, 1, 4), pd.Timestamp(2022, 1, 6)])

        self.assertTrue(utils._fill_days_df is not None)
        self.assertEqual(utils._fill_days_df['date'].min(), pd.Timestamp(2022, 1, 1))
        self.assertEqual(utils._fill_days_df['date'].max(), pd.Timestamp(2022, 1, 6))
        self.assertEqual(utils._fill_days_df['month_date'].to_list(), ['2022-01']*6)
        self.assertEqual(utils._fill_days_df['time'].to_list(), [time(0, 0, 0)]*6)
        self.assertEqual(utils._fill_days_df['amount'].to_list(), [.0]*6)
        self.assertEqual(utils._fill_days_df['currency'].to_list(), ['GBP']*6)
        self.assertEqual(utils._fill_days_df['amount_curr'].to_list(), [.0]*6)
        self.assertEqual(utils._fill_days_df['currency'].to_list(), ['GBP']*6)
        self.assertEqual(utils._fill_days_df['description'].to_list(), ['']*6)
        self.assertEqual(utils._fill_days_df['tags'].to_list(), [['FILLED']]*6)

    def test__get_fill_dates_expand_min(self):
        utils._fill_days_df = None
        utils._get_fill_dates([pd.Timestamp(2022, 1, 2), pd.Timestamp(2022, 1, 4), pd.Timestamp(2022, 1, 6)])

        self.assertTrue(utils._fill_days_df is not None)

        utils._get_fill_dates([pd.Timestamp(2022, 1, 1), pd.Timestamp(2022, 1, 4), pd.Timestamp(2022, 1, 6)])

        self.assertEqual(utils._fill_days_df['date'].min(), pd.Timestamp(2022, 1, 1))
        self.assertEqual(utils._fill_days_df['date'].max(), pd.Timestamp(2022, 1, 6))
        self.assertEqual(utils._fill_days_df['month_date'].to_list(), ['2022-01'] * 6)
        self.assertEqual(utils._fill_days_df['time'].to_list(), [time(0, 0, 0)] * 6)
        self.assertEqual(utils._fill_days_df['amount'].to_list(), [.0] * 6)
        self.assertEqual(utils._fill_days_df['currency'].to_list(), ['GBP'] * 6)
        self.assertEqual(utils._fill_days_df['amount_curr'].to_list(), [.0] * 6)
        self.assertEqual(utils._fill_days_df['currency'].to_list(), ['GBP'] * 6)
        self.assertEqual(utils._fill_days_df['description'].to_list(), [''] * 6)
        self.assertEqual(utils._fill_days_df['tags'].to_list(), [['FILLED']] * 6)

    def test__get_fill_dates_expand_max(self):
        utils._fill_days_df = None
        utils._get_fill_dates([pd.Timestamp(2022, 1, 1), pd.Timestamp(2022, 1, 4), pd.Timestamp(2022, 1, 5)])

        self.assertTrue(utils._fill_days_df is not None)

        utils._get_fill_dates([pd.Timestamp(2022, 1, 1), pd.Timestamp(2022, 1, 4), pd.Timestamp(2022, 1, 6)])

        self.assertEqual(utils._fill_days_df['date'].min(), pd.Timestamp(2022, 1, 1))
        self.assertEqual(utils._fill_days_df['date'].max(), pd.Timestamp(2022, 1, 6))
        self.assertEqual(utils._fill_days_df['month_date'].to_list(), ['2022-01'] * 6)
        self.assertEqual(utils._fill_days_df['time'].to_list(), [time(0, 0, 0)] * 6)
        self.assertEqual(utils._fill_days_df['amount'].to_list(), [.0] * 6)
        self.assertEqual(utils._fill_days_df['currency'].to_list(), ['GBP'] * 6)
        self.assertEqual(utils._fill_days_df['amount_curr'].to_list(), [.0] * 6)
        self.assertEqual(utils._fill_days_df['currency'].to_list(), ['GBP'] * 6)
        self.assertEqual(utils._fill_days_df['description'].to_list(), [''] * 6)
        self.assertEqual(utils._fill_days_df['tags'].to_list(), [['FILLED']] * 6)

    def test__get_fill_dates(self):
        fill_dates = utils._get_fill_dates([pd.Timestamp(2022, 1, 2), pd.Timestamp(2022, 1, 4), pd.Timestamp(2022, 1, 7)])

        self.assertEqual(fill_dates['date'].to_list(), [pd.Timestamp(2022, 1, 2), pd.Timestamp(2022, 1, 4), pd.Timestamp(2022, 1, 7)])
        self.assertEqual(fill_dates['month_date'].to_list(), ['2022-01'] * len(fill_dates))
        self.assertEqual(fill_dates['time'].to_list(), [time(0, 0, 0)] * len(fill_dates))
        self.assertEqual(fill_dates['amount'].to_list(), [.0] * len(fill_dates))
        self.assertEqual(fill_dates['currency'].to_list(), ['GBP'] * len(fill_dates))
        self.assertEqual(fill_dates['amount_curr'].to_list(), [.0] * len(fill_dates))
        self.assertEqual(fill_dates['currency'].to_list(), ['GBP'] * len(fill_dates))
        self.assertEqual(fill_dates['description'].to_list(), [''] * len(fill_dates))
        self.assertEqual(fill_dates['tags'].to_list(), [['FILLED']] * len(fill_dates))

