import unittest
from datetime import datetime

import pandas as pd

from mecon import groupings
from mecon.aggregators import CustomisedDefaultTransactionAggregator, CustomisedAmountTransactionAggregator
from mecon.transactions import Transactions, TransactionDateFiller


class TestTransactionAggregator(unittest.TestCase):
    def test_default_agg(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedDefaultTransactionAggregator()
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0)],
            'amount': [600.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [600.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_min_amount_per_day(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('min', 'day')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 3, 0, 0, 0)],
            'amount': [100.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [100.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_max_amount_per_day(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('max', 'day')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 3, 0, 0, 0)],
            'amount': [300.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [300.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_sum_amount_per_day(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('sum', 'day')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 3, 0, 0, 0)],
            'amount': [600.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [600.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_avg_amount_per_day(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('avg', 'day')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 3, 0, 0, 0)],
            'amount': [200.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [200.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_count_amount_per_day(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('count', 'day')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 3, 0, 0, 0)],
            'amount': [3],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [3],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_min_amount_per_week(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 10, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('min', 'week')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 8, 0, 0, 0)],
            'amount': [100.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [100.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_max_amount_per_week(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 10, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('max', 'week')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 8, 0, 0, 0)],
            'amount': [300.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [300.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_sum_amount_per_week(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 10, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('sum', 'week')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 8, 0, 0, 0)],
            'amount': [600.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [600.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_avg_amount_per_week(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 10, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('avg', 'week')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 8, 0, 0, 0)],
            'amount': [200.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [200.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_count_amount_per_week(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 10, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('count', 'week')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 8, 0, 0, 0)],
            'amount': [3],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [3],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_min_amount_per_month(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('min', 'month')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 1, 0, 0, 0)],
            'amount': [100.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [100.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_max_amount_per_month(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('max', 'month')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 1, 0, 0, 0)],
            'amount': [300.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [300.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_sum_amount_per_month(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('sum', 'month')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 1, 0, 0, 0)],
            'amount': [600.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [600.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_avg_amount_per_month(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('avg', 'month')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 1, 0, 0, 0)],
            'amount': [200.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [200.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_count_amount_per_month(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('count', 'month')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 2, 1, 0, 0, 0)],
            'amount': [3],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [3],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_min_amount_per_year(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('min', 'year')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0)],
            'amount': [100.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [100.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_max_amount_per_year(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('max', 'year')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0)],
            'amount': [300.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [300.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_sum_amount_per_year(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('sum', 'year')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0)],
            'amount': [600.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [600.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_avg_amount_per_year(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('avg', 'year')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0)],
            'amount': [200.0],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [200.0],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))

    def test_count_amount_per_year(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 3, 4, 5, 6), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        agg = CustomisedAmountTransactionAggregator('count', 'year')
        result_trans_df = agg.aggregation(transactions).dataframe()
        expected_trans_df = pd.DataFrame({
            'id': [11],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0)],
            'amount': [3],
            'currency': ['GBP,GBP,GBP'],
            'amount_cur': [3],
            'description': ['Transaction 1,Transaction 2,Transaction 3'],
            'tags': ['tag1,tag2']
        })
        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))


class TestFillTransactions(unittest.TestCase):
    def test_fill(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 13, 15],
            'datetime': [datetime(2023, 9, 1, 12, 23, 34),
                         datetime(2023, 9, 3, 12, 23, 34),
                         datetime(2023, 9, 5, 12, 23, 34)],
            'amount': [100.0, 200.0, 300.0, ],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        filler = TransactionDateFiller(
            fill_unit='day',
            id_fill=-1,
            amount_fill=1.0,
            currency_fill='currency_fill',
            amount_curr=1.0,
            description_fill='description_fill',
            tags_fills='tags_fills'
        )
        result_df = filler.fill(transactions).dataframe().reset_index(drop=True)
        expected_df = Transactions(pd.DataFrame({
            'id': [11, -1, 13, -1, 15],
            'datetime': [datetime(2023, 9, 1, 12, 23, 34),
                         datetime(2023, 9, 2, 0, 0, 0),
                         datetime(2023, 9, 3, 12, 23, 34),
                         datetime(2023, 9, 4, 0, 0, 0),
                         datetime(2023, 9, 5, 12, 23, 34)],
            'amount': [100.0, 1.0, 200.0, 1.0, 300.0],
            'currency': ['GBP', 'currency_fill', 'GBP', 'currency_fill', 'GBP'],
            'amount_cur': [100.0, 1.0, 200.0, 1.0, 300.0],
            'description': ['Transaction 1', 'description_fill', 'Transaction 2', 'description_fill', 'Transaction 3'],
            'tags': ['', 'tags_fills', 'tag1', 'tags_fills', 'tag1,tag2']
        })).dataframe().reset_index(drop=True)

        pd.testing.assert_frame_equal(result_df, expected_df)


class TestGroupAgg(unittest.TestCase):
    def test_group_agg(self):
        transactions = Transactions(pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 2, 1, 4, 5, 6), datetime(2021, 2, 3, 4, 5, 6),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        }))

        expected_trans_df = pd.DataFrame({
            'id': [11, 13],
            'datetime': [datetime(2021, 2, 1, 0, 0, 0), datetime(2021, 12, 27, 0, 0, 0)],
            'amount': [300.0, 300.0],
            'currency': ['GBP,GBP', 'GBP'],
            'amount_cur': [300.0, 300.0],
            'description': ['Transaction 1,Transaction 2', 'Transaction 3'],
            'tags': ['tag1', 'tag1,tag2']
        })

        grouper = groupings.WEEK
        agg = CustomisedAmountTransactionAggregator('sum', 'week')
        new_transactions = transactions.groupagg(grouper=grouper, aggregator=agg)
        result_trans_df = new_transactions.dataframe()

        pd.testing.assert_frame_equal(result_trans_df.reset_index(drop=True),
                                      expected_trans_df.reset_index(drop=True))


if __name__ == '__main__':
    unittest.main()
