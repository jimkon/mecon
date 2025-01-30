import unittest

import pandas as pd
from pandas import Timestamp

from mecon.data.transactions import Transactions


class TestTransactions(unittest.TestCase):
    def test_diff(self):
        transactions_a = Transactions(pd.DataFrame([  # the tags col will be reset, just keeping it for reference
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
        ]))

        transactions_b = Transactions(pd.DataFrame([  # the tags col will be reset, just keeping it for reference
            {'amount': -100, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
        ]))

        transactions_c = Transactions(pd.DataFrame([  # the tags col will be reset, just keeping it for reference
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Accommodation,Online payments,Rent'},
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
        ]))

        self.assertTupleEqual(transactions_a.diff(transactions_a).shape, (0, 7))
        self.assertTupleEqual(transactions_a.diff(transactions_a.copy()).shape, (0, 7))

        self.assertTupleEqual(transactions_a.diff(transactions_b).shape, (1, 7))

        self.assertTupleEqual(transactions_a.diff(transactions_c).shape, (0, 7))

    def test_equals(self):
        transactions_a = Transactions(pd.DataFrame([  # the tags col will be reset, just keeping it for reference
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
        ]))

        transactions_b = Transactions(pd.DataFrame([  # the tags col will be reset, just keeping it for reference
            {'amount': -100, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
        ]))


        transactions_c = Transactions(pd.DataFrame([  # the tags col will be reset, just keeping it for reference
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Accommodation,Online payments,Rent'},
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
        ]))

        self.assertEqual(transactions_a.equals(transactions_a), True)
        self.assertEqual(transactions_a.equals(transactions_a.copy()), True)

        self.assertEqual(transactions_a.equals(transactions_b), False)

        self.assertEqual(transactions_a.equals(transactions_c), True)


if __name__ == '__main__':
    unittest.main()
