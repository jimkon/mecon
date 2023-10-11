import unittest
from datetime import date, time, datetime
from unittest.mock import MagicMock

import pandas as pd

from mecon2.data import etl


class HSBCTransformerTest(unittest.TestCase):
    def test_transform_hsbc_transactions(self):
        df_hsbc = pd.DataFrame({
            'id': [1, 2, 3],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'amount': [100.0, '2,000.0', '-300.00'],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3']
        })

        expected_output = pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2022, 1, 1, 0, 0, 0), datetime(2022, 6, 15, 0, 0, 0),
                         datetime(2022, 12, 31, 0, 0, 0)],
            'amount': [100.0, 2000.0, -300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 2000.0, -300.0],
            'description': ['bank:HSBC, Transaction 1', 'bank:HSBC, Transaction 2', 'bank:HSBC, Transaction 3']
        })  # TODO check results

        transformer = etl.HSBCTransformer()
        transformed_df = transformer.transform(df_hsbc)
        pd.testing.assert_frame_equal(transformed_df, expected_output)


class MonzoTransformerTest(unittest.TestCase):
    def test_transform_monzo_transactions(self):
        df_monzo = pd.DataFrame(
            {
                'id': [1, 2, 3],
                'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
                'time': ["00:00:00", "12:30:30", "23:59:59"],
                'transaction_type': ['Payment', 'Expense', 'Payment'],
                'name': ['John Doe', 'Groceries', 'Jane Smith'],
                'emoji': ['💳', '🛒', '💳'],
                'category': ['Shopping', 'Food', 'Shopping'],
                'amount': [100.0, 50.0, 200.0],
                'currency': ['GBP', 'GBP', 'GBP'],
                'local_amount': [100.0, 50.0, 200.0],
                'local_currency': ['GBP', 'GBP', 'GBP'],
                'notes_tags': ['Note 1', None, 'Tag1, Tag2'],
                'address': ['123 Main St', None, '456 Elm St'],
                'receipt': ['https://example.com/receipt1', None, 'https://example.com/receipt2'],
                'description': ['Description 1', 'Description 2', 'Description 3'],
                'category_split': [None, 'Food/Groceries', None],
                'money_out': [None, 50.0, None],
                'money_in': [100.0, None, 200.0]
            }
        )

        expected_output = pd.DataFrame({
            'id': [21, 22, 23],
            'datetime': [datetime(2022, 1, 1, 0, 0, 0), datetime(2022, 6, 15, 12, 30, 30),
                         datetime(2022, 12, 31, 23, 59, 59)],
            'amount': [100.0, 50.0, 200.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 50.0, 200.0],
            'description': [
                'bank:Monzo, transaction_type: Payment, name: John Doe, emoji: 💳, category: Shopping, notes_tags: Note 1, address: 123 Main St, receipt: https://example.com/receipt1, description: Description 1, category_split: none, money_out: none, money_in: 100.0',
                'bank:Monzo, transaction_type: Expense, name: Groceries, emoji: 🛒, category: Food, notes_tags: none, address: none, receipt: none, description: Description 2, category_split: Food/Groceries, money_out: 50.0, money_in: none',
                'bank:Monzo, transaction_type: Payment, name: Jane Smith, emoji: 💳, category: Shopping, notes_tags: Tag1, Tag2, address: 456 Elm St, receipt: https://example.com/receipt2, description: Description 3, category_split: none, money_out: none, money_in: 200.0']
        })  # TODO check results

        transformer = etl.MonzoTransformer()
        transformed_df = transformer.transform(df_monzo)
        pd.testing.assert_frame_equal(transformed_df, expected_output)


class RevoTransformerTest(unittest.TestCase):
    def test_transform_revo_transactions(self):
        df_revo = pd.DataFrame({
            'id': [1, 2, 3],
            'type': ['Type 1', 'Type 2', 'Type 3'],
            'product': ['Product A', 'Product B', 'Product C'],
            'start_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'completed_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'description': ['Description 1', 'Description 2', 'Description 3'],
            'amount': [130.0, 240.0, 300.0],
            'fee': [10.0, 20.0, 30.0],
            'currency': ['USD', 'EUR', 'GBP'],
            'state': ['State 1', 'State 2', 'State 3'],
            'balance': [1000.0, 2000.0, 3000.0]
        }
        )

        expected_output = pd.DataFrame({
            'id': [31, 32, 33],
            'datetime': [datetime(2022, 1, 1, 0, 0, 0), datetime(2022, 6, 15, 12, 30, 30),
                         datetime(2022, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['USD', 'EUR', 'GBP'],
            'amount_cur': [130.0, 240.0, 300.0],
            'description': [
                'bank:Revolut, type: Type 1, product: Product A, completed_date: 2022-01-01 00:00:00, description: Description 1, fee: 10.0, state: State 1, balance: 1000.0',
                'bank:Revolut, type: Type 2, product: Product B, completed_date: 2022-06-15 12:30:30, description: Description 2, fee: 20.0, state: State 2, balance: 2000.0',
                'bank:Revolut, type: Type 3, product: Product C, completed_date: 2022-12-31 23:59:59, description: Description 3, fee: 30.0, state: State 3, balance: 3000.0']
        })

        transformer = etl.RevoTransformer()  # currency_converter will be FixedRateCurrencyConverter by default
        transformed_df = transformer.transform(df_revo)
        pd.testing.assert_frame_equal(transformed_df, expected_output)


if __name__ == '__main__':
    unittest.main()
