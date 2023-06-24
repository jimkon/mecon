import unittest
from unittest.mock import patch

import pandas as pd

from mecon2.data import etl



class HSBCTransformerTest(unittest.TestCase):
    def test_transform_hsbc_transactions(self):
        df_hsbc = pd.DataFrame({
            'id': [1, 2, 3],
            'date': ['2022-01-01', '2022-01-02', '2022-01-03'],
            'amount': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3']
        })

        expected_output = pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-03']),
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3']
        })

        transformer = etl.HSBCTransformer()
        transformed_df = transformer.transform(df_hsbc)
        pd.testing.assert_frame_equal(transformed_df, expected_output)


class MonzoTransformerTest(unittest.TestCase):
    def test_transform_monzo_transactions(self):
        df_monzo = pd.DataFrame({
            'id': [1, 2, 3],
            'date': ['2022-01-01', '2022-01-02', '2022-01-03'],
            'time': ['10:00:00', '11:00:00', '12:00:00'],
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
        })

        expected_output = pd.DataFrame({
            'id': [21, 22, 23],
            'datetime': pd.to_datetime(['2022-01-01 10:00:00', '2022-01-02 11:00:00', '2022-01-03 12:00:00']),
            'amount': [100.0, 50.0, 200.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 50.0, 200.0],
            'description': ['transaction_type: Payment, name: John Doe, emoji: 💳, category: Shopping, notes_tags: Note 1, address: 123 Main St, receipt: https://example.com/receipt1, description: Description 1, category_split: none, money_out: none, money_in: 100.0',
                            'transaction_type: Expense, name: Groceries, emoji: 🛒, category: Food, notes_tags: none, address: none, receipt: none, description: Description 2, category_split: Food/Groceries, money_out: 50.0, money_in: none',
                            'transaction_type: Payment, name: Jane Smith, emoji: 💳, category: Shopping, notes_tags: Tag1, Tag2, address: 456 Elm St, receipt: https://example.com/receipt2, description: Description 3, category_split: none, money_out: none, money_in: 200.0']
        })

        transformer = etl.MonzoTransformer()
        transformed_df = transformer.transform(df_monzo)
        pd.testing.assert_frame_equal(transformed_df, expected_output)


class RevoTransformerTest(unittest.TestCase):
    # @patch('mecon2.utils.currency.currency_rate_function')  # Mocking the currency_rate_function
    # def test_transform_revo_transactions(self, mock_currency_rate_function):
        # Mocking the currency conversion rates
        # mock_currency_rate_function.side_effect = lambda cur: {
        #     'GBP': 1.0,
        #     'EUR': 0.85,
        #     'USD': 0.73
        # }[cur]
    def test_transform_revo_transactions(self):
        df_revo = pd.DataFrame({
            'id': [1, 2, 3],
            'type': ['Type 1', 'Type 2', 'Type 3'],
            'product': ['Product A', 'Product B', 'Product C'],
            'start_date': pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-03']),
            'completed_date': pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-03']),
            'description': ['Description 1', 'Description 2', 'Description 3'],
            'amount': [130.0, 240.0, 300.0],
            'fee': [10.0, 20.0, 30.0],
            'currency': ['USD', 'EUR', 'GBP'],
            'state': ['State 1', 'State 2', 'State 3'],
            'balance': [1000.0, 2000.0, 3000.0]
        })

        expected_output = pd.DataFrame({
            'id': [31, 32, 33],
            'datetime': pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-03']),
            'amount': [100.0, 200.0, 300.0],
            'currency': ['USD', 'EUR', 'GBP'],
            'amount_cur': [130.0, 240.0, 300.0],
            'description': ['type: Type 1, product: Product A, completed_date: 2022-01-01 00:00:00, description: Description 1, fee: 10.0, state: State 1, balance: 1000.0',
                            'type: Type 2, product: Product B, completed_date: 2022-01-02 00:00:00, description: Description 2, fee: 20.0, state: State 2, balance: 2000.0',
                            'type: Type 3, product: Product C, completed_date: 2022-01-03 00:00:00, description: Description 3, fee: 30.0, state: State 3, balance: 3000.0']
        })

        transformer = etl.RevoTransformer()
        transformed_df = transformer.transform(df_revo)
        pd.testing.assert_frame_equal(transformed_df, expected_output)

if __name__ == '__main__':
    unittest.main()
