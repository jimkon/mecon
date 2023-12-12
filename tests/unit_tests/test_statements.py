import unittest
import pandas as pd
from unittest.mock import patch, Mock
from mecon.import_data.statements import StatementCSV, HSBCStatementCSV, MonzoStatementCSV, RevoStatementCSV


class TestStatementCSV(unittest.TestCase):
    def test_dataframe(self):
        statement = StatementCSV(None)
        statement.transform = Mock(return_value=pd.DataFrame({'col2': [4, 5, 6]}))

        df = statement.dataframe()

        statement.transform.assert_called_once()
        self.assertTrue(df.equals(pd.DataFrame({'col2': [4, 5, 6]})))


class TestHSBCStatementCSV(unittest.TestCase):

    def test_load(self):
        # Mock the read_csv method of pd
        with patch('pandas.read_csv') as mock_read_csv:
            statement = HSBCStatementCSV.from_path('example path')

            mock_read_csv.assert_called_with('example path', names=['date', 'description', 'amount'], header=None)


class TestMonzoStatementCSV(unittest.TestCase):

    def test_transform(self):
        statement_df = pd.DataFrame({
            "Transaction ID": [1, 2, 3],
            "Date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "Amount": [10.0, 20.0, 30.0],
            "Time": ["time"] * 3,
            "Type": ["transaction_type"] * 3,
            "Name": ["name"] * 3,
            "Emoji": ["emoji"] * 3,
            "Category": ["category"] * 3,
            "Currency": ["currency"] * 3,
            "Local amount": ["local_amount"] * 3,
            "Local currency": ["local_currency"] * 3,
            "Notes and #tags": ["notes_tags"] * 3,
            "Address": ["address"] * 3,
            "Receipt": ["receipt"] * 3,
            "Description": ["description"] * 3,
            "Category split": ["category_split"] * 3,
            "Money Out": ["money_out"] * 3,
            "Money In": ["money_in"] * 3,
        })

        statement = MonzoStatementCSV(statement_df)
        transformed_df = statement.transform()

        mapping = {
            # "Transaction ID": "id",  removed
            "Date": "date",
            "Time": "time",
            "Type": "transaction_type",
            "Name": "name",
            "Emoji": "emoji",
            "Category": "category",
            "Amount": "amount",
            "Currency": "currency",
            "Local amount": "local_amount",
            "Local currency": "local_currency",
            "Notes and #tags": "notes_tags",
            "Address": "address",
            "Receipt": "receipt",
            "Description": "description",
            "Category split": "category_split",
            "Money Out": "money_out",
            "Money In": "money_in",
        }
        for value in mapping.values():
            # Implement assertions for the transformed DataFrame
            self.assertIn(value, transformed_df.columns)
        # ...


class TestRevoStatementCSV(unittest.TestCase):
    def setUp(self):
        self.path = 'test_revo.csv'

    def test_transform(self):
        statement_df = pd.DataFrame({
            "Type": ["type"],
            "Product": ["product"],
            "Started Date": ["start_date"],
            "Completed Date": ["completed_date"],
            "Description": ["description"],
            "Amount": ["amount"],
            "Fee": ["fee"],
            "Currency": ["currency"],
            "State": ["state"],
            "Balance": ["balance"],
        })

        statement = RevoStatementCSV(statement_df)
        transformed_df = statement.transform()

        mapping = {
            "Type": "type",
            "Product": "product",
            "Started Date": "start_date",
            "Completed Date": "completed_date",
            "Description": "description",
            "Amount": "amount",
            "Fee": "fee",
            "Currency": "currency",
            "State": "state",
            "Balance": "balance",
        }
        for value in mapping.values():
            # Implement assertions for the transformed DataFrame
            self.assertIn(value, transformed_df.columns)


if __name__ == '__main__':
    unittest.main()
