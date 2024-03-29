import unittest
from datetime import datetime
from unittest import mock

import pandas as pd
from flask import Flask
from flask_testing import TestCase

from mecon.app import db, models
from mecon.data import db_controller


def _create_app():
    app = Flask(__name__, instance_relative_config=False)

    app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///:memory:"
    app.config['TESTING'] = True

    db.init_app(app)
    with app.app_context():
        db.create_all()
    return app


class TestTagsDBData(TestCase):
    def create_app(self):
        return _create_app()

    def setUp(self):
        self.accessor = db_controller.TagsDBAccessor()

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()

    def test_get_non_existing_tag(self):
        returned_tag = self.accessor.get_tag('a non existing tag')
        self.assertIsNone(returned_tag)

    def test_set_and_get_tag(self):
        expected_tag = {'name': 'test_tag1', 'conditions_json': "{'a': 1}"}
        self.accessor.set_tag(**expected_tag)
        returned_tag = self.accessor.get_tag(expected_tag['name'])
        self.assertEqual(expected_tag, returned_tag)

    def test_update_tag(self):
        tag = {'name': 'test_tag2', 'conditions_json': "{'a': 1}"}
        self.accessor.set_tag(**tag)
        updated_tag = {'name': 'test_tag2', 'conditions_json': "{'a': 2}"}
        self.accessor.set_tag(**updated_tag)
        returned_tag = self.accessor.get_tag('test_tag2')
        self.assertEqual(updated_tag, returned_tag)

    def test_non_existing_delete_tag(self):
        self.assertFalse(self.accessor.delete_tag('test_tag3'))

    def test_existing_delete_tag(self):
        tag = {'name': 'test_tag4', 'conditions_json': {"a": 1}}
        self.accessor.set_tag(**tag)
        self.assertTrue(self.accessor.delete_tag('test_tag4'))
        returned_tag = self.accessor.get_tag('test_tag4')
        self.assertEqual(returned_tag, None)

    def test_all_tags(self):
        self.accessor.delete_tag('test_tag1')
        self.accessor.delete_tag('test_tag2')
        self.accessor.delete_tag('test_tag3')
        self.accessor.delete_tag('test_tag4')

        expected_tags = [
            {'name': 'test_tag5', 'conditions_json': "{'a': 1}"},
            {'name': 'test_tag6', 'conditions_json': "{'a': 2}"},
            {'name': 'test_tag7', 'conditions_json': "{'a': 3}"},
        ]
        self.accessor.set_tag(**expected_tags[0])
        self.accessor.set_tag(**expected_tags[1])
        self.accessor.set_tag(**expected_tags[2])

        returned_tags = self.accessor.all_tags()
        self.assertEqual(expected_tags, returned_tags)


class HSBCTransactionsDBAccessorTestCase(TestCase):
    def create_app(self):
        return _create_app()

    def setUp(self):
        self.accessor = db_controller.HSBCTransactionsDBAccessor()

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()

    def test_import_statement_single_dataframe(self):
        # Create a sample DataFrame
        data = {
            'id': [1, 2, 3],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'amount': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3']
        }
        df = pd.DataFrame(data)

        # Call the import_statement method
        self.accessor.import_statement(df)

        # Check if the data is inserted into the database
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)
        pd.testing.assert_frame_equal(transactions, df)

    def test_import_statement_multiple_dataframes(self):
        # Create sample DataFrames
        data1 = {
            'id': [1, 2, 3],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'amount': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3']
        }
        data2 = {
            'id': [4, 5, 6],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'amount': [400.0, 500.0, 600.0],
            'description': ['Transaction 4', 'Transaction 5', 'Transaction 6']
        }
        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        dfs = pd.concat([df1, df2]).reset_index(drop=True)

        # Call the import_statement method
        self.accessor.import_statement([df1, df2])

        # Check if the data is inserted into the database
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 6)
        pd.testing.assert_frame_equal(transactions, dfs)

    def test_import_get_and_delete(self):
        self.accessor.delete_all()

        # Create sample DataFrames
        data1 = {
            'id': [1, 2, 3],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'amount': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3']
        }
        df1 = pd.DataFrame(data1)
        self.accessor.import_statement(df1)
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)
        pd.testing.assert_frame_equal(transactions, df1)

        data2 = {
            'id': [4, 5, 6],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'amount': [400.0, 500.0, 600.0],
            'description': ['Transaction 4', 'Transaction 5', 'Transaction 6']
        }
        df2 = pd.DataFrame(data2)
        self.accessor.import_statement(df2)
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 6)
        pd.testing.assert_frame_equal(transactions, pd.concat([df1, df2]).reset_index(drop=True))

        self.accessor.delete_all()
        transactions = self.accessor.get_transactions()
        self.assertEqual(transactions, None)


class MonzoTransactionsDBAccessorTestCase(TestCase):
    def create_app(self):
        return _create_app()

    def setUp(self):
        self.accessor = db_controller.MonzoTransactionsDBAccessor()

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()

    def test_import_statement_single_dataframe(self):
        # Create a sample DataFrame
        data = {
            'id': [1, 2, 3],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'time': ["00:00:00", "12:30:30", "23:59:59"],
            'transaction_type': ['Expense', 'Income', 'Expense'],
            'name': ['John Doe', 'Jane Smith', 'John Doe'],
            'emoji': ['🍔', '💰', '🍺'],
            'category': ['Food', 'Salary', 'Drinks'],
            'amount': [10.5, 200.0, 15.75],
            'currency': ['USD', 'USD', 'USD'],
            'local_amount': [10.5, 200.0, 15.75],
            'local_currency': ['USD', 'USD', 'USD'],
            'notes_tags': ['Lunch', '', '#nightout'],
            'address': ['123 Main St', '', '456 Elm St'],
            'receipt': ['http://example.com/receipt1', '', 'http://example.com/receipt3'],
            'description': ['Expense 1', 'Income 1', 'Expense 2'],
            'category_split': ['Food', '', 'Drinks'],
            'money_out': [10.5, None, 15.75],
            'money_in': [None, 200.0, None]
        }
        df = pd.DataFrame(data)

        # Call the import_statement method
        self.accessor.import_statement(df)

        # Check if the data is inserted into the database
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)
        pd.testing.assert_frame_equal(transactions, df)

    def test_import_statement_multiple_dataframes(self):
        # Create sample DataFrames
        data1 = {
            'id': [1, 2, 3],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'time': ["00:00:00", "12:30:30", "23:59:59"],
            'transaction_type': ['Expense', 'Income', 'Expense'],
            'name': ['John Doe', 'Jane Smith', 'John Doe'],
            'emoji': ['🍔', '💰', '🍺'],
            'category': ['Food', 'Salary', 'Drinks'],
            'amount': [10.5, 200.0, 15.75],
            'currency': ['USD', 'USD', 'USD'],
            'local_amount': [10.5, 200.0, 15.75],
            'local_currency': ['USD', 'USD', 'USD'],
            'notes_tags': ['Lunch', '', '#nightout'],
            'address': ['123 Main St', '', '456 Elm St'],
            'receipt': ['http://example.com/receipt1', '', 'http://example.com/receipt3'],
            'description': ['Expense 1', 'Income 1', 'Expense 2'],
            'category_split': ['Food', '', 'Drinks'],
            'money_out': [10.5, None, 15.75],
            'money_in': [None, 200.0, None]
        }
        data2 = {
            'id': [4, 5, 6],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'time': ["00:00:00", "12:30:30", "23:59:59"],
            'transaction_type': ['Expense', 'Income', 'Expense'],
            'name': ['John Doe', 'Jane Smith', 'John Doe'],
            'emoji': ['🍕', '💰', '🍻'],
            'category': ['Food', 'Salary', 'Drinks'],
            'amount': [15.0, 250.0, 20.5],
            'currency': ['USD', 'USD', 'USD'],
            'local_amount': [15.0, 250.0, 20.5],
            'local_currency': ['USD', 'USD', 'USD'],
            'notes_tags': ['Dinner', '', '#nightout'],
            'address': ['789 Elm St', '', '123 Main St'],
            'receipt': ['http://example.com/receipt4', '', 'http://example.com/receipt6'],
            'description': ['Expense 3', 'Income 2', 'Expense 4'],
            'category_split': ['Food', '', 'Drinks'],
            'money_out': [15.0, None, 20.5],
            'money_in': [None, 250.0, None]
        }
        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        dfs = pd.concat([df1, df2]).reset_index(drop=True)

        # Call the import_statement method
        self.accessor.import_statement([df1, df2])

        # Check if the data is inserted into the database
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 6)
        pd.testing.assert_frame_equal(transactions, dfs)

    def test_import_get_and_delete(self):
        self.accessor.delete_all()

        # Create sample DataFrames
        data1 = {
            'id': [1, 2, 3],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'time': ["00:00:00", "12:30:30", "23:59:59"],
            'transaction_type': ['Expense', 'Income', 'Expense'],
            'name': ['John Doe', 'Jane Smith', 'John Doe'],
            'emoji': ['🍔', '💰', '🍺'],
            'category': ['Food', 'Salary', 'Drinks'],
            'amount': [10.5, 200.0, 15.75],
            'currency': ['USD', 'USD', 'USD'],
            'local_amount': [10.5, 200.0, 15.75],
            'local_currency': ['USD', 'USD', 'USD'],
            'notes_tags': ['Lunch', '', '#nightout'],
            'address': ['123 Main St', '', '456 Elm St'],
            'receipt': ['http://example.com/receipt1', '', 'http://example.com/receipt3'],
            'description': ['Expense 1', 'Income 1', 'Expense 2'],
            'category_split': ['Food', '', 'Drinks'],
            'money_out': [10.5, None, 15.75],
            'money_in': [None, 200.0, None]
        }
        df1 = pd.DataFrame(data1)
        self.accessor.import_statement(df1)
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)
        pd.testing.assert_frame_equal(transactions, df1)

        data2 = {
            'id': [4, 5, 6],
            'date': ["01/01/2022", "15/06/2022", "31/12/2022"],
            'time': ["00:00:00", "12:30:30", "23:59:59"],
            'transaction_type': ['Expense', 'Income', 'Expense'],
            'name': ['John Doe', 'Jane Smith', 'John Doe'],
            'emoji': ['🍕', '💰', '🍻'],
            'category': ['Food', 'Salary', 'Drinks'],
            'amount': [15.0, 250.0, 20.5],
            'currency': ['USD', 'USD', 'USD'],
            'local_amount': [15.0, 250.0, 20.5],
            'local_currency': ['USD', 'USD', 'USD'],
            'notes_tags': ['Dinner', '', '#nightout'],
            'address': ['789 Elm St', '', '123 Main St'],
            'receipt': ['http://example.com/receipt4', '', 'http://example.com/receipt6'],
            'description': ['Expense 3', 'Income 2', 'Expense 4'],
            'category_split': ['Food', '', 'Drinks'],
            'money_out': [15.0, None, 20.5],
            'money_in': [None, 250.0, None]
        }
        df2 = pd.DataFrame(data2)
        self.accessor.import_statement(df2)
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 6)
        pd.testing.assert_frame_equal(transactions, pd.concat([df1, df2]).reset_index(drop=True))

        self.accessor.delete_all()
        transactions = self.accessor.get_transactions()
        self.assertEqual(transactions, None)


class RevoTransactionsDBAccessorTestCase(TestCase):
    def create_app(self):
        return _create_app()

    def setUp(self):
        self.accessor = db_controller.RevoTransactionsDBAccessor()

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()

    def test_import_statement_single_dataframe(self):
        # Create a sample DataFrame
        data = {
            'id': [1, 2, 3],
            'type': ['Purchase', 'Withdrawal', 'Purchase'],
            'product': ['Credit Card', 'Savings Account', 'Credit Card'],
            'start_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'completed_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'description': ['Purchase 1', 'Withdrawal 1', 'Purchase 2'],
            'amount': [100.0, 50.0, 80.0],
            'fee': [2.0, 1.0, 1.5],
            'currency': ['USD', 'USD', 'USD'],
            'state': ['Completed', 'Completed', 'Completed'],
            'balance': [500.0, 450.0, 420.0]
        }
        df = pd.DataFrame(data)

        # Call the import_statement method
        self.accessor.import_statement(df)

        # Check if the data is inserted into the database
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)
        pd.testing.assert_frame_equal(transactions, df)

    def test_import_statement_multiple_dataframes(self):
        # Create sample DataFrames
        data1 = {
            'id': [1, 2, 3],
            'type': ['Purchase', 'Withdrawal', 'Purchase'],
            'product': ['Credit Card', 'Savings Account', 'Credit Card'],
            'start_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'completed_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'description': ['Purchase 1', 'Withdrawal 1', 'Purchase 2'],
            'amount': [100.0, 50.0, 80.0],
            'fee': [2.0, 1.0, 1.5],
            'currency': ['USD', 'USD', 'USD'],
            'state': ['Completed', 'Completed', 'Completed'],
            'balance': [500.0, 450.0, 420.0]
        }
        data2 = {
            'id': [4, 5, 6],
            'type': ['Purchase', 'Withdrawal', 'Purchase'],
            'product': ['Credit Card', 'Savings Account', 'Credit Card'],
            'start_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'completed_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'description': ['Purchase 3', 'Withdrawal 2', 'Purchase 4'],
            'amount': [120.0, 70.0, 90.0],
            'fee': [2.5, 1.5, 1.0],
            'currency': ['USD', 'USD', 'USD'],
            'state': ['Completed', 'Completed', 'Completed'],
            'balance': [400.0, 370.0, 340.0]
        }
        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        dfs = pd.concat([df1, df2]).reset_index(drop=True)

        # Call the import_statement method
        self.accessor.import_statement([df1, df2])

        # Check if the data is inserted into the database
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 6)
        pd.testing.assert_frame_equal(transactions, dfs)

    def test_import_get_and_delete(self):
        self.accessor.delete_all()

        # Create sample DataFrames
        data1 = {
            'id': [1, 2, 3],
            'type': ['Purchase', 'Withdrawal', 'Purchase'],
            'product': ['Credit Card', 'Savings Account', 'Credit Card'],
            'start_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'completed_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'description': ['Purchase 1', 'Withdrawal 1', 'Purchase 2'],
            'amount': [100.0, 50.0, 80.0],
            'fee': [2.0, 1.0, 1.5],
            'currency': ['USD', 'USD', 'USD'],
            'state': ['Completed', 'Completed', 'Completed'],
            'balance': [500.0, 450.0, 420.0]
        }
        df1 = pd.DataFrame(data1)
        self.accessor.import_statement(df1)
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)
        pd.testing.assert_frame_equal(transactions, df1)

        data2 = {
            'id': [4, 5, 6],
            'type': ['Purchase', 'Withdrawal', 'Purchase'],
            'product': ['Credit Card', 'Savings Account', 'Credit Card'],
            'start_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'completed_date': ["2022-01-01 00:00:00", "2022-06-15 12:30:30", "2022-12-31 23:59:59"],
            'description': ['Purchase 3', 'Withdrawal 2', 'Purchase 4'],
            'amount': [120.0, 70.0, 90.0],
            'fee': [2.5, 1.5, 1.0],
            'currency': ['USD', 'USD', 'USD'],
            'state': ['Completed', 'Completed', 'Completed'],
            'balance': [400.0, 370.0, 340.0]
        }
        df2 = pd.DataFrame(data2)
        self.accessor.import_statement(df2)
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 6)
        pd.testing.assert_frame_equal(transactions, pd.concat([df1, df2]).reset_index(drop=True))

        self.accessor.delete_all()
        transactions = self.accessor.get_transactions()
        self.assertEqual(transactions, None)


class TransactionsDBAccessorssorTestCase(TestCase):
    def create_app(self):
        return _create_app()

    def setUp(self):
        self.accessor = db_controller.TransactionsDBAccessor()

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()

    def _load_db(self):
        db_controller.HSBCTransactionsDBAccessor().delete_all()
        db_controller.HSBCTransactionsDBAccessor().import_statement(
            pd.DataFrame(
                {
                    'id': [1, 2, 3],
                    'date': ["01/01/2023", "15/06/2023", "31/12/2023"],
                    'amount': ['1,000.0', '2,000.0', '3,000.0'],
                    'description': ['Transaction 1', 'Transaction 2', 'Transaction 3']
                }
            )
        )

        db_controller.MonzoTransactionsDBAccessor().delete_all()
        db_controller.MonzoTransactionsDBAccessor().import_statement(
            pd.DataFrame(
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
        )

        db_controller.RevoTransactionsDBAccessor().delete_all()
        db_controller.RevoTransactionsDBAccessor().import_statement(
            pd.DataFrame(
                {
                    'id': [1, 2, 3],
                    'type': ['Type 1', 'Type 2', 'Type 3'],
                    'product': ['Product A', 'Product B', 'Product C'],
                    'start_date': ["2021-01-01 00:00:00", "2021-06-15 12:30:30", "2021-12-31 23:59:59"],
                    'completed_date': ["2021-01-01 00:00:00", "2021-06-15 12:30:30", "2021-12-31 23:59:59"],
                    'description': ['Description 1', 'Description 2', 'Description 3'],
                    'amount': [100.0, 200.0, 300.0],
                    'fee': [10.0, 20.0, 30.0],
                    'currency': ['USD', 'EUR', 'GBP'],
                    'state': ['State 1', 'State 2', 'State 3'],
                    'balance': [1000.0, 2000.0, 3000.0]
                }
            )
        )

        expected_df = pd.DataFrame(
            {
                'id': [31, 32, 33, 21, 22, 23, 11, 12, 13],
                'datetime': [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 6, 15, 12, 30, 30),
                             datetime(2021, 12, 31, 23, 59, 59), datetime(2022, 1, 1, 0, 0, 0),
                             datetime(2022, 6, 15, 12, 30, 30), datetime(2022, 12, 31, 23, 59, 59),
                             datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 6, 15, 0, 0, 0),
                             datetime(2023, 12, 31, 0, 0, 0)],
                'amount': [130.0, 240.0, 300.0, 100.0, 50.0, 200.0, 1000.0, 2000.0, 3000.0],
                'currency': ['USD', 'EUR', 'GBP', 'GBP', 'GBP', 'GBP', 'GBP', 'GBP', 'GBP'],
                'amount_cur': [100.0, 200.0, 300.0, 100.0, 50.0, 200.0, 1000.0, 2000.0, 3000.0],
                'description': [
                    'bank:Revolut, type: Type 1, product: Product A, completed_date: 2021-01-01 00:00:00, description: Description 1, fee: 10.0, state: State 1, balance: 1000.0',
                    'bank:Revolut, type: Type 2, product: Product B, completed_date: 2021-06-15 12:30:30, description: Description 2, fee: 20.0, state: State 2, balance: 2000.0',
                    'bank:Revolut, type: Type 3, product: Product C, completed_date: 2021-12-31 23:59:59, description: Description 3, fee: 30.0, state: State 3, balance: 3000.0',
                    'bank:Monzo, transaction_type: Payment, name: John Doe, emoji: 💳, category: Shopping, notes_tags: Note 1, address: 123 Main St, receipt: https://example.com/receipt1, description: Description 1, category_split: none, money_out: none, money_in: 100.0',
                    'bank:Monzo, transaction_type: Expense, name: Groceries, emoji: 🛒, category: Food, notes_tags: none, address: none, receipt: none, description: Description 2, category_split: Food/Groceries, money_out: 50.0, money_in: none',
                    'bank:Monzo, transaction_type: Payment, name: Jane Smith, emoji: 💳, category: Shopping, notes_tags: Tag1, Tag2, address: 456 Elm St, receipt: https://example.com/receipt2, description: Description 3, category_split: none, money_out: none, money_in: 200.0',
                    'bank:HSBC, Transaction 1',
                    'bank:HSBC, Transaction 2',
                    'bank:HSBC, Transaction 3'],
                'tags': ['', '', '', '', '', '', '', '', '']
            }
        )
        return expected_df

    def test__transaction_df_validation_columns(self):
        try:
            df_mock = mock.MagicMock()
            df_mock.columns = {'id', 'datetime', 'amount', 'extra_col'}
            db_controller.TransactionsDBAccessor._transaction_df_columns_validation(df_mock)
        except db_controller.InvalidTransactionsDataframeColumnsException as cols_error:
            self.assertEqual(cols_error.missing_cols, {'currency', 'amount_cur', 'description'})
            self.assertEqual(cols_error.extra_cols, {'extra_col'})
        else:
            self.fail(f"Expecting InvalidTransactionsDataframeColumnsException but not raised.")

    def test__transaction_df_validation_types_success(self):
        test_df = pd.DataFrame({
            'id': [0, 1, 2],
            'datetime': [datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 1, 1, 0, 0, 0),
                         datetime(2000, 1, 1, 0, 0, 0)],
            'amount': [.0, 1, .2],
            'currency': ['0', '1', '2'],
            'amount_cur': [.0, 1, .2],
            'description': ['0', '1', '2']

        })
        db_controller.TransactionsDBAccessor._transaction_df_types_validation(test_df)

    def test__transaction_df_validation_types_fail(self):
        try:
            test_df = pd.DataFrame({
                'id': ['0', '1', '2'],
                'datetime': [0, 1, 2],
                'amount': ['0', '1', '2'],
                'currency': [0, 1, 2],
                'amount_cur': ['0', '1', '2'],
                'description': [0, 1, 2]

            })
            db_controller.TransactionsDBAccessor._transaction_df_types_validation(test_df)
        except db_controller.InvalidTransactionsDataframeDataTypesException as inv_types_error:
            expected_errors = [
                "invalid type for column 'id'. expected: int, got: object",
                "invalid type for column 'datetime'. expected: datetime, got: int64",
                "invalid type for column 'amount'. expected: number, got: object",
                "invalid type for column 'currency'. expected: string, got: int64",
                "invalid type for column 'amount_cur'. expected: number, got: object",
                "invalid type for column 'description'. expected: string, got: int64"
            ]
            self.assertEqual(inv_types_error.invalid_types, expected_errors)
        else:
            self.fail(f"Expecting InvalidTransactionsDataframeDataTypesException but not raised.")

    def test_get_transactions(self):
        self.assertIsNone(self.accessor.get_transactions())
        # expected_df = self._load_db()
        df = pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        })
        df.to_sql(models.TransactionsDBTable.__tablename__, db.engine, if_exists='replace', index=False)

        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)
        pd.testing.assert_frame_equal(transactions, df)

    def test_delete_transactions(self):
        df = pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        })
        df.to_sql(models.TransactionsDBTable.__tablename__, db.engine, if_exists='replace', index=False)

        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)

        self.accessor.delete_all()

        self.assertIsNone(self.accessor.get_transactions())

    def test_load_transactions(self):
        self.accessor.delete_all()
        expected_df = self._load_db()

        def convert_amounts(amount, currency, date):
            return currency.map({
                'USD': 1.3,
                'EUR': 1.2,
                'GBP': 1.0
            }) * amount

        with mock.patch.object(db_controller.etl.RevoTransformer, 'convert_amounts', side_effect=convert_amounts):
            self.accessor.load_transactions()

        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 9)
        pd.testing.assert_frame_equal(transactions, expected_df)

    def test_update_tags(self):
        df = pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        })
        df.to_sql(models.TransactionsDBTable.__tablename__, db.engine, if_exists='replace', index=False)

        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)
        pd.testing.assert_frame_equal(transactions, df)

        df_tags = pd.DataFrame(
            {
                'id': [12, 11, 13],  # id order is different
                'tags': ['tag2', '', 'tag2,tag1']
            }
        )
        self.accessor.update_tags(df_tags)

        df['tags'] = ['', 'tag2', 'tag2,tag1']
        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)
        pd.testing.assert_frame_equal(transactions, df)


if __name__ == '__main__':
    unittest.main()
