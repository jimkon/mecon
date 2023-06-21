import unittest
from datetime import datetime, date, time

import pandas as pd
from flask import Flask
from flask_testing import TestCase

from mecon2.app import db, models
from mecon2.data import db_controller


class TestTagsDBData(TestCase):
    def create_app(self):
        app = Flask(__name__, instance_relative_config=False)

        app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///:memory:"
        app.config['TESTING'] = True

        db.init_app(app)
        with app.app_context():
            db.create_all()
        return app

    def setUp(self):
        self.accessor = db_controller.TagsDBAccessor()

    def test_get_non_existing_tag(self):
        returned_tag = self.accessor.get_tag('a non existing tag')
        self.assertIsNone(returned_tag)

    def test_set_and_get_tag(self):
        expected_tag = {'name': 'test_tag1', 'conditions_json': {"a": 1}}
        self.accessor.set_tag(**expected_tag)
        returned_tag = self.accessor.get_tag(expected_tag['name'])
        self.assertEqual(expected_tag, returned_tag)

    def test_update_tag(self):
        tag = {'name': 'test_tag2', 'conditions_json': {"a": 1}}
        self.accessor.set_tag(**tag)
        updated_tag = {'name': 'test_tag2', 'conditions_json': {"a": 2}}
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
            {'name': 'test_tag5', 'conditions_json': {"a": 1}},
            {'name': 'test_tag6', 'conditions_json': {"a": 2}},
            {'name': 'test_tag7', 'conditions_json': {"a": 3}},
        ]
        self.accessor.set_tag(**expected_tags[0])
        self.accessor.set_tag(**expected_tags[1])
        self.accessor.set_tag(**expected_tags[2])

        returned_tags = self.accessor.all_tags()
        self.assertEqual(expected_tags, returned_tags)

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()


class HSBCTransactionsDBAccessorTestCase(TestCase):
    def create_app(self):
        app = Flask(__name__)
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        app.config['TESTING'] = True
        db.init_app(app)
        with app.app_context():
            db.create_all()
        return app

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
            'date': [date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)],
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
            'date': [date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)],
            'amount': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3']
        }
        data2 = {
            'id': [4, 5, 6],
            'date': [date(2022, 1, 4), date(2022, 1, 5), date(2022, 1, 6)],
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
            'date': [date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)],
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
            'date': [date(2022, 1, 4), date(2022, 1, 5), date(2022, 1, 6)],
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
        app = Flask(__name__)
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        app.config['TESTING'] = True
        db.init_app(app)
        with app.app_context():
            db.create_all()
        return app

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
            'date': [date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)],
            'time': [time(12, 0, 0), time(13, 30, 0), time(15, 45, 0)],
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
            'date': [date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)],
            'time': [time(12, 0, 0), time(13, 30, 0), time(15, 45, 0)],
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
            'date': [date(2022, 1, 4), date(2022, 1, 5), date(2022, 1, 6)],
            'time': [time(9, 30, 0), time(10, 45, 0), time(14, 15, 0)],
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
            'date': [date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)],
            'time': [time(12, 0, 0), time(13, 30, 0), time(15, 45, 0)],
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
            'date': [date(2022, 1, 4), date(2022, 1, 5), date(2022, 1, 6)],
            'time': [time(9, 30, 0), time(10, 45, 0), time(14, 15, 0)],
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
        app = Flask(__name__)
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        app.config['TESTING'] = True
        db.init_app(app)
        with app.app_context():
            db.create_all()
        return app

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
            'start_date': [datetime(2022, 1, 1, 9, 0, 0), datetime(2022, 1, 2, 14, 30, 0), datetime(2022, 1, 3, 11, 45, 0)],
            'completed_date': [datetime(2022, 1, 1, 10, 0, 0), datetime(2022, 1, 2, 15, 0, 0), datetime(2022, 1, 3, 12, 0, 0)],
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
            'start_date': [datetime(2022, 1, 1, 9, 0, 0), datetime(2022, 1, 2, 14, 30, 0), datetime(2022, 1, 3, 11, 45, 0)],
            'completed_date': [datetime(2022, 1, 1, 10, 0, 0), datetime(2022, 1, 2, 15, 0, 0), datetime(2022, 1, 3, 12, 0, 0)],
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
            'start_date': [datetime(2022, 1, 4, 9, 30, 0), datetime(2022, 1, 5, 14, 45, 0), datetime(2022, 1, 6, 10, 15, 0)],
            'completed_date': [datetime(2022, 1, 4, 10, 0, 0), datetime(2022, 1, 5, 15, 0, 0), datetime(2022, 1, 6, 10, 30, 0)],
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
            'start_date': [datetime(2022, 1, 1, 9, 0, 0), datetime(2022, 1, 2, 14, 30, 0), datetime(2022, 1, 3, 11, 45, 0)],
            'completed_date': [datetime(2022, 1, 1, 10, 0, 0), datetime(2022, 1, 2, 15, 0, 0), datetime(2022, 1, 3, 12, 0, 0)],
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
            'start_date': [datetime(2022, 1, 4, 9, 30, 0), datetime(2022, 1, 5, 14, 45, 0), datetime(2022, 1, 6, 10, 15, 0)],
            'completed_date': [datetime(2022, 1, 4, 10, 0, 0), datetime(2022, 1, 5, 15, 0, 0), datetime(2022, 1, 6, 10, 30, 0)],
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


if __name__ == '__main__':
    unittest.main()
