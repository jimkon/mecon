import pathlib
import tempfile
import unittest
from datetime import datetime
from unittest import mock

import pandas as pd

from mecon.app import db_controller
from mecon.app import db_extension
from mecon.app import models
from mecon.data.data_management import CachedFileDataManager
from mecon.etl.dataset import Dataset
from mecon.tags import tagging


@unittest.skip('In the process of removing the DB')
class TestTagsDBData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = db_extension.DBWrapper(':memory:')
        models.Base.metadata.create_all(cls.db._engine)  # Create all tables
        cls.accessor = db_controller.TagsDBAccessor(cls.db)  # Initialize the accessor

    @classmethod
    def tearDownClass(cls):
        # Cleanup database after each test (optional, you could rely on the in-memory DB being wiped)
        models.Base.metadata.drop_all(cls.db._engine)
        # cls.db.session.close()

    def test_get_non_existing_tag(self):
        returned_tag = self.accessor.get_tag('a non existing tag')
        self.assertIsNone(returned_tag)

    def test_set_and_get_tag(self):
        expected_tag = {'name': 'test_tag1', 'conditions_json': {'a': 1}}
        self.accessor.set_tag(**expected_tag)
        returned_tag = self.accessor.get_tag(expected_tag['name'])
        self.assertEqual(expected_tag, returned_tag)

    def test_update_tag(self):
        tag = {'name': 'test_tag2', 'conditions_json': {'a': 1}}
        self.accessor.set_tag(**tag)
        updated_tag = {'name': 'test_tag2', 'conditions_json': {'a': 2}}
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
        # self.accessor.delete_tag('test_tag1')
        # self.accessor.delete_tag('test_tag2')
        # self.accessor.delete_tag('test_tag3')
        # self.accessor.delete_tag('test_tag4')

        expected_tags = [
            {'name': 'test_tag5', 'conditions_json': {'a': 1}},
            {'name': 'test_tag6', 'conditions_json': {'a': 2}},
            {'name': 'test_tag7', 'conditions_json': {'a': 3}},
        ]
        self.accessor.set_tag(**expected_tags[0])
        self.accessor.set_tag(**expected_tags[1])
        self.accessor.set_tag(**expected_tags[2])

        returned_tags = self.accessor.all_tags()
        self.assertEqual(expected_tags, returned_tags)


@unittest.skip('In the process of removing the DB')
class TestTagsMetadataDBData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = db_extension.DBWrapper(':memory:')  # Using in-memory database for testing
        models.Base.metadata.create_all(cls.db._engine)  # Create all tables
        cls.accessor = db_controller.TagsMetadataDBAccessor(cls.db)  # Initialize the accessor

    @classmethod
    def tearDownClass(cls):
        # Cleanup database after each test (optional, as in-memory DB will be wiped)
        models.Base.metadata.drop_all(cls.db._engine)
        # cls.db.session.close()

    def test_replace_all_metadata(self):
        metadata_df = pd.DataFrame({
            'name': ['tag1', 'tag2'],
            'date_modified': [datetime.now(), datetime.now()],
            'total_money_in': [100.0, 200.0],
            'total_money_out': [50.0, 75.0],
            'count': [10, 20]
        })

        # Replace existing metadata with the new dataframe
        self.accessor.replace_all_metadata(metadata_df)

        # Fetch the data back and verify it
        returned_metadata = self.accessor.get_all_metadata()
        pd.testing.assert_frame_equal(metadata_df, returned_metadata)

    def test_get_all_metadata_empty(self):
        # Initially, the metadata table should be empty
        metadata_df = self.accessor.get_all_metadata()
        self.assertTrue(metadata_df.empty)

    def test_insert_single_metadata(self):
        metadata_df = pd.DataFrame({
            'name': ['tag3'],
            'date_modified': [datetime.now()],
            'total_money_in': [150.0],
            'total_money_out': [80.0],
            'count': [5]
        })

        # Replace metadata, which will insert this row
        self.accessor.replace_all_metadata(metadata_df)

        # Fetch and verify the inserted metadata
        returned_metadata = self.accessor.get_all_metadata()
        expected_metadata = pd.DataFrame({
            'name': ['tag3'],
            'date_modified': [metadata_df['date_modified'].iloc[0]],
            'total_money_in': [150.0],
            'total_money_out': [80.0],
            'count': [5]
        })

        pd.testing.assert_frame_equal(returned_metadata, expected_metadata)

    def test_update_metadata(self):
        metadata_df = pd.DataFrame({
            'name': ['tag4'],
            'date_modified': [datetime.now()],
            'total_money_in': [300.0],
            'total_money_out': [150.0],
            'count': [15]
        })

        # Replace metadata initially
        self.accessor.replace_all_metadata(metadata_df)

        # Update the data for tag4
        updated_metadata_df = pd.DataFrame({
            'name': ['tag4'],
            'date_modified': [datetime.now()],
            'total_money_in': [500.0],
            'total_money_out': [200.0],
            'count': [25]
        })

        # Replace metadata, which will update the existing row
        self.accessor.replace_all_metadata(updated_metadata_df)

        # Fetch and verify the updated metadata
        returned_metadata = self.accessor.get_all_metadata()
        expected_metadata = pd.DataFrame({
            'name': ['tag4'],
            'date_modified': [updated_metadata_df['date_modified'].iloc[0]],
            'total_money_in': [500.0],
            'total_money_out': [200.0],
            'count': [25]
        })

        pd.testing.assert_frame_equal(returned_metadata, expected_metadata)


@unittest.skip('In the process of removing the DB')
class HSBCTransactionsDBAccessorTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = db_extension.DBWrapper(':memory:')
        models.Base.metadata.create_all(cls.db._engine)  # Create all tables
        cls.accessor = db_controller.HSBCTransactionsDBAccessor(cls.db)  # Initialize the accessor

    @classmethod
    def tearDownClass(cls):
        # Cleanup database after each test (optional, you could rely on the in-memory DB being wiped)
        models.Base.metadata.drop_all(cls.db._engine)
        # cls.db.session.close()

    def test_import_statement_single_dataframe(self):
        self.accessor.delete_all()
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
        self.accessor.delete_all()
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


@unittest.skip('In the process of removing the DB')
class MonzoTransactionsDBAccessorTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = db_extension.DBWrapper(':memory:')
        models.Base.metadata.create_all(cls.db._engine)  # Create all tables
        cls.accessor = db_controller.MonzoTransactionsDBAccessor(cls.db)  # Initialize the accessor

    @classmethod
    def tearDownClass(cls):
        # Cleanup database after each test (optional, you could rely on the in-memory DB being wiped)
        models.Base.metadata.drop_all(cls.db._engine)
        # cls.db.session.close()

    def test_import_statement_single_dataframe(self):
        self.accessor.delete_all()
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


@unittest.skip('In the process of removing the DB')
class RevoTransactionsDBAccessorTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = db_extension.DBWrapper(':memory:')
        models.Base.metadata.create_all(cls.db._engine)  # Create all tables
        cls.accessor = db_controller.RevoTransactionsDBAccessor(cls.db)  # Initialize the accessor

    @classmethod
    def tearDownClass(cls):
        # Cleanup database after each test (optional, you could rely on the in-memory DB being wiped)
        models.Base.metadata.drop_all(cls.db._engine)
        # cls.db.session.close()

    def test_import_statement_single_dataframe(self):
        self.accessor.delete_all()
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


@unittest.skip('In the process of removing the DB')
class TransactionsDBAccessorssorTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = db_extension.DBWrapper(':memory:')
        models.Base.metadata.create_all(cls.db._engine)  # Create all tables
        cls.accessor = db_controller.TransactionsDBAccessor(cls.db)  # Initialize the accessor

    @classmethod
    def tearDownClass(cls):
        # Cleanup database after each test (optional, you could rely on the in-memory DB being wiped)
        models.Base.metadata.drop_all(cls.db._engine)
        # cls.db.session.close()

    def _load_db(self):
        db_controller.HSBCTransactionsDBAccessor(self.db).delete_all()
        db_controller.HSBCTransactionsDBAccessor(self.db).import_statement(
            pd.DataFrame(
                {
                    'id': [1, 2, 3],
                    'date': ["01/01/2023", "06/15/2023", "12/31/2023"],
                    'amount': ['1,000.0', '2,000.0', '3,000.0'],
                    'description': ['Transaction 1', 'Transaction 2', 'Transaction 3']
                }
            )
        )

        db_controller.MonzoTransactionsDBAccessor(self.db).delete_all()
        db_controller.MonzoTransactionsDBAccessor(self.db).import_statement(
            pd.DataFrame(
                {
                    'id': [1, 2, 3],
                    'date': ["2022-01-01", "2022-06-15", "2022-12-31"],
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

        db_controller.RevoTransactionsDBAccessor(self.db).delete_all()
        db_controller.RevoTransactionsDBAccessor(self.db).import_statement(
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
                'id': ['RVLTd20210101t000000ap13000i31', 'RVLTd20210615t123030ap24000i32',
                       'RVLTd20211231t235959ap30000i33',
                       'MZNd20220101t000000ap10000i1', 'MZNd20220615t123030ap5000i2', 'MZNd20221231t235959ap20000i3',
                       'HSBCd20230101t000000ap100000i1', 'HSBCd20230615t000000ap200000i2',
                       'HSBCd20231231t000000ap300000i3'],
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
            'id': ['0', '1', '2'],
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
                'id': [0, 1, 2],
                'datetime': [0, 1, 2],
                'amount': ['0', '1', '2'],
                'currency': [0, 1, 2],
                'amount_cur': ['0', '1', '2'],
                'description': [0, 1, 2]

            })
            db_controller.TransactionsDBAccessor._transaction_df_types_validation(test_df)
        except db_controller.InvalidTransactionsDataframeDataTypesException as inv_types_error:
            expected_errors = ["Invalid type for column 'id'. Expected string, got: int64",
                               "Invalid type for column 'datetime'. Expected datetime, got: int64",
                               "Invalid type for column 'amount'. Expected numeric, got: object",
                               "Invalid type for column 'currency'. Expected string, got: int64",
                               "Invalid type for column 'amount_cur'. Expected numeric, got: object",
                               "Invalid type for column 'description'. Expected string, got: int64"]
            self.assertEqual(inv_types_error.invalid_types, expected_errors)
        else:
            self.fail(f"Expecting InvalidTransactionsDataframeDataTypesException but not raised.")

    def test__transaction_df_values_validation(self):
        test_df = pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [None, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        })

        with self.assertRaises(db_controller.InvalidTransactionsDataframeDataValueException):
            db_controller.TransactionsDBAccessor._transaction_df_values_validation(test_df)

    def test_get_transactions(self):
        self.assertEqual(len(self.accessor.get_transactions()),
                         0)  # TODO it should return the right columns even if empty

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
        df.to_sql(models.TransactionsDBTable.__tablename__, self.db.engine, if_exists='replace', index=False)

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
        df.to_sql(models.TransactionsDBTable.__tablename__, self.db.engine, if_exists='replace', index=False)

        transactions = self.accessor.get_transactions()
        self.assertEqual(len(transactions), 3)

        self.accessor.delete_all()

        self.assertEqual(len(self.accessor.get_transactions()),
                         0)  # TODO it should return the right columns even if empty

    def test_load_transactions(self):
        self.accessor.delete_all()
        expected_df = self._load_db()

        def convert_amounts(amount, currency, date):
            return currency.map({
                'USD': 1.3,
                'EUR': 1.2,
                'GBP': 1.0
            }) * amount

        with mock.patch.object(db_controller.transformers.RevoStatementTransformer, 'convert_amounts',
                               side_effect=convert_amounts):
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
        df.to_sql(models.TransactionsDBTable.__tablename__, self.db.engine, if_exists='replace', index=False)

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


class CachedFileDataManagerTestDataFlow(unittest.TestCase):
    def setUp(self):
        self.working_dir = tempfile.TemporaryDirectory()
        self.working_dir_path = pathlib.Path(self.working_dir.name)

        self.data_path = self.working_dir_path / 'data/db'
        self.data_path.mkdir(parents=True, exist_ok=True)

        with open(self.data_path / 'transactions.csv', 'w') as tags_file:
            tags_file.write("""id,datetime,amount,currency,amount_cur,description,tags
RVLTd201901217t150315ap833i3634,2019-12-17 15:03:15,8,EUR,10.0,"bank:Revolut, desc example","Afternoon,Friends transfers,MoneyIn,Revolut,Spending,Transfers,All"
RVLTd20200228t150811an833i3635,2020-02-28 15:08:11,-8,EUR,-10.0,"bank:Revolut, desc example","Afternoon,Friends transfers,MoneyOut,Revolut,Spending,Transfers,All"
RVLTd20200508t090618ap666i3636,2020-05-08 09:06:18,6,EUR,8.0,"bank:Revolut, desc example","Alpha Bank,MoneyIn,Morning,Revolut,Inside transfers,Spending,Transfers,All"
RVLTd20210617t180640an485i3637,2021-06-17 18:06:40,-4,EUR,-5.82,"bank:Revolut, desc example","Afternoon,GiffGaff,MoneyOut,Revolut,Other bills,Spending,All"
RVLTd20210625t100635an96i3638,2021-06-25 10:06:35,-1,EUR,-1.16,"bank:Revolut, desc example","Alpha Bank,MoneyOut,Morning,Revolut,Inside transfers,Spending,Transfers,All"
MZNd20230321t082145ap100itx_00009iC3annMpNMjlaD7RZ,2023-03-21 08:21:45,1.0,GBP,1.0,"bank:Monzo, desc example","Alpha Bank,MoneyIn,Monzo,Morning,Spending,Transfers,All"
MZNd20240827t082145ap100itx_00009iC3annMpNMjlaD7RZ,2024-08-27 08:21:45,1.0,GBP,1.0,"bank:Monzo, desc example","Alpha Bank,MoneyIn,Monzo,Morning,Spending,Transfers,All"
""")

        with open(self.data_path / 'tags.csv', 'w') as tags_file:
            tags_file.write("""name,conditions_json,date_created
test_tag,"[{""description.lower"":{""contains"":""something""}}]",2025-01-22 00:40:10
test_tag2,"[{""description"":{""contains"":""something else""}}]",2025-01-21 02:40:10
""")

        self.dataset = Dataset.from_dirpath(self.working_dir_path)
        self.dm = CachedFileDataManager(self.dataset)

    def tearDown(self):
        self.working_dir.cleanup()

    def test_get_tag(self):
        existing_tag = self.dm.get_tag('test_tag')
        self.assertEqual(existing_tag.name, 'test_tag')

        non_existing_tag = self.dm.get_tag('dadsas_tag')
        self.assertIsNone(non_existing_tag)

    def test_update_tag(self):
        existing_tag = self.dm.get_tag('test_tag')
        self.assertEqual(existing_tag.name, 'test_tag')
        self.assertEqual(existing_tag.rule.to_json(), [{'description.lower': {'contains': 'something'}}])
        existing_tag._rule = tagging.Disjunction.from_json([{}])
        self.dm.update_tag(existing_tag, update_tags=False)
        existing_tag = self.dm.get_tag('test_tag')
        self.assertEqual(existing_tag.name, 'test_tag')
        self.assertEqual(existing_tag.rule.to_json(), [{}])

        non_existing_tag = tagging.Tag.from_json_string('test_tag3', '[{}]')
        self.dm.update_tag(non_existing_tag, update_tags=False)
        non_existing_tag = self.dm.get_tag('test_tag3')
        self.assertEqual(non_existing_tag.name, 'test_tag3')
        self.assertEqual(non_existing_tag.rule.to_json(), [{}])
        self.assertEqual(self.dm.tags_df['date_created'].isna().sum(), 0)

    def test_delete_tag(self):
        self.assertIsNotNone(self.dm.get_tag('test_tag'))
        self.dm.delete_tag('test_tag')
        self.assertIsNone(self.dm.get_tag('test_tag'))

    def test_all_tags(self):
        tags = self.dm.all_tags()

        self.assertEqual(len(tags), 2)
        self.assertListEqual([tag.name for tag in tags], ['test_tag', 'test_tag2'])

    def test_tagged_transactions_after_modifying_tags(self):
        transactions = self.dm.get_transactions()
        self.assertEqual(transactions.containing_tags('test_tag1').size(), 0)

        tag = tagging.Tag.from_json_string('test_tag1', '[{}]')
        self.dm.update_tag(tag, update_tags=True)

        transactions = self.dm.get_transactions()
        self.assertEqual(transactions.containing_tags('test_tag1').size(), 7)

        self.dm.delete_tag('test_tag1')

        transactions = self.dm.get_transactions()
        self.assertEqual(transactions.containing_tags('test_tag1').size(), 0)


if __name__ == '__main__':
    unittest.main()
