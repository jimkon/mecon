import unittest
from unittest.mock import Mock, patch, call

from datetime import datetime
import pandas as pd

from mecon.data.data_management import DataManager, CacheDataManager


class TestDataManager(unittest.TestCase):
    def setUp(self):
        self.transactions_io = Mock()
        self.transactions_io.get_transactions = Mock()
        self.transactions_io.get_transactions.return_value = pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        })
        # self.transactions_io.delete_all = Mock()
        # self.transactions_io.load_transactions = Mock()

        self.tags_io = Mock()
        self.hsbc_stats_io = Mock()
        self.monzo_stats_io = Mock()
        self.revo_stats_io = Mock()

        self.data_manager = DataManager(
            trans_io=self.transactions_io,
            tags_io=self.tags_io,
            hsbc_stats_io=self.hsbc_stats_io,
            monzo_stats_io=self.monzo_stats_io,
            revo_stats_io=self.revo_stats_io
        )
        self.data_manager._cache = Mock()
        # self.data_manager._cache.reset = Mock()

    def test_get_transactions(self):
        with patch('mecon.data.data_management.Transactions') as mock_trans:
            result = self.data_manager.get_transactions()
            self.data_manager._transactions.get_transactions.assert_called_once()
            mock_trans.assert_called_once()

    def test_reset_transactions(self):
        with patch.object(self.data_manager, 'reset_transaction_tags'):
            self.data_manager.reset_transactions()
            self.transactions_io.delete_all.assert_called_once()
            self.transactions_io.load_transactions.assert_called_once()
            self.data_manager.reset_transaction_tags.assert_called_once()

    def test_add_statement_valid_bank(self):
        statement = pd.DataFrame()
        bank = 'HSBC'
        with self.subTest(bank=bank):
            with patch.object(self.data_manager, f'_hsbc_statements') as mock_statements:
                self.data_manager.add_statement(statement, bank)
                mock_statements.import_statement.assert_called_once_with(statement)

        bank = 'Monzo'
        with self.subTest(bank=bank):
            with patch.object(self.data_manager, f'_monzo_statements') as mock_statements:
                self.data_manager.add_statement(statement, bank)
                mock_statements.import_statement.assert_called_once_with(statement)

        bank = 'Revolut'
        with self.subTest(bank=bank):
            with patch.object(self.data_manager, f'_revo_statements') as mock_statements:
                self.data_manager.add_statement(statement, bank)
                mock_statements.import_statement.assert_called_once_with(statement)

    def test_add_statement_invalid_bank(self):
        statement = pd.DataFrame()
        with self.assertRaises(ValueError):
            self.data_manager.add_statement(statement, 'InvalidBank')

    def test_reset_statements(self):
        with patch.object(self.data_manager._hsbc_statements, 'delete_all') as mock_hsbc_delete, \
                patch.object(self.data_manager._monzo_statements, 'delete_all') as mock_monzo_delete, \
                patch.object(self.data_manager._revo_statements, 'delete_all') as mock_revo_delete:
            self.data_manager.reset_statements()
            mock_hsbc_delete.assert_called_once()
            mock_monzo_delete.assert_called_once()
            mock_revo_delete.assert_called_once()

    def test_get_tag(self):
        with patch('mecon.data.data_management.Tag.from_json') as mock_tag_factory:
            tag_name = 'tag1'
            tag_conditions_str = "{'a.str': {'equal': '1'}}"
            self.tags_io.get_tag.return_value = {'name': tag_name, 'conditions_json': tag_conditions_str}
            result = self.data_manager.get_tag(tag_name)
            mock_tag_factory.assert_called_once_with(tag_name, tag_conditions_str)

    def test_get_tag_not_found(self):
        with patch('mecon.data.data_management.Tag.from_json_string') as mock_tag_factory:
            tag_name = 'tag1'
            self.tags_io.get_tag.return_value = None
            result = self.data_manager.get_tag(tag_name)
            mock_tag_factory.assert_not_called()

    def test_update_tag(self):
        tag = Mock()
        with patch.object(self.data_manager, 'reset_transaction_tags') as mock_reset_tags:
            self.data_manager.update_tag(tag)
            self.tags_io.set_tag.assert_called_once()
            mock_reset_tags.assert_called_once()

    def test_update_tag_no_reset(self):
        tag = Mock()
        with patch.object(self.data_manager, 'reset_transaction_tags') as mock_reset_tags:
            self.data_manager.update_tag(tag, update_tags=False)
            self.tags_io.set_tag.assert_called_once()
            mock_reset_tags.assert_not_called()

    def test_delete_tag(self):
        tag_name = 'tag1'
        with patch.object(self.data_manager, 'reset_transaction_tags') as mock_reset_tags:
            self.data_manager.delete_tag(tag_name)
        self.tags_io.delete_tag.assert_called_once_with(tag_name)
        mock_reset_tags.assert_called_once()

    def test_delete_tag_no_reset(self):
        tag_name = 'tag1'
        with patch.object(self.data_manager, 'reset_transaction_tags') as mock_reset_tags:
            self.data_manager.delete_tag(tag_name, update_tags=False)
        self.tags_io.delete_tag.assert_called_once_with(tag_name)
        mock_reset_tags.assert_not_called()

    def test_all_tags(self):
        with patch('mecon.data.data_management.Tag.from_json') as mock_tag_factory:
            tags = [{'name': 'tag1', 'conditions_json': {}}, {'name': 'tag2', 'conditions_json': []}]
            self.tags_io.all_tags.return_value = tags
            result = self.data_manager.all_tags()
            mock_tag_factory.assert_has_calls([call('tag1', {}), call('tag2', [])])

    def test_reset_transaction_tags(self):
        transactions_mock = Mock()
        transactions_mock.reset_tags = Mock(return_value=transactions_mock)
        transactions_mock.apply_tag = Mock(return_value=transactions_mock)
        transactions_mock.dataframe = Mock(return_value='dataframe')
        self.data_manager.all_tags = Mock(return_value=['tag1', 'tag2'])
        with patch.object(self.data_manager, 'get_transactions', return_value=transactions_mock):
            with patch.object(self.data_manager._transactions, 'update_tags') as mock_update_tags:
                self.data_manager.reset_transaction_tags()
                transactions_mock.reset_tags.assert_called_once()
                self.data_manager.all_tags.assert_called_once()
                transactions_mock.apply_tag.assert_has_calls([call('tag1'), call('tag2')])
                mock_update_tags.assert_called_once_with('dataframe')


class TestCacheDataManager(unittest.TestCase):
    def setUp(self):
        self.transactions_io = Mock()
        self.transactions_io.get_transactions = Mock()
        self.transactions_io.get_transactions.return_value = pd.DataFrame({
            'id': [11, 12, 13],
            'datetime': [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 6, 15, 12, 30, 30),
                         datetime(2021, 12, 31, 23, 59, 59)],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['GBP', 'GBP', 'GBP'],
            'amount_cur': [100.0, 200.0, 300.0],
            'description': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'tags': ['', 'tag1', 'tag1,tag2']
        })
        # self.transactions_io.delete_all = Mock()
        # self.transactions_io.load_transactions = Mock()

        self.tags_io = Mock()
        self.hsbc_stats_io = Mock()
        self.monzo_stats_io = Mock()
        self.revo_stats_io = Mock()

        self.data_manager = CacheDataManager(
            trans_io=self.transactions_io,
            tags_io=self.tags_io,
            hsbc_stats_io=self.hsbc_stats_io,
            monzo_stats_io=self.monzo_stats_io,
            revo_stats_io=self.revo_stats_io
        )
        self.data_manager._cache = Mock()
        # self.data_manager._cache.reset = Mock()

    def test_get_transactions(self):
        self.data_manager._cache.transaction = None
        with patch('mecon.data.data_management.Transactions') as mock_trans:
            result = self.data_manager.get_transactions()
            self.data_manager._transactions.get_transactions.assert_called_once()
            mock_trans.assert_called_once()

            result = self.data_manager.get_transactions()
            self.data_manager._transactions.get_transactions.assert_called_once()
            mock_trans.assert_called_once()
            self.assertIsNotNone(self.data_manager._cache.transaction)

    def test_reset_transactions(self):
        with patch.object(self.data_manager, 'reset_transaction_tags'):
            self.data_manager.reset_transactions()
            self.transactions_io.delete_all.assert_called_once()
            self.transactions_io.load_transactions.assert_called_once()
            self.data_manager.reset_transaction_tags.assert_called_once()
            self.data_manager._cache.reset_transactions.assert_called_once()

    def test_add_statement_valid_bank(self):
        statement = pd.DataFrame()
        bank = 'HSBC'
        with self.subTest(bank=bank):
            with patch.object(self.data_manager, f'_hsbc_statements') as mock_statements:
                self.data_manager.add_statement(statement, bank)
                mock_statements.import_statement.assert_called_once_with(statement)

        bank = 'Monzo'
        with self.subTest(bank=bank):
            with patch.object(self.data_manager, f'_monzo_statements') as mock_statements:
                self.data_manager.add_statement(statement, bank)
                mock_statements.import_statement.assert_called_once_with(statement)

        bank = 'Revolut'
        with self.subTest(bank=bank):
            with patch.object(self.data_manager, f'_revo_statements') as mock_statements:
                self.data_manager.add_statement(statement, bank)
                mock_statements.import_statement.assert_called_once_with(statement)

        self.assertEqual(self.data_manager._cache.reset_statements.call_count, 3)

    def test_add_statement_invalid_bank(self):
        statement = pd.DataFrame()
        with self.assertRaises(ValueError):
            self.data_manager.add_statement(statement, 'InvalidBank')

    def test_reset_statements(self):
        with patch.object(self.data_manager._hsbc_statements, 'delete_all') as mock_hsbc_delete, \
                patch.object(self.data_manager._monzo_statements, 'delete_all') as mock_monzo_delete, \
                patch.object(self.data_manager._revo_statements, 'delete_all') as mock_revo_delete:
            self.data_manager.reset_statements()
            mock_hsbc_delete.assert_called_once()
            mock_monzo_delete.assert_called_once()
            mock_revo_delete.assert_called_once()
            self.data_manager._cache.reset_statements.assert_called_once()

    def test_all_tags(self):
        self.data_manager._cache.tags = None
        tags = [{'name': 'tag1', 'conditions_json': {}}, {'name': 'tag2', 'conditions_json': []}]
        self.tags_io.all_tags.return_value = tags
        tags = self.data_manager.all_tags()
        self.assertEqual(tags[0].name, 'tag1')
        self.assertEqual(tags[1].name, 'tag2')
        self.tags_io.all_tags.assert_called_once()

        tags = self.data_manager.all_tags()
        self.assertEqual(tags[0].name, 'tag1')
        self.assertEqual(tags[1].name, 'tag2')
        self.tags_io.all_tags.assert_called_once()

    def test_get_tag(self):
        self.data_manager._cache.tags = None
        tags = [{'name': 'tag1', 'conditions_json': {}}, {'name': 'tag2', 'conditions_json': []}]
        self.tags_io.all_tags.return_value = tags
        tag = self.data_manager.get_tag('tag1')
        self.assertEqual(tag.name, 'tag1')

        tag = self.data_manager.get_tag('tag2')
        self.assertEqual(tag.name, 'tag2')

        tag = self.data_manager.get_tag('tag3')
        self.assertEqual(tag, None)

        self.tags_io.all_tags.assert_called_once()

    def test_update_tag(self):
        tag = Mock()
        with patch.object(self.data_manager, 'reset_transaction_tags') as mock_reset_tags:
            self.data_manager.update_tag(tag)
            self.tags_io.set_tag.assert_called_once()
            self.data_manager._cache.reset_tags.assert_called_once()
            mock_reset_tags.assert_called_once()

    def test_update_tag_no_reset(self):
        tag = Mock()
        with patch.object(self.data_manager, 'reset_transaction_tags') as mock_reset_tags:
            self.data_manager.update_tag(tag, update_tags=False)
            self.tags_io.set_tag.assert_called_once()
            self.data_manager._cache.reset_tags.assert_called_once()
            mock_reset_tags.assert_not_called()

    def test_delete_tag(self):
        tag_name = 'tag1'
        with patch.object(self.data_manager, 'reset_transaction_tags') as mock_reset_tags:
            self.data_manager.delete_tag(tag_name)
            self.tags_io.delete_tag.assert_called_once_with(tag_name)
            self.data_manager._cache.reset_tags.assert_called_once()
            mock_reset_tags.assert_called_once()

    def test_delete_tag_no_reset(self):
        tag_name = 'tag1'
        with patch.object(self.data_manager, 'reset_transaction_tags') as mock_reset_tags:
            self.data_manager.delete_tag(tag_name, update_tags=False)
            self.tags_io.delete_tag.assert_called_once_with(tag_name)
            self.data_manager._cache.reset_tags.assert_called_once()
            mock_reset_tags.assert_not_called()

    @patch('mecon.data.data_management.tag_monitoring.TaggingStatsMonitoringSystem')
    def test_reset_transaction_tags(self, mock_monitoring):
        transactions_mock = Mock()
        transactions_mock.reset_tags = Mock(return_value=transactions_mock)
        transactions_mock.apply_tag = Mock(return_value=transactions_mock)
        transactions_mock.dataframe = Mock(return_value='dataframe')
        self.data_manager.all_tags = Mock(return_value=['tag1', 'tag2'])
        with patch.object(self.data_manager, 'get_transactions', return_value=transactions_mock):
            with patch.object(self.data_manager._transactions, 'update_tags') as mock_update_tags:
                self.data_manager.reset_transaction_tags()
                transactions_mock.reset_tags.assert_called_once()
                self.data_manager.all_tags.assert_called_once()
                self.data_manager._cache.reset_transactions.assert_called_once()
                transactions_mock.apply_tag.assert_has_calls([call('tag1'), call('tag2')])
                mock_update_tags.assert_called_once_with('dataframe')


if __name__ == '__main__':
    unittest.main()
