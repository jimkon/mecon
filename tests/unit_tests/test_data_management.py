import unittest
from datetime import datetime
from unittest.mock import Mock, patch, call

import pandas as pd

from mecon.data.data_management import DataManager, CachedDataManager


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
        self.tags_metadata_io = Mock()
        self.hsbc_stats_io = Mock()
        self.monzo_stats_io = Mock()
        self.revo_stats_io = Mock()

        self.data_manager = DataManager(
            trans_io=self.transactions_io,
            tags_io=self.tags_io,
            tags_metadata_io=self.tags_metadata_io,
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

    @patch('mecon.data.data_management.tag_stats_from_transactions')
    def test_reset_transaction_tags(self, tag_stats_mck):
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

    def test_get_tags_metadata(self):
        # Mock the return values for dependencies
        tags_data = [
            {'name': 'tag1', 'date_created': '2023-01-01'},
            {'name': 'tag2', 'date_created': '2023-01-02'}
        ]
        metadata_data = [
            {'name': 'tag1', 'total_money_in': 100.0, 'total_money_out': 50.0, 'count': 10},
            {'name': 'tag2', 'total_money_in': 200.0, 'total_money_out': 100.0, 'count': 20}
        ]

        self.tags_io.all_tags.return_value = tags_data
        self.tags_metadata_io.get_all_metadata.return_value = pd.DataFrame(metadata_data)

        # Expected result
        expected_df = pd.DataFrame([
            {'name': 'tag1',
             # 'date_created': '2023-01-01',# TODO date_created currently not returned by the tags_io
             'total_money_in': 100.0, 'total_money_out': 50.0,
             'count': 10},
            {'name': 'tag2',
             # 'date_created': '2023-01-02',# TODO date_created currently not returned by the tags_io
             'total_money_in': 200.0, 'total_money_out': 100.0,
             'count': 20}
        ])

        # Call the method
        result_df = self.data_manager.get_tags_metadata()

        # Assert
        pd.testing.assert_frame_equal(result_df, expected_df)
        # self.tags_io.all_tags.assert_called_once()# TODO date_created currently not returned by the tags_io
        self.tags_metadata_io.get_all_metadata.assert_called_once()

    # # TODO date_created currently not returned by the tags_io
    # def test_get_tags_metadata_no_metadata(self):
    #     # Mock the return values for dependencies
    #     tags_data = [
    #         {'name': 'tag1', 'date_created': '2023-01-01'},
    #         {'name': 'tag2', 'date_created': '2023-01-02'}
    #     ]
    #
    #     self.tags_io.all_tags.return_value = tags_data
    #     self.tags_metadata_io.get_all_metadata.return_value = pd.DataFrame(
    #         columns=['name', 'total_money_in', 'total_money_out', 'count'])
    #
    #     # Expected result (tags with no metadata)
    #     expected_df = pd.DataFrame([
    #         {'name': 'tag1',
    #          # 'date_created': '2023-01-01',# TODO date_created currently not returned by the tags_io
    #          'total_money_in': None, 'total_money_out': None,
    #          'count': None},
    #         {'name': 'tag2',
    #          # 'date_created': '2023-01-02',# TODO date_created currently not returned by the tags_io
    #          'total_money_in': None, 'total_money_out': None,
    #          'count': None}
    #     ])
    #
    #     # Call the method
    #     result_df = self.data_manager.get_tags_metadata()
    #
    #     # Assert
    #     pd.testing.assert_frame_equal(result_df, expected_df)
    #     # self.tags_io.all_tags.assert_called_once()
    #     self.tags_metadata_io.get_all_metadata.assert_called_once()

    def test_replace_tags_metadata(self):
        # Mock the input DataFrame
        metadata_df = pd.DataFrame([
            {'name': 'tag1', 'total_money_in': 100.0, 'total_money_out': 50.0, 'count': 10},
            {'name': 'tag2', 'total_money_in': 200.0, 'total_money_out': 100.0, 'count': 20}
        ])

        # Call the method
        self.data_manager.replace_tags_metadata(metadata_df)

        # Assert
        self.tags_metadata_io.replace_all_metadata.assert_called_once_with(metadata_df)


class TestCachedDataManager(unittest.TestCase):
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
        self.tags_metadata_io = Mock()
        self.hsbc_stats_io = Mock()
        self.monzo_stats_io = Mock()
        self.revo_stats_io = Mock()

        self.data_manager = CachedDataManager(
            trans_io=self.transactions_io,
            tags_io=self.tags_io,
            tags_metadata_io=self.tags_metadata_io,
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

    # @patch('mecon.data.data_management.tag_monitoring.TaggingStatsMonitoringSystem')
    # def test_reset_transaction_tags(self, mock_monitoring):
    @patch('mecon.data.data_management.tag_stats_from_transactions')
    def test_reset_transaction_tags(self, tag_stats_mck):
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

    def test_get_tags_metadata_with_cache(self):
        mock_metadata = Mock()
        self.data_manager._cache.tags_metadata = mock_metadata
        result = self.data_manager.get_tags_metadata()
        self.assertEqual(result, mock_metadata)
        self.tags_metadata_io.get_all_metadata.assert_not_called()

    def test_get_tags_metadata_without_cache(self):
        self.data_manager._cache.tags_metadata = None
        with patch.object(self.tags_metadata_io, 'get_all_metadata', return_value='mock_metadata') as mock_func:
            result = self.data_manager.get_tags_metadata()
            mock_func.assert_called_once()
            self.assertEqual(result, 'mock_metadata')

    def test_replace_tags_metadata(self):
        metadata_df = pd.DataFrame()
        self.data_manager.replace_tags_metadata(metadata_df)
        self.tags_metadata_io.replace_all_metadata.assert_called_once_with(metadata_df)
        self.data_manager._cache.reset_tags_metadata.assert_called_once()


if __name__ == '__main__':
    unittest.main()
