# import unittest
# from unittest.mock import Mock, patch, call
#
# import pandas as pd
# from pandas import Timestamp
#
# import utils
#
#
# class TestSaveTagChanges(unittest.TestCase):
#     def test_save_tag_changes_one(self):
#         self.data_manager = Mock()
#
#         # Mock the get_tag and update_tag methods
#         self.data_manager.get_tag.return_value = 'mocked_tag_object'
#         self.data_manager.update_tag.return_value = None
#         added_tags = {
#             'id1': ['tag1']
#         }
#         removed_tags = {}
#
#         # Run the function under test
#         with patch('mecon.tags.tag_helpers.add_rule_for_id') as mock_add_rule_for_id:
#             utils.save_tag_changes(added_tags, removed_tags, self.data_manager)
#             mock_add_rule_for_id.assert_called_with('mocked_tag_object', ['id1'])
#
#         # Verify the calls to data_manager
#         self.data_manager.get_tag.assert_called_once_with('tag1')
#         self.data_manager.update_tag.assert_called_once()
#
#     def test_save_tag_changes_more(self):
#         self.data_manager = Mock()
#
#         # Mock the get_tag and update_tag methods
#         self.data_manager.get_tag.side_effect = ['mocked_tag_object1', 'mocked_tag_object2']
#         self.data_manager.update_tag.return_value = None
#         added_tags = {
#             'id1': ['tag1'],
#             'id2': ['tag2']
#         }
#         removed_tags = {}
#
#         # Run the function under test
#         with patch('mecon.tags.tag_helpers.add_rule_for_id') as mock_add_rule_for_id:
#             utils.save_tag_changes(added_tags, removed_tags, self.data_manager)
#             mock_add_rule_for_id.assert_has_calls([call('mocked_tag_object1', ['id1']),
#                                                    call('mocked_tag_object2', ['id2'])])
#
#         # Verify the calls to data_manager
#         self.data_manager.get_tag.assert_has_calls([call('tag1'), call('tag2')])
#         self.assertEqual(self.data_manager.update_tag.call_count, 2)
#
#     def test_save_tag_changes_one_tag_many_ids(self):
#         self.data_manager = Mock()
#
#         # Mock the get_tag and update_tag methods
#         self.data_manager.get_tag.side_effect = ['mocked_tag_object1']
#         self.data_manager.update_tag.return_value = None
#         added_tags = {
#             'id1': ['tag1'],
#             'id2': ['tag1']
#         }
#         removed_tags = {}
#
#         # Run the function under test
#         with patch('mecon.tags.tag_helpers.add_rule_for_id') as mock_add_rule_for_id:
#             utils.save_tag_changes(added_tags, removed_tags, self.data_manager)
#             mock_add_rule_for_id.assert_has_calls([call('mocked_tag_object1', ['id1', 'id2'])])
#
#         # Verify the calls to data_manager
#         self.data_manager.get_tag.assert_has_calls([call('tag1')])
#         self.assertEqual(self.data_manager.update_tag.call_count, 1)
#
#
# class TestSortAndFilter(unittest.TestCase):
#     def setUp(self):
#         from mecon.app.datasets import WorkingDataManager, CachedDBDataManager
#         from mecon.data.transactions import Transactions
#         from mecon.settings import Settings
#         from mecon.data import groupings
#         import pathlib
#
#         datasets_dir = pathlib.Path(__file__).parent.parent.parent / 'datasets'
#         settings = Settings()
#         settings['DATASETS_DIR'] = str(datasets_dir)
#         data_manager = WorkingDataManager()
#         self.transactions = data_manager.get_transactions()
#
#     def test_sort_and_filter(self):
#         all_transactions_df = self.transactions.dataframe()
#         transactions_nt_0_df = utils.sort_and_filter_transactions_df(self.transactions,
#                                                                      'Newest transactions',
#                                                                      0,
#                                                                      5)
#         pd.testing.assert_frame_equal(
#             transactions_nt_0_df.reset_index(drop=True),
#             pd.DataFrame(
#                 {'id': ['MZNd20190426t082145ap100i1'],
#                  'datetime': [Timestamp('2019-04-26 08:21:45')], 'amount': [1.0],
#                  'currency': ['GBP'],
#                  'amount_cur': [1.0],
#                  'description': [
#                      'bank:Monzo, transaction_type: Faster payment, name: THE CURRENCY CLOUD LTD, emoji: none, category: none, notes_tags: REVOLUT, address: none, receipt: none, description: REVOLUT, category_split: none, money_out: none, money_in: 1.0'],
#                  'tags': ['Monzo,Spending,All,Alpha Bank,MoneyIn,Morning']}).reset_index(drop=True)
#         )
#
#         transactions_nt_1_df = utils.sort_and_filter_transactions_df(self.transactions,
#                                                                      'Newest transactions',
#                                                                      1,
#                                                                      5)
#
#         pd.testing.assert_frame_equal(
#             transactions_nt_1_df.reset_index(drop=True),
#             pd.DataFrame(
#                 {'id': ['MZNd20190501t083313ap1000i2', 'RVLTd20190430t084831an1000i33', 'RVLTd20190430t081113ap88i32',
#                         'RVLTd20190429t232439ap2000i31'],
#                  'datetime': [Timestamp('2019-05-01 08:33:13'), Timestamp('2019-04-30 08:48:31'),
#                               Timestamp('2019-04-30 08:11:13'), Timestamp('2019-04-29 23:24:39')],
#                  'amount': [10.0, -10.0, 0.88, 20.0], 'currency': ['GBP', 'GBP', 'GBP', 'GBP'],
#                  'amount_cur': [10.0, -10.0, 0.88, 20.0], 'description': [
#                     'bank:Monzo, transaction_type: Faster payment, name: THE CURRENCY CLOUD LTD, emoji: none, category: none, notes_tags: REVOLUT, address: none, receipt: none, description: REVOLUT, category_split: none, money_out: none, money_in: 10.0',
#                     'bank:Revolut, type: TRANSFER, product: Current, completed_date: 2019-04-30 08:48:33, description: To Dimitrios Kontzedakis, fee: 0.0, state: COMPLETED, balance: 10.88',
#                     'bank:Revolut, type: EXCHANGE, product: Current, completed_date: 2019-04-30 08:11:13, description: Exchanged to GBP, fee: 0.0, state: COMPLETED, balance: 20.88',
#                     'bank:Revolut, type: TOPUP, product: Current, completed_date: 2019-04-29 23:24:39, description: Payment from Kontzedakis Dimitrios, fee: 0.0, state: COMPLETED, balance: 20.0'],
#                  'tags': ['Monzo,Spending,All,Alpha Bank,MoneyIn,Morning',
#                           'Revolut,Transfers,Spending,All,MoneyOut,Inside transfers,Morning',
#                           'Revolut,Spending,All,MoneyIn,Currency exchange,Morning',
#                           'Revolut,Spending,All,MoneyIn,Inside transfers,Night']}).reset_index(drop=True)
#         )
#
#         transactions_lt_0_df = utils.sort_and_filter_transactions_df(self.transactions,
#                                                                      'Least tagged',
#                                                                      0,
#                                                                      5)
#         pd.testing.assert_frame_equal(
#             transactions_lt_0_df.reset_index(drop=True),
#             pd.DataFrame({'id': ['MZNd20200225t191423an0i233', 'MZNd20230329t095457an0i718',
#                                  'MZNd20200206t195159an0i173', 'MZNd20201206t040636an0i382',
#                                  'MZNd20230301t141448an0i649'],
#                           'datetime': [Timestamp('2020-02-25 19:14:23'), Timestamp('2023-03-29 09:54:57'),
#                                        Timestamp('2020-02-06 19:51:59'), Timestamp('2020-12-06 04:06:36'),
#                                        Timestamp('2023-03-01 14:14:48')], 'amount': [0.0, 0.0, 0.0, 0.0, 0.0],
#                           'currency': ['GBP', 'GBP', 'GBP', 'GBP', 'GBP'], 'amount_cur': [0.0, 0.0, 0.0, 0.0, 0.0],
#                           'description': [
#                               'bank:Monzo, transaction_type: Card payment, name: Google, emoji: 💻, category: General, notes_tags: Active card check, address: none, receipt: none, description: GOOGLE *TEMPORARY HOLD g.co/helppay# GBR, category_split: none, money_out: 0.0, money_in: none',
#                               'bank:Monzo, transaction_type: Card payment, name: Google, emoji: 💻, category: Entertainment, notes_tags: Active card check, address: none, receipt: none, description: GOOGLE *TEMPORARY HOLD g.co/helppay# GBR, category_split: none, money_out: 0.0, money_in: none',
#                               'bank:Monzo, transaction_type: Card payment, name: ATM, emoji: 💵, category: Finances, notes_tags: Balance check, address: 122 Fortess Road, receipt: none, description: 130-134 FORTESS ROADSa LONDON        GBR, category_split: none, money_out: 0.0, money_in: none',
#                               'bank:Monzo, transaction_type: Card payment, name: Bfi Player, emoji: 🎞, category: Entertainment, notes_tags: Active card check, address: none, receipt: none, description: BFI PLAYER             +442072551444 GBR, category_split: none, money_out: 0.0, money_in: none',
#                               'bank:Monzo, transaction_type: Card payment, name: Google, emoji: 💻, category: Entertainment, notes_tags: Active card check, address: none, receipt: none, description: GOOGLE *TEMPORARY HOLD g.co/helppay# GBR, category_split: none, money_out: 0.0, money_in: none'],
#                           'tags': ['Monzo,Spending,All,Afternoon', 'Monzo,Spending,All,Morning',
#                                    'Monzo,Spending,All,Afternoon', 'Monzo,Spending,All,Night',
#                                    'Monzo,Spending,All,Afternoon']}).reset_index(drop=True)
#         )
#
#         transactions_lt_1_df = utils.sort_and_filter_transactions_df(self.transactions,
#                                                                      'Least tagged',
#                                                                      1000,
#                                                                      5)
#         pd.testing.assert_frame_equal(
#             transactions_lt_1_df.reset_index(drop=True),
#             pd.DataFrame({'id': ['MZNd20240526t211915an5203i1572', 'MZNd20200214t191311an350i207',
#                                  'RVLTd20200324t090807an650i399', 'MZNd20200322t175345an990i308',
#                                  'MZNd20240416t085649an750i1480'],
#                           'datetime': [Timestamp('2024-05-26 21:19:15'), Timestamp('2020-02-14 19:13:11'),
#                                        Timestamp('2020-03-24 09:08:07'), Timestamp('2020-03-22 17:53:45'),
#                                        Timestamp('2024-04-16 08:56:49')], 'amount': [-52.03, -3.5, -6.5, -9.9, -7.5],
#                           'currency': ['GBP', 'GBP', 'GBP', 'GBP', 'GBP'],
#                           'amount_cur': [-52.03, -3.5, -6.5, -9.9, -7.5], 'description': [
#                     'bank:Monzo, transaction_type: Card payment, name: On The Bab, emoji: 🍜, category: Eating out, notes_tags: none, address: 305 Old Street, receipt: none, description: ON THE BAB             LONDON  EC1V  GBR, category_split: none, money_out: -52.03, money_in: none',
#                     'bank:Monzo, transaction_type: Card payment, name: Co-op, emoji: 🍏, category: Groceries, notes_tags: none, address: Loughborough Junction, receipt: none, description: CO-OP GROUP FOOD       LONDON        GBR, category_split: none, money_out: -3.5, money_in: none',
#                     'bank:Revolut, type: CARD_PAYMENT, product: Current, completed_date: 2020-03-25 01:14:19, description: Spotify, fee: 0.0, state: COMPLETED, balance: 23.82',
#                     'bank:Monzo, transaction_type: Card payment, name: Nisa Local, emoji: 🍏✉️, category: Groceries, notes_tags: none, address: 188-190 Coldharbour Lane, receipt: none, description: NISA LOCAL             LONDON        GBR, category_split: none, money_out: -9.9, money_in: none',
#                     'bank:Monzo, transaction_type: Card payment, name: Viva Cafe Bistro, emoji: 🍽, category: Eating out, notes_tags: none, address: 220-222 Trafalgar Road, receipt: none, description: VIVA CAFE BISTRO       LONDON        GBR, category_split: none, money_out: -7.5, money_in: none'],
#                           'tags': ['Monzo,Tap,Spending,All,MoneyOut,Eating out,Night',
#                                    'Monzo,Super Market,Spending,All,MoneyOut,Living costs,Afternoon',
#                                    'Revolut,Spotify,Other bills,Spending,All,MoneyOut,Morning',
#                                    'Monzo,Super Market,Spending,All,MoneyOut,Living costs,Afternoon',
#                                    'Monzo,Tap,Spending,All,MoneyOut,Eating out,Morning']}).reset_index(drop=True)
#         )
#
#
# if __name__ == '__main__':
#     unittest.main()
