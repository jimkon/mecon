# TODO
# import unittest
#
# from mecon.app.datasets import (WorkingDatasetDir,
#                                 WorkingDatasetDirInfo)
#
#
# test_dataset_obj = WorkingDatasetDir().set_working_dataset('test')
#
#
# class InfoTestCase(unittest.TestCase):
#     def test_statement_files_info(self):
#         info = WorkingDatasetDirInfo().statement_files_info()
#
#         self.assertSetEqual(set(info.keys()), {'HSBC', 'Monzo', 'Revolut'})
#         self.assertEqual(len(info['HSBC']), 5)
#         self.assertEqual(len(info['Monzo']), 1)
#         self.assertEqual(len(info['Revolut']), 11)
#
#     # def test_reset_db(self):
#     #     self.assertEqual(test_dataset_obj.name, 'test')
#     #
#     #     test_dataset_obj.db.unlink(missing_ok=True)
#     #     self.assertFalse(test_dataset_obj.db.exists())
#     #
#     #     data_bp._reset_db()
#     #
#     #     dm = WorkingDataManager()
#     #     transactions = dm.get_transactions()
#     #
#     #     self.assertEqual(transactions.size(), 570)
#     #     self.assertEqual(transactions.datetime.min(), Timestamp('2019-05-11 16:57:56'))
#     #     self.assertEqual(transactions.datetime.max(), Timestamp('2024-02-18 05:20:42'))
#     #     self.assertEqual(int(transactions.amount.sum()), 8500)
#     #     # self.assertEqual(transactions.currency), 8500)
#     #
#     #     ...
#
#
