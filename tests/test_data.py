# import unittest
#
# import pandas as pd
#
# from mecon.statements import tagged_statement as data
#
# dummy_df = pd.DataFrame.from_dict({
#     'date': {0: pd.Timestamp('2019-04-11 00:00:00'), 1: pd.Timestamp('2019-04-13 00:00:00'), 2: pd.Timestamp('2019-04-25 00:00:00'), 3: pd.Timestamp('2019-04-26 00:00:00')},
#     'month_date': {0: '2019-04', 1: '2019-04', 2: '2019-04', 3: '2019-04'},
#     'time': {0: '08:06:18', 1: '17:06:40', 2: '09:06:35', 3: '08:21:45'},
#     'amount': {0: 7.04, 1: -5.1216, 2: -1.0208, 3: 1.0},
#     'currency': {0: 'EUR', 1: 'EUR', 2: 'EUR', 3: 'GBP'},
#     'amount_curr': {0: 8.0, 1: -5.82, 2: -1.16, 3: 1.0},
#     'description': {0: 'Payment from Kontzedakis Dimitrios,Bank:Revolut', 1: 'Giffgaff,Bank:Revolut', 2: 'To Dimitrios Kontzedakis,Bank:Revolut', 3: 'Bank:Monzo,Type:Faster payment,Name:THE CURRENCY CLOUD LTD,Emoji:None,Category:None,Notes and #tags:REVOLUT,Address:None,Receipt:None,Description:REVOLUT,Category split:None,'},
#     'tags': {0: ['Revolut', 'Thursday', 'Chania residency'], 1: ['Revolut', 'Giffgaff', 'Other Bills', 'Saturday', 'Chania residency'], 2: ['Revolut', 'Bank transfer', 'Thursday', 'Chania residency'], 3: ['Monzo', 'Friday', 'Chania residency']}}
# )
#
#
# class TestTaggedData(unittest.TestCase):
#     def test_copy(self):
#         data1 = data.TaggedData(dummy_df)
#         data2 = data1.copy()
#         self.assertNotEqual(data1, data2)
#         self.assertTrue(data1.dataframe() is not data2.dataframe())
#         self.assertTrue(data1.dataframe().equals(data2.dataframe()))
#
#
# if __name__ == '__main__':
#     unittest.main()
