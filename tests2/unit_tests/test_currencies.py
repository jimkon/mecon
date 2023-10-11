import unittest

from mecon2.utils import currencies as curr


class TestCurrencyConverterABC(unittest.TestCase):
    def test_amount_to_gbp(self):
        class ExampleCurrencyConverted(curr.CurrencyConverterABC):
            def curr_to_GBP(self, curr, date=None):
                if curr == 'EUR' and date=='example_date_1':
                    return 2
                elif curr == 'EUR' and date=='example_date_2':
                    return 3
                else:
                    return 4

        converter = ExampleCurrencyConverted()

        self.assertEqual(converter.amount_to_gbp(2, 'EUR', date='example_date_1'), 1)
        self.assertEqual(converter.amount_to_gbp(3, 'EUR', date='example_date_2'), 1)
        self.assertEqual(converter.amount_to_gbp(4, 'NOT_EUR', date='example_date_1'), 1)
        self.assertEqual(converter.amount_to_gbp(4, 'EUR', date=None), 1)

    def test_validation(self):
        class ExampleCurrencyConverted(curr.CurrencyConverterABC):
            def curr_to_GBP(self, curr, date=None):
                if curr == 'EUR' and date=='example_date_1':
                    return 0
                elif curr == 'EUR' and date=='example_date_2':
                    return -1
                else:
                    return 209348234

        converter = ExampleCurrencyConverted()

        with self.assertRaises(curr.PotentiallyInvalidCurrencyRate):
            converter.amount_to_gbp(2, 'EUR', date='example_date_1')

        with self.assertRaises(curr.PotentiallyInvalidCurrencyRate):
            converter.amount_to_gbp(2, 'EUR', date='example_date_2')

        with self.assertRaises(curr.PotentiallyInvalidCurrencyRate):
            converter.amount_to_gbp(2, 'NOT_EUR', date='example_date_1')

        with self.assertRaises(curr.PotentiallyInvalidCurrencyRate):
            converter.amount_to_gbp(2, 'EUR', date=None)


if __name__ == '__main__':
    unittest.main()
