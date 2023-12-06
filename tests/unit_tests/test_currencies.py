import unittest
from datetime import datetime
from unittest.mock import patch, Mock, call

import requests
from forex_python.converter import RatesNotAvailableError

from mecon.utils import currencies as curr


class TestCurrencyConverterABC(unittest.TestCase):
    def test_amount_to_gbp(self):
        class ExampleCurrencyConverted(curr.CurrencyConverterABC):
            def curr_to_GBP(self, curr, date=None):
                if curr == 'EUR' and date == 'example_date_1':
                    return 2
                elif curr == 'EUR' and date == 'example_date_2':
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
                if curr == 'EUR' and date == 'example_date_1':
                    return 0
                elif curr == 'EUR' and date == 'example_date_2':
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


class TestFixedRateCurrencyConverter(unittest.TestCase):
    def test_curr_to_GBP(self):
        # Mock the currency rates
        currency_rates = {
            'USD': 1.3,
            'EUR': 1.2,
            'GBP': 1.0
        }

        # Create an instance of the FixedRateCurrencyConverter
        converter = curr.FixedRateCurrencyConverter(**currency_rates)

        # Test the curr_to_GBP method
        gbp_rate = converter.curr_to_GBP('USD')
        self.assertEqual(gbp_rate, 1.3)

        gbp_rate = converter.curr_to_GBP('EUR')
        self.assertEqual(gbp_rate, 1.2)

        gbp_rate = converter.curr_to_GBP('GBP')
        self.assertEqual(gbp_rate, 1.0)

        # Test when currency is not in the rates
        gbp_rate = converter.curr_to_GBP('CAD')
        self.assertEqual(gbp_rate, 1.)


class TestForexLookupCurrencyConverter(unittest.TestCase):
    def setUp(self) -> None:
        self.prev_n_tries = curr.N_RETRIES_OF_FOREX_CURRENCY_LOOKUP
        curr.N_RETRIES_OF_FOREX_CURRENCY_LOOKUP = 10

    def tearDown(self) -> None:
        curr.N_RETRIES_OF_FOREX_CURRENCY_LOOKUP = self.prev_n_tries

    @patch('mecon.utils.currencies.CurrencyRates')
    def test_curr_to_GBP(self, MockCurrencyRates):
        # Mock the CurrencyRates class
        mock_currency_rates = MockCurrencyRates.return_value

        # Create an instance of the ForexLookupCurrencyConverter
        converter = curr.ForexLookupCurrencyConverter()
        converter.curr_to_GBP('USD', date=datetime.now())

        # Test the assertion in curr_to_GBP
        with self.assertRaises(AssertionError):
            converter.curr_to_GBP('USD', date=None)

    @patch('mecon.utils.currencies.CurrencyRates')
    def test_forex_lookup_success(self, MockCurrencyRates):
        # Mock the CurrencyRates class and its methods
        mock_rates_instance = MockCurrencyRates.return_value
        mock_rates_instance.convert.return_value = 1.3

        # Create an instance of the ForexLookupCurrencyConverter
        converter = curr.ForexLookupCurrencyConverter()

        # Mock the datetime
        date = datetime(2023, 9, 30)

        # Test the forex_lookup method when the first conversion runs successfully
        gbp_rate = converter.forex_lookup('USD', date)
        self.assertEqual(gbp_rate, 1.3)
        mock_rates_instance.convert.assert_called_once_with('GBP', 'USD', 1, date)

    @patch('mecon.utils.currencies.CurrencyRates')
    def test_forex_lookup_retry(self, MockCurrencyRates):
        # Mock the CurrencyRates class and its methods
        mock_rates_instance = MockCurrencyRates.return_value
        mock_rates_instance.convert.side_effect = [RatesNotAvailableError, RatesNotAvailableError, 1.3]

        # Create an instance of the ForexLookupCurrencyConverter
        converter = curr.ForexLookupCurrencyConverter()

        # Test the forex_lookup method when convert fails twice and raises ConnectionError
        gbp_rate = converter.forex_lookup('USD', datetime(2023, 9, 30))
        self.assertEqual(gbp_rate, 1.3)
        self.assertEqual(mock_rates_instance.convert.call_count, 3)
        mock_rates_instance.convert.assert_has_calls([
            call('GBP', 'USD', 1, datetime(2023, 9, 30)),
            call('GBP', 'USD', 1, datetime(2023, 9, 29)),
            call('GBP', 'USD', 1, datetime(2023, 9, 28)),
        ])

    @patch('mecon.utils.currencies.CurrencyRates')
    def test_forex_lookup_connection_error(self, MockCurrencyRates):
        # Mock the CurrencyRates class and its methods
        mock_rates_instance = MockCurrencyRates.return_value
        mock_rates_instance.convert.side_effect = requests.exceptions.ConnectionError

        # Create an instance of the ForexLookupCurrencyConverter
        converter = curr.ForexLookupCurrencyConverter()

        # Test the forex_lookup method when convert fails twice and raises ConnectionError
        with self.assertRaises(RatesNotAvailableError):
            converter.forex_lookup('USD', datetime(2023, 9, 30))
        self.assertEqual(mock_rates_instance.convert.call_count, 1)
        mock_rates_instance.convert.assert_has_calls([
            call('GBP', 'USD', 1, datetime(2023, 9, 30)),
        ])

    @patch('mecon.utils.currencies.CurrencyRates')
    def test_forex_lookup_rates_not_available(self, MockCurrencyRates):
        # Mock the CurrencyRates class and its methods
        mock_rates_instance = MockCurrencyRates.return_value
        mock_rates_instance.convert.side_effect = RatesNotAvailableError

        # Create an instance of the ForexLookupCurrencyConverter
        converter = curr.ForexLookupCurrencyConverter()

        # Test the forex_lookup method when RatesNotAvailableError is raised
        with self.assertRaises(RatesNotAvailableError):
            converter.forex_lookup('USD', datetime(2023, 9, 30))
        self.assertEqual(mock_rates_instance.convert.call_count, 10)


class TestCachedForexLookupCurrencyConverter(unittest.TestCase):
    @patch('mecon.utils.currencies.pathlib.Path')
    def test_curr_to_GBP_empty_lookup_dict(self, mock_pathlib):
        # Mock the configuration
        mock_pathlib.return_value.exists.return_value = False

        # Mock the ForexLookupCurrencyConverter
        mock_forex_converter = Mock()
        mock_forex_converter.curr_to_GBP.return_value = 1.3

        # Create an instance of the CachedForexLookupCurrencyConverter
        converter = curr.CachedForexLookupCurrencyConverter()
        converter._forex_converter = mock_forex_converter

        # Test curr_to_GBP when the lookup_dict is empty
        gbp_rate = converter.curr_to_GBP('USD', datetime(2023, 9, 30))
        self.assertEqual(gbp_rate, 1.3)
        mock_forex_converter.curr_to_GBP.assert_called_once_with('USD', datetime(2023, 9, 30))

    @patch('mecon.utils.currencies.pathlib.Path')
    def test_curr_to_GBP_not_empty_lookup_dict(self, mock_pathlib):
        # Mock the configuration
        mock_pathlib.return_value.exists.return_value = True
        mock_pathlib.return_value.read_text.return_value = '{"USD": {"20233009": 1.3}}'
        # mock_json.dumps.return_value = '{}'

        # Mock the ForexLookupCurrencyConverter
        mock_forex_converter = Mock()
        mock_forex_converter.curr_to_GBP.return_value = 1.3

        # Create an instance of the CachedForexLookupCurrencyConverter
        converter = curr.CachedForexLookupCurrencyConverter()
        converter._forex_converter = mock_forex_converter

        # Test curr_to_GBP when the lookup_dict is empty
        gbp_rate = converter.curr_to_GBP('USD', datetime(2023, 9, 30))
        self.assertEqual(gbp_rate, 1.3)
        mock_forex_converter.curr_to_GBP.assert_not_called()


class TestHybridLookupCurrencyConverter(unittest.TestCase):
    @patch('mecon.utils.currencies.FixedRateCurrencyConverter', autospec=True)
    @patch('mecon.utils.currencies.CachedForexLookupCurrencyConverter', autospec=True)
    def test_curr_to_GBP_forex_succeeds(self, MockCachedForexLookupCurrencyConverter, MockFixedRateCurrencyConverter):
        # Mock CachedForexLookupCurrencyConverter to succeed
        mock_cached_converter = Mock()
        mock_cached_converter.curr_to_GBP.return_value = 1.3
        MockCachedForexLookupCurrencyConverter.return_value = mock_cached_converter

        # Mock FixedRateCurrencyConverter
        mock_fixed_converter = Mock()
        MockFixedRateCurrencyConverter.return_value = mock_fixed_converter

        # Create an instance of the HybridLookupCurrencyConverter
        converter = curr.HybridLookupCurrencyConverter()

        # Test curr_to_GBP when CachedForexLookupCurrencyConverter succeeds
        gbp_rate = converter.curr_to_GBP('USD', '2023-09-30')
        self.assertEqual(gbp_rate, 1.3)

        # Ensure FixedRateCurrencyConverter methods were not called
        mock_fixed_converter.curr_to_GBP.assert_not_called()

    @patch('mecon.utils.currencies.FixedRateCurrencyConverter', autospec=True)
    @patch('mecon.utils.currencies.CachedForexLookupCurrencyConverter', autospec=True)
    def test_curr_to_GBP_forex_fails(self, MockCachedForexLookupCurrencyConverter, MockFixedRateCurrencyConverter):
        # Mock CachedForexLookupCurrencyConverter to raise forex_python.converter.RatesNotAvailableError
        mock_cached_converter = Mock()
        mock_cached_converter.curr_to_GBP.side_effect = RatesNotAvailableError('Rates not available')
        MockCachedForexLookupCurrencyConverter.return_value = mock_cached_converter

        # Mock FixedRateCurrencyConverter to succeed
        mock_fixed_converter = Mock()
        mock_fixed_converter.curr_to_GBP.return_value = 1.4
        MockFixedRateCurrencyConverter.return_value = mock_fixed_converter

        # Create an instance of the HybridLookupCurrencyConverter
        converter = curr.HybridLookupCurrencyConverter()

        # Test curr_to_GBP when CachedForexLookupCurrencyConverter fails and FixedRateCurrencyConverter succeeds
        gbp_rate = converter.curr_to_GBP('USD', '2023-09-30')
        self.assertEqual(gbp_rate, 1.4)

        # Ensure FixedRateCurrencyConverter methods were called
        mock_fixed_converter.curr_to_GBP.assert_called_once_with('USD', '2023-09-30')


if __name__ == '__main__':
    unittest.main()
