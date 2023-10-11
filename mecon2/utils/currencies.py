import abc


class PotentiallyInvalidCurrencyRate(Exception):
    pass


class CurrencyConverterABC(abc.ABC):
    def amount_to_gbp(self, amount, currency, date=None):
        rate = self.curr_to_GBP(currency, date)

        if rate <= 0 or rate >= 1000:
            raise PotentiallyInvalidCurrencyRate(f"Rate: {rate} for {currency} on {date}")

        result = amount / rate
        return result

    @abc.abstractmethod
    def curr_to_GBP(self, curr, date=None):
        pass


class FixedRateCurrencyConverter(CurrencyConverterABC):
    currency_rates = {
        'USD': 1.3,
        'EUR': 1.2,
        'GBP': 1.0
    }

    def __init__(self, **currency_rates):
        self._rates = currency_rates if len(currency_rates) > 0 else FixedRateCurrencyConverter.currency_rates
        t = 0

    def curr_to_GBP(self, curr, date=None):
        if curr not in self._rates:
            return 1
        return self._rates[curr]

# pip install forex-python
#
# from forex_python.converter import CurrencyRates
#
# def amount_to_gbp(amount, currency, date):
#     # Create a CurrencyRates object to access exchange rate data
#     c = CurrencyRates()
#
#     # Convert the amount to GBP using historical exchange rate data
#     gbp_amount = c.convert(currency, 'GBP', amount, date)
#
#     return gbp_amount
#
# amount = 100  # Original amount
# currency = 'EUR'  # Original currency (Euro)
# date = '2023-07-15'  # Date for historical exchange rate
#
# converted_amount = amount_to_gbp(amount, currency, date)
# print(f"{amount} {currency} is equivalent to {converted_amount:.2f} GBP on {date}")
