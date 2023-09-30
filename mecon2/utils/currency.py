
class FixedRateCurrencyConverter:
    currency_rates = {
        'USD': 1.3,
        'EUR': 1.2,
        'GBP': 1.0
    }

    def __init__(self, **currency_rates):
        self._rates = currency_rates if len(currency_rates) else FixedRateCurrencyConverter.currency_rates

    def curr_to_GBP(self, curr):
        if curr not in self._rates:
            return 1
        return self._rates[curr]


CURRENCY_CONVERTER = FixedRateCurrencyConverter()


def currency_rate_function(curr):
    # raise NotImplemented()
    return CURRENCY_CONVERTER.curr_to_GBP(curr)


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
