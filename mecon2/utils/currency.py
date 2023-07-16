
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
            return .0
        return self._rates[curr]


CURRENCY_CONVERTER = FixedRateCurrencyConverter()


def currency_rate_function(curr):
    # raise NotImplemented()
    return CURRENCY_CONVERTER.curr_to_GBP(curr)


