import abc
from datetime import datetime
from functools import lru_cache

from forex_python.converter import CurrencyRates


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


class ForexLookupCurrencyConverter(CurrencyConverterABC):
    @lru_cache(4096)
    def curr_to_GBP(self, curr, date=None):
        assert date is not None, f"Did not expect date=None when converting {curr}"

        c = CurrencyRates()
        gbp_amount = c.convert('GBP', curr, 1, date)

        return gbp_amount


class CachedForexLookupCurrencyConverter(ForexLookupCurrencyConverter):
    #  TODO implement the cached version where each look up is stored in a file in order to enable offline exec
    pass
