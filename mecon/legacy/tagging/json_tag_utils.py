import re
from functools import wraps

from legacy import calendar_utils


def lower(s): return s.lower()


def upper(s): return s.upper()


field_processing_functions_dict = {
    'lower': lower,
    'upper': upper,
    'abs': abs,
    'int': int,
    'str': str,
    'dayofweek': calendar_utils.dayofweek,
}


def _validate_comparison_args_dec(match_func):
    @wraps(match_func)
    def wrapper(x, y):
        if isinstance(x, str) and isinstance(y, str):
            length = min(len(x), len(y))
            x, y = x[:length], y[:length]

        return match_func(x, y)
    return wrapper


@_validate_comparison_args_dec
def greater(x, y): return x > y


@_validate_comparison_args_dec
def greater_equal(x, y): return x >= y


@_validate_comparison_args_dec
def equals(x, y): return x == y


@_validate_comparison_args_dec
def less_equal(x, y):
    return x <= y


@_validate_comparison_args_dec
def less(x, y): return x < y


def contains(x, y): return y in x


def not_contains(x, y): return y not in x


def regex(x, y): return bool(re.search(x, y))


match_funcs_dict = {
    'greater': greater,
    'greater_equal': greater_equal,
    'equals': equals,
    'less_equal': less_equal,
    'less': less,
    'contains': contains,
    'not_contains': not_contains,
    'regex': regex
}


