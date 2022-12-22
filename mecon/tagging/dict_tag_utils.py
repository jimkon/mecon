import re


def lower(s): return s.lower()


def upper(s): return s.upper()


field_processing_functions_dict = {
    'lower': lower,
    'upper': upper,
    'abs': abs,
    'int': int,
    'str': str
}


def greater(x, y): return x > y


def greater_equal(x, y): return x >= y


def equals(x, y): return x == y


def less_equal(x, y): return x <= y


def less(x, y): return x < y


def contains(x, y): return y in x


def not_contains(x, y): return y not in x


def regex(x, y): return bool(re.match(y, x))


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


