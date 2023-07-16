import re


class ComparisonFunctionDoesNotExistError(Exception):
    pass


class ComparisonFunctionAlreadyExistError(Exception):
    pass


class CompareOperator:
    """
    Compare operators used by Condition.
    """
    _instances = {}

    def __init__(self, name, function):
        self.name = name
        self.function = function

        if name in self._instances:
            raise ComparisonFunctionAlreadyExistError
        self._instances[name] = self

    def __call__(self, value_1, value_2):
        return self.apply(value_1, value_2)

    def apply(self, value_1, value_2):
        return self.function(value_1, value_2)

    def validate(self):
        pass

    @classmethod
    def from_key(cls, key):
        if key not in cls._instances:
            raise ComparisonFunctionDoesNotExistError
        return cls._instances[key]


GREATER = CompareOperator('greater', lambda a, b: a > b)
GREATER_EQUAL = CompareOperator('greater_equal', lambda a, b: a >= b)
LESS = CompareOperator('less', lambda a, b: a < b)
LESS_EQUAL = CompareOperator('less_equal', lambda a, b: a <= b)
EQUAL = CompareOperator('equal', lambda a, b: a == b)

CONTAINS = CompareOperator('contains', lambda a, b: b in a)
NOT_CONTAINS = CompareOperator('not_contains', lambda a, b: b not in a)
REGEX = CompareOperator('regex', lambda a, b: bool(re.search(a, b)))
