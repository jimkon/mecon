import re

from mecon2.utils import instance_management


class CompareOperator(instance_management.Multiton):
    """
    Compare operators used by Condition.
    """

    def __init__(self, name, function):
        super().__init__(instance_name=name)
        self.name = name
        self.function = function

    def __call__(self, value_1, value_2):
        return self.apply(value_1, value_2)

    def apply(self, value_1, value_2):
        res = self.function(value_1, value_2)
        return res

    def validate(self):
        pass

    def __repr__(self):
        return f"CompareOp({self.name})"


GREATER = CompareOperator('greater', lambda a, b: a > b)
GREATER_EQUAL = CompareOperator('greater_equal', lambda a, b: a >= b)
LESS = CompareOperator('less', lambda a, b: a < b)
LESS_EQUAL = CompareOperator('less_equal', lambda a, b: a <= b)
EQUAL = CompareOperator('equal', lambda a, b: a == b)

CONTAINS = CompareOperator('contains', lambda a, b: b in a)
NOT_CONTAINS = CompareOperator('not_contains', lambda a, b: b not in a)
REGEX = CompareOperator('regex',
                        lambda a, b: bool(re.search(pattern=b, string=a)) if (a is not None and len(a) > 0) else False)
