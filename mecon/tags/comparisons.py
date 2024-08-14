import re

from mecon.utils import instance_management


def _any_input_items_in_target_items(input_items, target_items):
    if not isinstance(input_items, list):
        input_items = [input_items]
    for input_item in input_items:
        if input_item in target_items:
            return True
    return False


class CompareOperatorMustReturnBooleanResults(Exception):
    pass


class TypesOfComparedValuesDoNotMatch(Exception):
    ...


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
        try:
            result = self.function(value_1, value_2)
        except TypeError as error:
            raise TypesOfComparedValuesDoNotMatch(f"Compare operation: {self} received unmatched input types."
                                                  f" {value_1=} ({type(value_1)})  {value_2=} ({type(value_2)})"
                                                  f"\n{error=}")

        self.validate_result(result)
        return result

    def validate_result(self, result):
        if result != True and result != False:
            raise CompareOperatorMustReturnBooleanResults(f"Compare operation: {self} return result of {type(result)=}: {result=}")

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

IN = CompareOperator('in', lambda a, b: _any_input_items_in_target_items(a, b))
NOT_IN = CompareOperator('not_in', lambda a, b: not _any_input_items_in_target_items(a, b))

IN_CSV = CompareOperator('in_csv', lambda a, b: _any_input_items_in_target_items(a, b.split(',')))
NOT_IN_CSV = CompareOperator('not_in_csv', lambda a, b: not _any_input_items_in_target_items(a, b.split(',')))
