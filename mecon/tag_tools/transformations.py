from mecon.utils import calendar_utils, instance_management


class TransformationFunction(instance_management.Multiton):
    """
    Transformation operation used by Condition
    """
    def __init__(self, name, function):
        self.name = name
        super().__init__(instance_name=name)
        self.function = function if function is not None else lambda x: x

    def __call__(self, value):
        return self.apply(value)

    def apply(self, value):
        return self.function(value)

    def __repr__(self):
        return f"TransformationFunction({self.name})"


NO_TRANSFORMATION = TransformationFunction('none', None)

STR = TransformationFunction('str', lambda x: str(x))
LOWER = TransformationFunction('lower', lambda x: str(x).lower())
UPPER = TransformationFunction('upper', lambda x: str(x).upper())

INT = TransformationFunction('int', lambda x: int(x))
ABS = TransformationFunction('abs', lambda x: abs(int(x)))

# TODO:v3 datetime transformations don't work
# TODO: v3 add part of day, day of week
DATE = TransformationFunction('date', lambda x: x.date())  # TODO:v3 extract date
TIME = TransformationFunction('time', lambda x: x.time())  # TODO:v3 extract time
DAY_OF_WEEK = TransformationFunction('day_of_week', calendar_utils.dayofweek)

