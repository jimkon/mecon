from mecon2.utils import calendar_utils

class TransformationFunctionDoesNotExistError(Exception):
    pass


class TransformationFunctionAlreadyExistError(Exception):
    pass


class TransformationFunction:
    """
    Transformation operation used by Condition
    """
    _instances = {}

    def __init__(self, name, function):
        self.name = name
        self.function = function

        if name in self._instances:
            raise TransformationFunctionAlreadyExistError
        self._instances[name] = self

    def apply(self, value):
        return self.function(value)

    def validate(self):
        pass

    @classmethod
    def from_key(cls, key):
        if key not in cls._instances:
            raise TransformationFunctionDoesNotExistError
        return cls._instances[key]

    def __call__(self, value):
        return self.apply(value)


STR = TransformationFunction('str', lambda x: str(x))
LOWER = TransformationFunction('lower', lambda x: str(x).lower())
UPPER = TransformationFunction('upper', lambda x: str(x).upper())

INT = TransformationFunction('int', lambda x: int(x))
ABS = TransformationFunction('abs', lambda x: abs(int(x)))

DATE = TransformationFunction('date', lambda x: x.date())  # TODO extract date
TIME = TransformationFunction('time', lambda x: x.time())  # TODO extract time
DAY_OF_WEEK = TransformationFunction('day_of_week', calendar_utils.dayofweek)