

class CompareOperator:
    """
    Compare operators used by Condition.
    """
    def __init__(self, name, function):
        self.name = name
        self.function = function

    def __call__(self, *args, **kwargs):
        pass

    def apply(self):
        pass

    def validate(self):
        pass

    @staticmethod
    def factory_method():
        pass


GREATER = CompareOperator('>', lambda a, b: a > b)
