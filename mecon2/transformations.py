class TransformationFunction:
    def __init__(self, name, function):
        self.name = name
        self.function = function

    def apply(self):
        pass

    def validate(self):
        pass

    @staticmethod
    def factory_method():
        pass


