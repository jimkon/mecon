import abc


class AbstractBooleanFunction(abc.ABC):
    @abc.abstractmethod
    def compute(self):
        pass


class Condition(AbstractBooleanFunction):
    def __init__(self, field, transformation_op, compare_op, value):
        pass

    def compute(self):
        pass


class Conjuction(AbstractBooleanFunction):
    def __init__(self, abf_list):
        pass

    def compute(self):
        pass


class Disjunction(AbstractBooleanFunction):
    def __init__(self, abf_list):
        pass

    def compute(self):
        pass


class Tag:
    def __init__(self, name, abf):
        pass


class Tagger(abc.ABC):
    def tag(self, tag, taggable_data):
        pass
