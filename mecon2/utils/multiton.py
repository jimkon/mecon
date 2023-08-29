class InstanceAlreadyExistError(Exception):
    pass


class InstanceDoesNotExistError(Exception):
    pass


class Multiton:
    _instances = {}

    def __init__(self, instance_name):
        if instance_name in self._instances:
            raise InstanceAlreadyExistError
        self._instance_name = instance_name
        self._instances[self._instance_name] = self

    @property
    def instance_name(self):
        return self._instance_name

    @classmethod
    def from_key(cls, key):
        if key not in cls._instances:
            raise InstanceDoesNotExistError(f"Instance '{key=}' does not exist.")
        return cls._instances[key]

    @classmethod
    def all_transformations(cls):
        return cls._instances.values()

