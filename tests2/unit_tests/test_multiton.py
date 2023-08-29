import unittest

from mecon2.utils.multiton import Multiton, InstanceDoesNotExistError, InstanceAlreadyExistError


class ExampleMultiton(Multiton):
    def __init__(self, name, other_args):
        super().__init__(instance_name=name)


class TestInstances(unittest.TestCase):
    def test_instances(self):
        ExampleMultiton('a', None)
        ExampleMultiton('b', None)
        ExampleMultiton('c', None)

        with self.assertRaises(InstanceAlreadyExistError):
            ExampleMultiton('a', None)

        self.assertIsNotNone(ExampleMultiton.from_key('a'))
        self.assertIsNotNone(ExampleMultiton.from_key('b'))
        self.assertIsNotNone(ExampleMultiton.from_key('c'))

        with self.assertRaises(InstanceDoesNotExistError):
            ExampleMultiton.from_key('d')

        del ExampleMultiton._instances['a']
        del ExampleMultiton._instances['b']
        del ExampleMultiton._instances['c']

