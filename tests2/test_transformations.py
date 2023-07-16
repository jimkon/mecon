import unittest
from datetime import datetime, date, time

from mecon2 import transformations as trns


class TestInstances(unittest.TestCase):
    def test_instances(self):
        trns.TransformationFunction('a', None)
        trns.TransformationFunction('b', None)
        trns.TransformationFunction('c', None)

        with self.assertRaises(trns.TransformationFunctionAlreadyExistError):
            trns.TransformationFunction('a', None)

        self.assertIsNotNone(trns.TransformationFunction.from_key('a'))
        self.assertIsNotNone(trns.TransformationFunction.from_key('b'))
        self.assertIsNotNone(trns.TransformationFunction.from_key('c'))

        with self.assertRaises(trns.TransformationFunctionDoesNotExistError):
            trns.TransformationFunction.from_key('d')

        del trns.TransformationFunction._instances['a']
        del trns.TransformationFunction._instances['b']
        del trns.TransformationFunction._instances['c']


class TestTransformationFunctions(unittest.TestCase):
    def test_str(self):
        tf = trns.TransformationFunction.from_key('str')

        self.assertEqual(tf('1'), '1')
        self.assertEqual(tf(1), '1')

    def test_lower(self):
        tf = trns.TransformationFunction.from_key('lower')

        self.assertEqual(tf('a'), 'a')
        self.assertEqual(tf('A'), 'a')

    def test_upper(self):
        tf = trns.TransformationFunction.from_key('upper')

        self.assertEqual(tf('a'), 'A')
        self.assertEqual(tf('A'), 'A')

    def test_int(self):
        tf = trns.TransformationFunction.from_key('int')

        self.assertEqual(tf('1'), 1)
        self.assertEqual(tf(1.1), 1)

    def test_abs(self):
        tf = trns.TransformationFunction.from_key('abs')

        self.assertEqual(tf(1), 1)
        self.assertEqual(tf(-1), 1)

    def test_date(self):
        tf = trns.TransformationFunction.from_key('date')

        self.assertEqual((tf(datetime(2020, 1, 1, 12, 30, 30))), date(2020, 1, 1))

    def test_time(self):
        tf = trns.TransformationFunction.from_key('time')

        self.assertEqual((tf(datetime(2020, 1, 1, 12, 30, 30))), time(12, 30, 30 ))

    def test_day_of_week(self):
        tf = trns.TransformationFunction.from_key('day_of_week')

        self.assertEqual((tf(datetime(2023, 7, 17, 12, 30, 30))), 'Monday')
        self.assertEqual((tf(datetime(2023, 7, 18, 12, 30, 30))), 'Tuesday')
        self.assertEqual((tf(datetime(2023, 7, 19, 12, 30, 30))), 'Wednesday')
        self.assertEqual((tf(datetime(2023, 7, 20, 12, 30, 30))), 'Thursday')
        self.assertEqual((tf(datetime(2023, 7, 21, 12, 30, 30))), 'Friday')
        self.assertEqual((tf(datetime(2023, 7, 22, 12, 30, 30))), 'Saturday')
        self.assertEqual((tf(datetime(2023, 7, 23, 12, 30, 30))), 'Sunday')


if __name__ == '__main__':
    unittest.main()
