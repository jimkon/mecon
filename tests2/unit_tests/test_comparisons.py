import unittest

from mecon2 import comparisons as cmp


class TestInstances(unittest.TestCase):
    def test_instances(self):
        cmp.CompareOperator('a', None)
        cmp.CompareOperator('b', None)
        cmp.CompareOperator('c', None)

        with self.assertRaises(cmp.ComparisonFunctionAlreadyExistError):
            cmp.CompareOperator('a', None)

        self.assertIsNotNone(cmp.CompareOperator.from_key('a'))
        self.assertIsNotNone(cmp.CompareOperator.from_key('b'))
        self.assertIsNotNone(cmp.CompareOperator.from_key('c'))

        with self.assertRaises(cmp.ComparisonFunctionDoesNotExistError):
            cmp.CompareOperator.from_key('d')

        del cmp.CompareOperator._instances['a']
        del cmp.CompareOperator._instances['b']
        del cmp.CompareOperator._instances['c']


class TestCompareOperator(unittest.TestCase):
    def test_greater(self):
        co = cmp.CompareOperator.from_key('greater')

        self.assertEqual(co(1, 0), True)
        self.assertEqual(co(1, 1), False)
        self.assertEqual(co(0, 1), False)
        self.assertEqual(co('1', '0'), True)
        self.assertEqual(co('1', '1'), False)
        self.assertEqual(co('0', '1'), False)
        self.assertEqual(co('1', '05'), True)
        self.assertEqual(co('15', '1'), True)  # TODO it was false in mecon1
        self.assertEqual(co('05', '1'), False)
        self.assertEqual(co('2020-01-02', '2020-01-01'), True)
        self.assertEqual(co('2020-01-02', '2020-01-02'), False)
        self.assertEqual(co('2020-01-01', '2020-01-02'), False)

    def test_greater_equal(self):
        co = cmp.CompareOperator.from_key('greater_equal')

        self.assertEqual(co(1, 0), True)
        self.assertEqual(co(1, 1), True)
        self.assertEqual(co(0, 1), False)
        self.assertEqual(co('1', '0'), True)
        self.assertEqual(co('1', '1'), True)
        self.assertEqual(co('0', '1'), False)
        self.assertEqual(co('1', '05'), True)
        self.assertEqual(co('15', '1'), True)
        self.assertEqual(co('05', '1'), False)
        self.assertEqual(co('2020-01-02', '2020-01-01'), True)
        self.assertEqual(co('2020-01-02', '2020-01-02'), True)
        self.assertEqual(co('2020-01-01', '2020-01-02'), False)

    def test_equal(self):
        co = cmp.CompareOperator.from_key('equal')

        self.assertEqual(co(1, 0), False)
        self.assertEqual(co(1, 1), True)
        self.assertEqual(co(0, 1), False)
        self.assertEqual(co('1', '0'), False)
        self.assertEqual(co('1', '1'), True)
        self.assertEqual(co('0', '1'), False)
        self.assertEqual(co('1', '05'), False)
        self.assertEqual(co('15', '1'), False)  # TODO it was True in mecon1
        self.assertEqual(co('05', '1'), False)
        self.assertEqual(co('2020-01-02', '2020-01-01'), False)
        self.assertEqual(co('2020-01-02', '2020-01-02'), True)
        self.assertEqual(co('2020-01-01', '2020-01-02'), False)

    def test_less_equal(self):
        co = cmp.CompareOperator.from_key('less_equal')

        self.assertEqual(co(1, 0), False)
        self.assertEqual(co(1, 1), True)
        self.assertEqual(co(0, 1), True)
        self.assertEqual(co('1', '0'), False)
        self.assertEqual(co('1', '1'), True)
        self.assertEqual(co('0', '1'), True)
        self.assertEqual(co('1', '05'), False)
        self.assertEqual(co('15', '1'), False)  # TODO it was True in mecon1
        self.assertEqual(co('05', '1'), True)
        self.assertEqual(co('2020-01-02', '2020-01-01'), False)
        self.assertEqual(co('2020-01-02', '2020-01-02'), True)
        self.assertEqual(co('2020-01-01', '2020-01-02'), True)

    def test_less(self):
        co = cmp.CompareOperator.from_key('less')

        self.assertEqual(co(1, 0), False)
        self.assertEqual(co(1, 1), False)
        self.assertEqual(co(0, 1), True)
        self.assertEqual(co('1', '0'), False)
        self.assertEqual(co('1', '1'), False)
        self.assertEqual(co('0', '1'), True)
        self.assertEqual(co('1', '05'), False)
        self.assertEqual(co('15', '1'), False)
        self.assertEqual(co('05', '1'), True)
        self.assertEqual(co('2020-01-02', '2020-01-01'), False)
        self.assertEqual(co('2020-01-02', '2020-01-02'), False)
        self.assertEqual(co('2020-01-01', '2020-01-02'), True)

    def test_contains(self):
        co = cmp.CompareOperator.from_key('contains')

        self.assertEqual(co('abcd', 'a'), True)
        self.assertEqual(co(['a', 'b', 'c', 'd'], 'a'), True)
        self.assertEqual(co('abcd', 'x'), False)
        self.assertEqual(co(['a', 'b', 'c', 'd'], 'x'), False)

    def test_not_contains(self):
        co = cmp.CompareOperator.from_key('not_contains')

        self.assertEqual(co('abcd', 'a'), False)
        self.assertEqual(co(['a', 'b', 'c', 'd'], 'a'), False)
        self.assertEqual(co('abcd', 'x'), True)
        self.assertEqual(co(['a', 'b', 'c', 'd'], 'x'), True)

    def test_regex(self):
        co = cmp.CompareOperator.from_key('regex')

        self.assertEqual(co('[0-9]+', 'abc123xyz'), True)
        self.assertEqual(co('[0-9]+', 'abcxyz'), False)


if __name__ == '__main__':
    unittest.main()