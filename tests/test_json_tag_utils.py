import unittest
from datetime import date


import mecon.tagging.json_tag_utils as utils


class TestPreprocessingFunctions(unittest.TestCase):
    def test_lower(self):
        self.assertEqual(utils.field_processing_functions_dict['lower']("AaAa"), 'aaaa')

    def test_upper(self):
        self.assertEqual(utils.field_processing_functions_dict['upper']("AaAa"), 'AAAA')

    def test_dayofweek(self):
        self.assertEqual(utils.field_processing_functions_dict['dayofweek'](date(2023, 1, 9)), 'Monday')
        self.assertEqual(utils.field_processing_functions_dict['dayofweek'](date(2023, 1, 10)), 'Tuesday')
        self.assertEqual(utils.field_processing_functions_dict['dayofweek'](date(2023, 1, 11)), 'Wednesday')
        self.assertEqual(utils.field_processing_functions_dict['dayofweek'](date(2023, 1, 12)), 'Thursday')
        self.assertEqual(utils.field_processing_functions_dict['dayofweek'](date(2023, 1, 13)), 'Friday')
        self.assertEqual(utils.field_processing_functions_dict['dayofweek'](date(2023, 1, 14)), 'Saturday')
        self.assertEqual(utils.field_processing_functions_dict['dayofweek'](date(2023, 1, 15)), 'Sunday')


class TestMatchFunctions(unittest.TestCase):
    def test_greater(self):
        self.assertEqual(utils.match_funcs_dict['greater'](1, 0), True)
        self.assertEqual(utils.match_funcs_dict['greater'](1, 1), False)
        self.assertEqual(utils.match_funcs_dict['greater'](0, 1), False)
        self.assertEqual(utils.match_funcs_dict['greater']('1', '0'), True)
        self.assertEqual(utils.match_funcs_dict['greater']('1', '1'), False)
        self.assertEqual(utils.match_funcs_dict['greater']('0', '1'), False)
        self.assertEqual(utils.match_funcs_dict['greater']('1', '05'), True)
        self.assertEqual(utils.match_funcs_dict['greater']('15', '1'), False)
        self.assertEqual(utils.match_funcs_dict['greater']('05', '1'), False)
        self.assertEqual(utils.match_funcs_dict['greater']('2020-01-02', '2020-01-01'), True)
        self.assertEqual(utils.match_funcs_dict['greater']('2020-01-02', '2020-01-02'), False)
        self.assertEqual(utils.match_funcs_dict['greater']('2020-01-01', '2020-01-02'), False)

    def test_greater_equal(self):
        self.assertEqual(utils.match_funcs_dict['greater_equal'](1, 0), True)
        self.assertEqual(utils.match_funcs_dict['greater_equal'](1, 1), True)
        self.assertEqual(utils.match_funcs_dict['greater_equal'](0, 1), False)
        self.assertEqual(utils.match_funcs_dict['greater_equal']('1', '0'), True)
        self.assertEqual(utils.match_funcs_dict['greater_equal']('1', '1'), True)
        self.assertEqual(utils.match_funcs_dict['greater_equal']('0', '1'), False)
        self.assertEqual(utils.match_funcs_dict['greater_equal']('1', '05'), True)
        self.assertEqual(utils.match_funcs_dict['greater_equal']('15', '1'), True)
        self.assertEqual(utils.match_funcs_dict['greater_equal']('05', '1'), False)
        self.assertEqual(utils.match_funcs_dict['greater_equal']('2020-01-02', '2020-01-01'), True)
        self.assertEqual(utils.match_funcs_dict['greater_equal']('2020-01-02', '2020-01-02'), True)
        self.assertEqual(utils.match_funcs_dict['greater_equal']('2020-01-01', '2020-01-02'), False)

    def test_equals(self):
        self.assertEqual(utils.match_funcs_dict['equals'](1, 0), False)
        self.assertEqual(utils.match_funcs_dict['equals'](1, 1), True)
        self.assertEqual(utils.match_funcs_dict['equals'](0, 1), False)
        self.assertEqual(utils.match_funcs_dict['equals']('1', '0'), False)
        self.assertEqual(utils.match_funcs_dict['equals']('1', '1'), True)
        self.assertEqual(utils.match_funcs_dict['equals']('0', '1'), False)
        self.assertEqual(utils.match_funcs_dict['equals']('1', '05'), False)
        self.assertEqual(utils.match_funcs_dict['equals']('15', '1'), True)
        self.assertEqual(utils.match_funcs_dict['equals']('05', '1'), False)
        self.assertEqual(utils.match_funcs_dict['equals']('2020-01-02', '2020-01-01'), False)
        self.assertEqual(utils.match_funcs_dict['equals']('2020-01-02', '2020-01-02'), True)
        self.assertEqual(utils.match_funcs_dict['equals']('2020-01-01', '2020-01-02'), False)

    def test_less_equal(self):
        self.assertEqual(utils.match_funcs_dict['less_equal'](1, 0), False)
        self.assertEqual(utils.match_funcs_dict['less_equal'](1, 1), True)
        self.assertEqual(utils.match_funcs_dict['less_equal'](0, 1), True)
        self.assertEqual(utils.match_funcs_dict['less_equal']('1', '0'), False)
        self.assertEqual(utils.match_funcs_dict['less_equal']('1', '1'), True)
        self.assertEqual(utils.match_funcs_dict['less_equal']('0', '1'), True)
        self.assertEqual(utils.match_funcs_dict['less_equal']('1', '05'), False)
        self.assertEqual(utils.match_funcs_dict['less_equal']('15', '1'), True)
        self.assertEqual(utils.match_funcs_dict['less_equal']('05', '1'), True)
        self.assertEqual(utils.match_funcs_dict['less_equal']('2020-01-02', '2020-01-01'), False)
        self.assertEqual(utils.match_funcs_dict['less_equal']('2020-01-02', '2020-01-02'), True)
        self.assertEqual(utils.match_funcs_dict['less_equal']('2020-01-01', '2020-01-02'), True)

    def test_less(self):
        self.assertEqual(utils.match_funcs_dict['less'](1, 0), False)
        self.assertEqual(utils.match_funcs_dict['less'](1, 1), False)
        self.assertEqual(utils.match_funcs_dict['less'](0, 1), True)
        self.assertEqual(utils.match_funcs_dict['less']('1', '0'), False)
        self.assertEqual(utils.match_funcs_dict['less']('1', '1'), False)
        self.assertEqual(utils.match_funcs_dict['less']('0', '1'), True)
        self.assertEqual(utils.match_funcs_dict['less']('1', '05'), False)
        self.assertEqual(utils.match_funcs_dict['less']('15', '1'), False)
        self.assertEqual(utils.match_funcs_dict['less']('05', '1'), True)
        self.assertEqual(utils.match_funcs_dict['less']('2020-01-02', '2020-01-01'), False)
        self.assertEqual(utils.match_funcs_dict['less']('2020-01-02', '2020-01-02'), False)
        self.assertEqual(utils.match_funcs_dict['less']('2020-01-01', '2020-01-02'), True)

    def test_contains(self):
        self.assertEqual(utils.match_funcs_dict['contains']('abcd', 'a'), True)
        self.assertEqual(utils.match_funcs_dict['contains'](['a', 'b', 'c', 'd'], 'a'), True)
        self.assertEqual(utils.match_funcs_dict['contains']('abcd', 'x'), False)
        self.assertEqual(utils.match_funcs_dict['contains'](['a', 'b', 'c', 'd'], 'x'), False)

    def test_not_contains(self):
        self.assertEqual(utils.match_funcs_dict['not_contains']('abcd', 'a'), False)
        self.assertEqual(utils.match_funcs_dict['not_contains'](['a', 'b', 'c', 'd'], 'a'), False)
        self.assertEqual(utils.match_funcs_dict['not_contains']('abcd', 'x'), True)
        self.assertEqual(utils.match_funcs_dict['not_contains'](['a', 'b', 'c', 'd'], 'x'), True)

    def test_regex(self):
        self.assertEqual(utils.match_funcs_dict['regex']('[0-9]+', 'abc123xyz'), True)
        self.assertEqual(utils.match_funcs_dict['regex']('[0-9]+', 'abcxyz'), False)


if __name__ == "__main__":
    unittest.main()
