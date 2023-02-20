import unittest

import numpy as np
import pandas as pd

from mecon.tagging import json_tags as tag


class TestSelectCondition(unittest.TestCase):
    def test_preprocessing_function(self):
        self.assertEqual(tag.SelectCondition('a', str, lambda x, y: len(x) > y, 2).preprocessing_function, str)
        self.assertEqual(tag.SelectCondition('a', 'str', lambda x, y: len(x) > y, 2).preprocessing_function, str)

    def test_apply(self):
        test_df = pd.DataFrame(data={
            'a': [1, 12, 123, 1234],
        })

        rule = tag.SelectCondition('a', str, lambda x, y: len(x) > y, 2)
        res = rule(test_df)
        self.assertEqual(res, [False, False, True, True])

        rule = tag.SelectCondition('a', 'str', lambda x, y: len(x) > y, 2)
        res = rule(test_df)
        self.assertEqual(res, [False, False, True, True])

        rule = tag.SelectCondition('a', None, 'greater', 12)
        res = rule(test_df)
        self.assertEqual(res, [False, False, True, True])


class TestRuleDict(unittest.TestCase):
    def test_calc_each_condition(self):
        test_df = pd.DataFrame(data={
            'a': [1, 12, 123, 1234],
        })

        rule1 = tag.SelectCondition('a', None, lambda x, y: x > y, 10)
        rule2 = tag.SelectCondition('a', None, lambda x, y: x > y, 100)
        rule3 = tag.SelectCondition('a', None, lambda x, y: x > y, 1000)

        cond_values = tag.RuleDict([rule1, rule2, rule3]).calc_each_condition(test_df)
        expected_cond_values = [[False, True, True, True], [False, False, True, True], [False, False, False, True]]
        self.assertTrue(np.array_equal(cond_values, expected_cond_values))

    def test_calculate(self):
        test_df = pd.DataFrame(data={
            'a': [1, 12, 123, 1234],
        })

        rule1 = tag.SelectCondition('a', None, lambda x, y: x > y, 10)
        rule2 = tag.SelectCondition('a', None, lambda x, y: x > y, 100)
        rule3 = tag.SelectCondition('a', None, lambda x, y: x > y, 1000)

        cond_values = tag.RuleDict([rule1, rule2, rule3]).calculate(test_df)
        expected_cond_values = [False, False, False, True]
        self.assertTrue(np.array_equal(cond_values, expected_cond_values))

    def test_analyse_rule_dict(self):
        tag_dict = {
            'col_value1': {
                'cond_f1': ['cond_value1', 'cond_value2'],
                'cond_f2': 'cond_value3',
            },
            'col_value2.preproc_f1': {
                'cond_f3': 'cond_value4',
            }
        }
        rule_dict = tag.RuleDict.from_dict(tag_dict)
        conds = rule_dict.conditions
        # rule 1
        rule = conds[0]
        self.assertTrue(rule._column == 'col_value1')
        self.assertTrue(rule._preprocessing_function == None)
        self.assertTrue(rule._condition_function == 'cond_f1')
        self.assertTrue(rule._condition_value == 'cond_value1')

        # rule 2
        cond = conds[1]
        self.assertTrue(cond._column == 'col_value1')
        self.assertTrue(cond._preprocessing_function == None)
        self.assertTrue(cond._condition_function == 'cond_f1')
        self.assertTrue(cond._condition_value == 'cond_value2')

        # rule 3
        cond = conds[2]
        self.assertTrue(cond._column == 'col_value1')
        self.assertTrue(cond._preprocessing_function == None)
        self.assertTrue(cond._condition_function == 'cond_f2')
        self.assertTrue(cond._condition_value == 'cond_value3')

        # rule 4
        cond = conds[3]
        self.assertTrue(cond._column == 'col_value2')
        self.assertTrue(cond._preprocessing_function == 'preproc_f1')
        self.assertTrue(cond._condition_function == 'cond_f3')
        self.assertTrue(cond._condition_value == 'cond_value4')


class TestSelectQuery(unittest.TestCase):
    def test_calculate(self):
        test_df = pd.DataFrame(data={'a': [1, 2, 3, 4],
                                     'b': [1, 10, 100, 1000],
                                     'c': ['aa', 'ab', 'ac', 'ad']})

        test_json = [{
                'a': {'greater_equal': 2},
                'b': {'greater_equal': 100},
                'c': {'contains': ['a', 'd']}
            },
            {
                'a': {'less_equal': 2},
                'b': {'less_equal': 10},
                'c': {'contains': ['a', 'b']}
            }
        ]

        cond_values = tag.SelectQuery(test_json).calculate(test_df)
        expected_cond_values = [False, True, False, True]
        self.assertTrue(np.array_equal(cond_values, expected_cond_values))


class TestDictTag(unittest.TestCase):
    def test__calc_condition(self):
        test_df = pd.DataFrame(data={'a': [1, 2, 3, 4],
                                     'b': [1, 10, 100, 1000],
                                     'c': ['aa', 'ab', 'ac', 'ad'],
                                     'tags': [[], [], [], []]})

        tag_dict = [{
            'a': {'greater_equal': 2},
            'b': {'less_equal': 100},
            'c': {'contains': ['a', 'b']}
            },
            {
                'a': {'greater_equal': 3},
                'b': {'less_equal': 1000},
                'c': {'contains': ['a', 'c']}
            }
        ]

        tagger = tag.JsonTag('test_tag', tag_dict)
        tagger.tag(test_df)

        self.assertEqual(set(test_df.columns), {'a', 'b', 'c', 'tags'})
        self.assertListEqual(test_df['tags'].to_list(), [[], ['test_tag'], ['test_tag'], []])
