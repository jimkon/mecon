import unittest
from unittest.mock import patch, call

import pandas as pd
from mecon.tagging import dict_tag as tag


class TestRule(unittest.TestCase):
    def test_preprocessing_function(self):
        self.assertEqual(tag.Rule('a', str, lambda x, y: len(x) > y, 2).preprocessing_function, str)
        self.assertEqual(tag.Rule('a', 'str', lambda x, y: len(x) > y, 2).preprocessing_function, str)

    def test_apply(self):
        test_df = pd.DataFrame(data={
            'a': [1, 12, 123, 1234],
        })

        rule = tag.Rule('a', str, lambda x, y: len(x) > y, 2)
        res = test_df.apply(rule, axis=1)
        self.assertEqual(res.to_list(), [False, False, True, True])

        rule = tag.Rule('a', 'str', lambda x, y: len(x) > y, 2)
        res = test_df.apply(rule, axis=1)
        self.assertEqual(res.to_list(), [False, False, True, True])

        rule = tag.Rule('a', None, 'greater', 12)
        res = test_df.apply(rule, axis=1)
        self.assertEqual(res.to_list(), [False, False, True, True])


class TestRuleDict(unittest.TestCase):
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
        rules = tag.analyse_rule_dict(tag_dict)

        # rule 1
        rule = rules[0]
        self.assertTrue(rule._column == 'col_value1')
        self.assertTrue(rule._preprocessing_function == None)
        self.assertTrue(rule._condition_function == 'cond_f1')
        self.assertTrue(rule._condition_value == 'cond_value1')

        # rule 2
        rule = rules[1]
        self.assertTrue(rule._column == 'col_value1')
        self.assertTrue(rule._preprocessing_function == None)
        self.assertTrue(rule._condition_function == 'cond_f1')
        self.assertTrue(rule._condition_value == 'cond_value2')

        # rule 3
        rule = rules[2]
        self.assertTrue(rule._column == 'col_value1')
        self.assertTrue(rule._preprocessing_function == None)
        self.assertTrue(rule._condition_function == 'cond_f2')
        self.assertTrue(rule._condition_value == 'cond_value3')

        # rule 4
        rule = rules[3]
        self.assertTrue(rule._column == 'col_value2')
        self.assertTrue(rule._preprocessing_function == 'preproc_f1')
        self.assertTrue(rule._condition_function == 'cond_f3')
        self.assertTrue(rule._condition_value == 'cond_value4')


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

        tagger = tag.DictTag('test_tag', tag_dict)
        tagger.tag(test_df)

        self.assertEqual(set(test_df.columns), {'a', 'b', 'c', 'tags'})
        self.assertListEqual(test_df['tags'].to_list(), [[], ['test_tag'], ['test_tag'], []])
