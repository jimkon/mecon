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
    # def test__field_values(self):
    #     test_df = pd.DataFrame(data={
    #         'a': [1, 2, 3, 4],
    #         'b': [1.0, 2.0, 3.5, 4.9999],
    #         'c': [1.0, 2.0, 3.5, 4.9999],
    #         'd': ['a', 'b',  'c', 'd'],
    #         'e': ['A', 'B', 'C', 'D'],
    #         'f': [-1, 2.2, -3, 4],
    #
    #     })
    #     tag_dict = {
    #         'a': {'greater_equal': 2},
    #         'b': {'less_equal': 100},
    #         'c.int': {'less_equal': 100},
    #         'd.upper': {'less_equal': 100},
    #         'e.lower': {'less_equal': 100},
    #         'f.abs': {'less_equal': 100},
    #     }
    #     tagger = tag.DictTag('test_tag', tag_dict)
    #
    #     self.assertListEqual(tagger._field_values('a', test_df).to_list(), [1, 2, 3, 4])
    #     self.assertListEqual(tagger._field_values('b', test_df).to_list(), [1.0, 2.0, 3.5, 4.9999])
    #     self.assertListEqual(tagger._field_values('c', test_df).to_list(), [1, 2, 3, 4])
    #     self.assertListEqual(tagger._field_values('d', test_df).to_list(), ['A', 'B', 'C', 'D'])
    #     self.assertListEqual(tagger._field_values('e', test_df).to_list(), ['a', 'b',  'c', 'd'])
    #     self.assertListEqual(tagger._field_values('f', test_df).to_list(), [1, 2.2, 3, 4])
    #
    #     self.assertTrue(isinstance(tagger._field_values('a', test_df), pd.Series))
    #
    # def test__field_conds_numbers(self):
    #     test_df = pd.DataFrame(data={
    #         'a': [1, 2, 3, 4],
    #
    #     })
    #     tag_dict = {
    #         'a': {
    #             'greater': 2,
    #             'greater_equal': 2,
    #             'equals': 2,
    #             'less_equal': 2,
    #             'less': 2,
    #         },
    #         'b': {
    #
    #         }
    #     }
    #     tagger = tag.DictTag('test_tag', tag_dict)
    #
    #     a_field_conds = tagger._field_conds('a', test_df)
    #
    #     self.assertTrue(isinstance(a_field_conds[0], pd.Series))
    #     self.assertListEqual(a_field_conds[0].to_list(), [False, False, True, True])
    #     self.assertListEqual(a_field_conds[1].to_list(), [False, True, True, True])
    #     self.assertListEqual(a_field_conds[2].to_list(), [False, True, False, False])
    #     self.assertListEqual(a_field_conds[3].to_list(), [True, True, False, False])
    #     self.assertListEqual(a_field_conds[4].to_list(), [True, False, False, False])
    #
    # def test__field_conds_string(self):
    #     test_df = pd.DataFrame(data={
    #         'a': ['a', 'a in str', 'alias', 'test'],
    #
    #     })
    #     tag_dict = {
    #         'a': {
    #             'equals': 'a',
    #             'contains': 'a',
    #             'not_contains': 't',
    #             'regex': '^a...s$',
    #         },
    #     }
    #     tagger = tag.DictTag('test_tag', tag_dict)
    #
    #     a_field_conds = tagger._field_conds('a', test_df)
    #
    #     self.assertTrue(isinstance(a_field_conds[0], pd.Series))
    #     self.assertListEqual(a_field_conds[0].to_list(), [True, False, False, False])
    #     self.assertListEqual(a_field_conds[1].to_list(), [True, True, True, False])
    #     self.assertListEqual(a_field_conds[2].to_list(), [True, False, True, False])
    #     self.assertListEqual(a_field_conds[3].to_list(), [False, False, True, False])
    #
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
