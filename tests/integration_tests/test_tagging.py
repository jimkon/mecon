import unittest

import pandas as pd

from mecon import comparisons
from mecon import transformations
from mecon import tagging


class TestCondition(unittest.TestCase):
    def test_init_condition_from_string_values(self):
        condition = tagging.Condition.from_string_values(
            field='field',
            transformation_op_key='str',
            compare_op_key='greater',
            value='1'
        )
        self.assertEqual(condition._transformation_op, transformations.STR)
        self.assertEqual(condition._compare_op, comparisons.GREATER)

        self.assertEqual(condition.compute({'field': 0}), False)
        self.assertEqual(condition.compute({'field': '1'}), False)
        self.assertEqual(condition.compute({'field': 2}), True)

    def test_to_dict(self):
        dict1 = tagging.Condition.from_string_values(
            field='field',
            transformation_op_key=None,
            compare_op_key='greater',
            value='1'
        ).to_dict()
        self.assertDictEqual(dict1, {
            "field": {"greater": "1"}
        })

        dict1 = tagging.Condition.from_string_values(
            field='field',
            transformation_op_key="str",
            compare_op_key='greater',
            value='1'
        ).to_dict()
        self.assertDictEqual(dict1, {
            "field.str": {"greater": "1"}
        })

        with self.assertRaises(NotImplementedError):
            tagging.Condition(
                field='field',
                transformation_op=lambda x: x,
                compare_op=lambda x: x,
                value='1'
            ).to_dict()


class TestConjunction(unittest.TestCase):
    def test_init_conjunction_from_dict(self):
        test_dict = {
            'col1': {
                'greater': [1, 10],
                'less': 100,
            },
            'col2.str': {
                'equal': '20',
            }
        }

        conjunction = tagging.Conjunction.from_dict(test_dict)
        rules = conjunction._rules

        self.assertEqual(len(rules), 4)

        self.assertEqual(rules[0]._field, 'col1')
        self.assertEqual(rules[0]._transformation_op, transformations.NO_TRANSFORMATION)
        self.assertEqual(rules[0]._compare_op, comparisons.GREATER)
        self.assertEqual(rules[0]._value, 1)

        self.assertEqual(rules[1]._field, 'col1')
        self.assertEqual(rules[1]._transformation_op, transformations.NO_TRANSFORMATION)
        self.assertEqual(rules[1]._compare_op, comparisons.GREATER)
        self.assertEqual(rules[1]._value, 10)

        self.assertEqual(rules[2]._field, 'col1')
        self.assertEqual(rules[2]._transformation_op, transformations.NO_TRANSFORMATION)
        self.assertEqual(rules[2]._compare_op, comparisons.LESS)
        self.assertEqual(rules[2]._value, 100)

        self.assertEqual(rules[3]._field, 'col2')
        self.assertEqual(rules[3]._transformation_op, transformations.STR)
        self.assertEqual(rules[3]._compare_op, comparisons.EQUAL)
        self.assertEqual(rules[3]._value, '20')

        self.assertEqual(conjunction.compute({'col1': 0, 'col2': 20}), False)
        self.assertEqual(conjunction.compute({'col1': 1, 'col2': 20}), False)
        self.assertEqual(conjunction.compute({'col1': 2, 'col2': 20}), False)
        self.assertEqual(conjunction.compute({'col1': 10, 'col2': 20}), False)
        self.assertEqual(conjunction.compute({'col1': 11, 'col2': 20}), True)
        self.assertEqual(conjunction.compute({'col1': 90, 'col2': 20}), True)
        self.assertEqual(conjunction.compute({'col1': 100, 'col2': 20}), False)
        self.assertEqual(conjunction.compute({'col1': 90, 'col2': 19}), False)

    def test_to_dict(self):
        cond1 = tagging.Condition.from_string_values(
            field='field1',
            transformation_op_key="str",
            compare_op_key='greater',
            value='1'
        )
        cond2 = tagging.Condition.from_string_values(
            field='field2',
            transformation_op_key="str",
            compare_op_key='greater',
            value='2'
        )
        cond3 = tagging.Condition.from_string_values(
            field='field2',
            transformation_op_key="str",
            compare_op_key='greater',
            value='3'
        )
        cond4 = tagging.Condition.from_string_values(
            field='field2',
            transformation_op_key=None,
            compare_op_key='less',
            value='4'
        )
        conjunction = tagging.Conjunction([cond1, cond2, cond3, cond4])
        result_dict = conjunction.to_dict()
        self.assertDictEqual(result_dict,
                             {
                                 "field1.str": {"greater": "1"},
                                 "field2.str": {"greater": ["2", "3"]},
                                 "field2": {"less": "4"},
                             })


class TestDisjunction(unittest.TestCase):
    def test_init_disjunction_from_json(self):
        test_json = [
            {
                'col1': {
                    'greater': 1,
                }
            },
            {
                'col1': {
                    'less': -1,
                }
            }

        ]

        disjunction = tagging.Disjunction.from_json(test_json)
        rules = disjunction._rules

        self.assertEqual(len(rules), 2)
        conj1, conj2 = disjunction._rules[0]._rules, disjunction._rules[1]._rules
        self.assertEqual(len(conj1), 1)
        self.assertEqual(len(conj1), 1)

        self.assertEqual(conj1[0]._field, 'col1')
        self.assertEqual(conj1[0]._transformation_op, transformations.NO_TRANSFORMATION)
        self.assertEqual(conj1[0]._compare_op, comparisons.GREATER)
        self.assertEqual(conj1[0]._value, 1)

        self.assertEqual(conj2[0]._field, 'col1')
        self.assertEqual(conj2[0]._transformation_op, transformations.NO_TRANSFORMATION)
        self.assertEqual(conj2[0]._compare_op, comparisons.LESS)
        self.assertEqual(conj2[0]._value, -1)

        self.assertEqual(disjunction.compute({'col1': -2}), True)
        self.assertEqual(disjunction.compute({'col1': -1}), False)
        self.assertEqual(disjunction.compute({'col1': 0}), False)
        self.assertEqual(disjunction.compute({'col1': 1}), False)
        self.assertEqual(disjunction.compute({'col1': 2}), True)

    def test_to_dict(self):
        cond1 = tagging.Condition.from_string_values(
            field='field1',
            transformation_op_key="str",
            compare_op_key='greater',
            value='1'
        )
        cond2 = tagging.Condition.from_string_values(
            field='field2',
            transformation_op_key="str",
            compare_op_key='greater',
            value='2'
        )
        cond3 = tagging.Condition.from_string_values(
            field='field2',
            transformation_op_key="str",
            compare_op_key='greater',
            value='3'
        )
        cond4 = tagging.Condition.from_string_values(
            field='field2',
            transformation_op_key=None,
            compare_op_key='less',
            value='4'
        )
        disjunction = tagging.Disjunction([
            cond1,
            tagging.Conjunction([cond2, cond3]),
            cond4
        ])
        result_dict = disjunction.to_json()
        self.assertListEqual(result_dict, [
            {
                "field1.str": {"greater": "1"}
            },
            {
                "field2.str": {"greater": ["2", "3"]}
            },
            {
                "field2": {"less": "4"},
            }
        ])

    def test_append(self):
        cond1 = tagging.Condition.from_string_values(
            field='field1',
            transformation_op_key="str",
            compare_op_key='greater',
            value='1'
        )
        cond2 = tagging.Condition.from_string_values(
            field='field2',
            transformation_op_key=None,
            compare_op_key='less',
            value='4'
        )
        disjunction = tagging.Disjunction([
            cond1
        ]).append(cond2)
        result_dict = disjunction.to_json()
        self.assertListEqual(result_dict, [
            {
                "field2": {"less": "4"},
            },
            {
                "field1.str": {"greater": "1"}
            }
        ])


class TestTagging(unittest.TestCase):
    def test_tagger_in_dfs(self):
        test_json = [
            {
                'col1': {
                    'greater': 1,
                }
            },
            {
                'col1': {
                    'less': -1,
                }
            }
        ]

        df = pd.DataFrame({
            'col1': [-2, -1, 0, 1, 2],
            'tags': ['', '', '', '', '']
        })

        tag = tagging.Tag.from_json('test_tag', test_json)
        tagger = tagging.Tagger()

        tagger.tag(tag, df)

        expected_df = pd.DataFrame({
            'col1': [-2, -1, 0, 1, 2],
            'tags': ['test_tag', '', '', '', 'test_tag']
        })
        pd.testing.assert_frame_equal(df, expected_df)


if __name__ == '__main__':
    unittest.main()

