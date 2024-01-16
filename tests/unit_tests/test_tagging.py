import unittest
from unittest.mock import patch, Mock, call
from datetime import datetime

import pandas as pd

from mecon.tag_tools import tagging


class ConditionTestCase(unittest.TestCase):
    def test_init_validation_success(self):
        tagging.Condition(
            field='field',
            transformation_op=lambda x: str(x),
            compare_op=lambda x, y: x == y,
            value='1'
        )
        tagging.Condition(
            field='field',
            transformation_op=None,
            compare_op=lambda x, y: x == y,
            value='1'
        )

    def test_init_field_validation_fail(self):
        with self.assertRaises(tagging.FieldIsNotStringException):
            condition = tagging.Condition(
                field=1,
                transformation_op=lambda x: str(x),
                compare_op=lambda x, y: x == y,
                value='1'
            )

    def test_init_transformation_op_validation_fail(self):
        with self.assertRaises(tagging.NotACallableException):
            condition = tagging.Condition(
                field='field',
                transformation_op='not_a_callable',
                compare_op=lambda x, y: x == y,
                value='1'
            )

        with self.assertRaises(tagging.NotACallableException):
            condition = tagging.Condition(
                field='field',
                transformation_op=str,
                compare_op='not_a_callable',
                value='1'
            )

    def test_init_compare_op_validation_fail(self):
        with self.assertRaises(tagging.FieldIsNotStringException):
            condition = tagging.Condition(
                field=1,
                transformation_op=lambda x: str(x),
                compare_op='not_a_callable',
                value='1'
            )

    def test_condition(self):
        condition = tagging.Condition(
            field='field',
            transformation_op=lambda x: str(x),
            compare_op=lambda x, y: x == y,
            value='1'
        )

        self.assertEqual(condition.compute({'field': 0}), False)
        self.assertEqual(condition.compute({'field': 1}), True)
        self.assertEqual(condition.compute({'field': '1'}), True)
        self.assertEqual(condition.compute({'field': 2}), False)

    def test_repr(self):
        condition = tagging.Condition(
            field='field',
            transformation_op=lambda x: str(x),
            compare_op=lambda x, y: x == y,
            value='1'
        )
        str(condition)  # make sure it doesn't crash


class ConjunctionTestCase(unittest.TestCase):
    def test_init_validation_success(self):
        conj = tagging.Conjunction([
            tagging.Condition(
                field='field',
                transformation_op=lambda x: str(x),
                compare_op=lambda x, y: x == y,
                value='1'
            ),
            tagging.Condition(
                field='field',
                transformation_op=lambda x: str(x),
                compare_op=lambda x, y: x == y,
                value='1'
            )
        ])
        self.assertTrue(len(conj.rules) > 0)

    def test_init_validation_fail(self):
        with self.assertRaises(tagging.NotARuleException):
            tagging.Conjunction([
                tagging.Condition(
                    field='field',
                    transformation_op=lambda x: str(x),
                    compare_op=lambda x, y: x == y,
                    value='1'
                ),
                'a'
            ])

    def test_conjunction(self):
        conjunction = tagging.Conjunction([
            tagging.Condition(
                field='int_field',
                transformation_op=lambda x: int(x),
                compare_op=lambda x, y: x > y,
                value=2
            ),
            tagging.Condition(
                field='int_field',
                transformation_op=lambda x: int(x),
                compare_op=lambda x, y: x <= y,
                value=4
            )
        ])

        self.assertEqual(conjunction.compute({'int_field': 1}), False)
        self.assertEqual(conjunction.compute({'int_field': 2}), False)
        self.assertEqual(conjunction.compute({'int_field': 3}), True)
        self.assertEqual(conjunction.compute({'int_field': 4}), True)
        self.assertEqual(conjunction.compute({'int_field': 5}), False)


class DisjunctionTestCase(unittest.TestCase):
    def test_init_validation_success(self):
        disj = tagging.Disjunction([
            tagging.Condition(
                field='field',
                transformation_op=lambda x: str(x),
                compare_op=lambda x, y: x == y,
                value='1'
            ),
            tagging.Condition(
                field='field',
                transformation_op=lambda x: str(x),
                compare_op=lambda x, y: x == y,
                value='1'
            )
        ])
        self.assertTrue(len(disj.rules) > 0)

    def test_init_validation_fail(self):
        with self.assertRaises(tagging.NotARuleException):
            tagging.Disjunction([
                tagging.Condition(
                    field='field',
                    transformation_op=lambda x: str(x),
                    compare_op=lambda x, y: x == y,
                    value='1'
                ),
                'a'
            ])

    def test_conjunction(self):
        conjunction = tagging.Disjunction([
            tagging.Condition(
                field='int_field',
                transformation_op=lambda x: int(x),
                compare_op=lambda x, y: x < y,
                value=2
            ),
            tagging.Condition(
                field='int_field',
                transformation_op=lambda x: int(x),
                compare_op=lambda x, y: x >= y,
                value=4
            )
        ])

        self.assertEqual(conjunction.compute({'int_field': 1}), True)
        self.assertEqual(conjunction.compute({'int_field': 2}), False)
        self.assertEqual(conjunction.compute({'int_field': 3}), False)
        self.assertEqual(conjunction.compute({'int_field': 4}), True)
        self.assertEqual(conjunction.compute({'int_field': 5}), True)

    def test_from_json(self):
        rule_dict = {
            "field1.str": {"greater": "1"}
        }
        self.assertIsInstance(tagging.Disjunction.from_json(rule_dict), tagging.Disjunction)

        with self.assertRaises(ValueError):
            tagging.Disjunction.from_json('invalid_input')

    def test_from_json_string(self):
        rule_dict_str = '{"field1.str": {"greater": "1"}}'
        self.assertIsInstance(tagging.Disjunction.from_json_string(rule_dict_str), tagging.Disjunction)

        with self.assertRaises(ValueError):
            tagging.Disjunction.from_json('invalid_input')

    def test_from_dataframe(self):
        example_df = pd.DataFrame(
            {'a': [1, 2], 'b': ['a', 'b'], "c": [datetime(2020, 1, 1, 12, 30, 45), datetime(2022, 2, 2, 2, 22, 22)]})
        disj = tagging.Disjunction.from_dataframe(example_df)

        result_json = [{
            "a.str": {"equal": "1"},
            "b.str": {"equal": "a"},
            "c.str": {"equal": "2020-01-01 12:30:45"}
        },
            {
                "a.str": {"equal": "2"},
                "b.str": {"equal": "b"},
                "c.str": {"equal": "2022-02-02 02:22:22"}
            }
        ]
        self.assertEqual(disj.to_json(), result_json)

    def test_from_dataframe_exclude_cols(self):
        example_df = pd.DataFrame(
            {'a': [1, 2], 'b': ['a', 'b'], "c": [datetime(2020, 1, 1, 12, 30, 45), datetime(2022, 2, 2, 2, 22, 22)]})
        disj = tagging.Disjunction.from_dataframe(example_df, exclude_cols=['a', 'c'])

        result_json = [{
            "b.str": {"equal": "a"}
        },
            {
                "b.str": {"equal": "b"}
            }
        ]
        self.assertEqual(disj.to_json(), result_json)


class TestTag(unittest.TestCase):
    def test_from_json_string(self):
        _json_str = """{"field1.str": {"greater": '1'}}"""
        tagging.Tag.from_json_string('test_name', _json_str)


class TestTagger(unittest.TestCase):
    def test_get_index_for_rule(self):
        class TestRule:
            def compute(self, x):
                return x['field'] == 2

        rule = TestRule()
        df = pd.DataFrame({'field': [0, 1, 2, 3, 4]})

        tagger = tagging.Tagger()

        rows_to_tag = tagger.get_index_for_rule(df, rule)
        self.assertListEqual(rows_to_tag.to_list(), [False, False, True, False, False])

    def test_filter_df_with_rule(self):
        class TestRule:
            def compute(self, x):
                return x['a'] == 2 or x['b'] == '4'

        rule = TestRule()
        df = pd.DataFrame({'a': [0, 1, 2, 3, 4], 'b': ['0', '1', '2', '3', '4']})

        tagger = tagging.Tagger()

        res_df = tagger.filter_df_with_rule(df, rule)
        expected_df = pd.DataFrame({'a': [2, 4], 'b': ['2', '4']}).reset_index(drop=True)
        pd.testing.assert_frame_equal(res_df.reset_index(drop=True), expected_df)

    def test_filter_df_with_negated_rule(self):
        class TestRule:
            def compute(self, x):
                return x['a'] == 2 or x['b'] == '4'

        rule = TestRule()
        df = pd.DataFrame({'a': [0, 1, 2, 3, 4], 'b': ['0', '1', '2', '3', '4']})

        tagger = tagging.Tagger()

        res_df = tagger.filter_df_with_negated_rule(df, rule)
        expected_df = pd.DataFrame({'a': [0, 1, 3], 'b': ['0', '1', '3']}).reset_index(drop=True)
        pd.testing.assert_frame_equal(res_df.reset_index(drop=True), expected_df)

    def test_already_tagged_rows(self):
        tag_name = 'test_tag'
        df = pd.DataFrame({
            'tags': ['',
                     'test_tag',
                     'another_tag',
                     'another_tag,test_tag']
        })

        tagger = tagging.Tagger()

        already_tagged_rows = tagger._already_tagged_rows(tag_name, df)
        self.assertListEqual(already_tagged_rows.to_list(), [False, True, False, True])

    def test_remove_tag(self):
        tag_name = 'test_tag'
        df = pd.DataFrame({
            'tags': [
                '',
                'test_tag',
                'another_tag',
                'test_tag,another_tag',
                'another_tag,test_tag',
                'test_tag,another_tag,another_tag',
                'another_tag,test_tag,another_tag',
                'another_tag,another_tag,test_tag']
        })

        tagger = tagging.Tagger()

        tagger.remove_tag(tag_name, df)
        pd.testing.assert_series_equal(
            df['tags'],
            pd.Series(['',
                       '',
                       'another_tag',
                       'another_tag',
                       'another_tag',
                       'another_tag,another_tag',
                       'another_tag,another_tag',
                       'another_tag,another_tag',
                       ], name='tags'))

    def test_add_tag(self):
        tag_name = 'test_tag'
        df = pd.DataFrame({
            'tags': [
                'not_to_be_tagged',
                '',
                'not_to_be_tagged',
                'test_tag',
                'another_tag',
                'test_tag,another_tag',
                'another_tag,test_tag',
                'test_tag,another_tag,another_tag',
                'another_tag,test_tag,another_tag',
                'another_tag,another_tag,test_tag',
            ]
        })

        tagger = tagging.Tagger()
        df1 = df.copy()
        tagger.add_tag(tag_name, df1, [False, True, False, True, True, True, True, True, True, True])
        pd.testing.assert_series_equal(
            df1['tags'],
            pd.Series([
                'not_to_be_tagged',
                'test_tag',
                'not_to_be_tagged',
                'test_tag',
                'another_tag,test_tag',
                'test_tag,another_tag',
                'another_tag,test_tag',
                'test_tag,another_tag,another_tag',
                'another_tag,test_tag,another_tag',
                'another_tag,another_tag,test_tag'
            ], name='tags'))

    def test_tag_without_removing_old_tags(self):
        class TestRule:
            def compute(self, x):
                return x['field'] == 2 or x['field'] == 3

        rule = TestRule()
        tag = tagging.Tag('test_tag', rule)
        df = pd.DataFrame({'field': [0, 1, 2, 3, 4],
                           'tags': ['', 'test_tag', 'another_tag', 'test_tag', 'another_tag']})

        tagger = tagging.Tagger()
        tagger.tag(tag, df)

        expected_df = pd.DataFrame({'field': [0, 1, 2, 3, 4],
                                    'tags': ['', 'test_tag', 'another_tag,test_tag', 'test_tag', 'another_tag']})
        pd.testing.assert_frame_equal(df, expected_df)

    def test_tag_with_removing_old_tags(self):
        class TestRule:
            def compute(self, x):
                return x['field'] == 2 or x['field'] == 3

        rule = TestRule()
        tag = tagging.Tag('test_tag', rule)
        df = pd.DataFrame({'field': [0, 1, 2, 3, 4],
                           'tags': ['', 'test_tag', 'another_tag', 'test_tag', 'another_tag']})

        tagger = tagging.Tagger()
        tagger.tag(tag, df, remove_old_tags=True)

        expected_df = pd.DataFrame({'field': [0, 1, 2, 3, 4],
                                    'tags': ['', '', 'another_tag,test_tag', 'test_tag', 'another_tag']})
        pd.testing.assert_frame_equal(df, expected_df)


class TestTagMatchCondition(unittest.TestCase):
    def test_match(self):
        match_condition = tagging.TagMatchCondition('test_tag')

        self.assertEqual(match_condition.compute({'tags': 'test_tag'}), True)
        self.assertEqual(match_condition.compute({'tags': ''}), False)
        self.assertEqual(match_condition.compute({'tags': 'another_tag'}), False)
        self.assertEqual(match_condition.compute({'tags': 'another_tag,test_tag'}), True)
        self.assertEqual(match_condition.compute({'tags': 'test_tag,another_tag'}), True)
        self.assertEqual(match_condition.compute({'tags': 'another_tag,test_tag'}), True)
        self.assertEqual(match_condition.compute({'tags': 'test_tag,another_tag,another_tag'}), True)
        self.assertEqual(match_condition.compute({'tags': 'another_tag,test_tag,another_tag'}), True)
        self.assertEqual(match_condition.compute({'tags': 'another_tag,another_tag,test_tag'}), True)
        self.assertEqual(match_condition.compute({'tags': 'not_test_tag'}), False)


class RuleObserverFunctionalityTestCase(unittest.TestCase):
    def test_observers(self):
        condition = tagging.Condition(
            field='field',
            transformation_op=None,
            compare_op=lambda x, y: x > y,
            value=1
        )
        observer1, observer2 = Mock(), Mock()

        condition.add_observers(observer1)
        condition.add_observers(observer2)

        with patch.object(observer1, 'observe'):
            with patch.object(observer2, 'observe'):
                condition.compute({'field': 0})

                observer1.assert_has_calls([call(condition, {'field': 0}, False)])
                observer2.assert_has_calls([call(condition, {'field': 0}, False)])


class HardCodedRuleTestCase(unittest.TestCase):
    def test_condition(self):
        class ExampleHardCodedRule(tagging.HardCodedRule):
            def calculate_matching_ids(self, df_in):
                return {'id_1', 'id_3'}
        cond = ExampleHardCodedRule(None)

        example_df = pd.DataFrame({'id': ['id_1', 'id_2', 'id_3', 'id_4']})
        cond_values = example_df.apply(cond.compute, axis=1).to_list()
        self.assertListEqual(cond_values, [True, False, True, False])


if __name__ == '__main__':
    unittest.main()
