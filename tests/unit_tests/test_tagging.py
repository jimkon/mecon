import unittest

import pandas as pd

from mecon import tagging


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


class ConjunctionTestCase(unittest.TestCase):
    def test_init_validation_success(self):
        tagging.Conjunction([
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
        self.assertTrue(True)

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
        tagging.Disjunction([
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
        self.assertTrue(True)

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



if __name__ == '__main__':
    unittest.main()
