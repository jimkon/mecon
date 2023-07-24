import abc
import json

import pandas as pd

from mecon2 import comparisons
from mecon2 import transformations


class FieldIsNotStringException(Exception):
    pass


class NotACallableException(Exception):
    pass


class NotAnRuleException(Exception):
    pass


class AbstractRule(abc.ABC):
    @abc.abstractmethod
    def compute(self, element):
        pass


class Condition(AbstractRule):
    def __init__(self, field, transformation_op, compare_op, value):
        if not isinstance(field, str):
            raise FieldIsNotStringException
        self._field = field

        if transformation_op is not None and not hasattr(transformation_op, '__call__'):
            raise NotACallableException('Transformation operator has to be a callable object.')
        self._transformation_op = transformation_op if transformation_op is not None else lambda x: x

        if not hasattr(compare_op, '__call__'):
            raise NotACallableException('Compare operator has to be a callable object.')
        self._compare_op = compare_op

        self._value = value

    def compute(self, element):
        return self._compare_op(self._transformation_op(element[self._field]), self._value)

    @staticmethod
    def from_string_values(field, transformation_op_key, compare_op_key, value):
        transformation_op = transformations.TransformationFunction.from_key(transformation_op_key if transformation_op_key else 'none')
        compare_op = comparisons.CompareOperator.from_key(compare_op_key)
        return Condition(field, transformation_op, compare_op, value)

    def __repr__(self):
        return f"{self._transformation_op}(x[{self._field}]) {self._compare_op} {self._value}"


class Conjunction(AbstractRule):
    def __init__(self, rule_list):
        for rule in rule_list:
            if not issubclass(rule.__class__, AbstractRule):
                raise NotAnRuleException
        self._rules = rule_list

    def compute(self, element):
        return all([rule.compute(element) for rule in self._rules])

    @staticmethod
    def from_dict(_dict):
        rules_list = []
        for col_name_full, col_dict in _dict.items():
            field = col_name_full.split('.')[0]
            transformation_op = col_name_full.split('.')[1] if len(col_name_full.split('.')) > 1 else None
            for compare_op, compare_value_list in _dict[col_name_full].items():
                if not isinstance(compare_value_list, list):
                    compare_value_list = [compare_value_list]

                for compare_value in compare_value_list:
                    rules_list.append(Condition.from_string_values(field, transformation_op, compare_op, compare_value))
        return Conjunction(rules_list)


class Disjunction(AbstractRule):
    def __init__(self, rule_list):
        for rule in rule_list:
            if not issubclass(rule.__class__, AbstractRule):
                raise NotAnRuleException
        self._rules = rule_list

    def compute(self, element):
        return any([rule.compute(element) for rule in self._rules])

    @staticmethod
    def from_json(_json):
        if isinstance(_json, dict):
            _json = [_json]

        if isinstance(_json, list):
            rule_list = [Conjunction.from_dict(_dict) for _dict in _json]
            return Disjunction(rule_list)
        else:
            raise ValueError(f"Attempted to create disjunction object from {type(_json)=}")


class Tag:
    def __init__(self, name, rule):
        self._name = name
        self._rule = rule

    @property
    def name(self):
        return self._name

    @property
    def rule(self):
        return self._rule

    @property
    def affected_columns(self):  # TODO unused and untested
        def _get_fields_rec(rule):
            if isinstance(rule, Condition):
                return [rule._field]  # TODO create and access property instead
            elif isinstance(rule, Conjunction) or isinstance(rule, Disjunction):
                rules = []
                for rule in rule._rules:  # TODO create and access property instead
                    rules.extend(_get_fields_rec(rule._rules))
                return rules

        return {field for field in _get_fields_rec(self._rule)}

    @staticmethod
    def from_json(name, _json):
        return Tag(name, Disjunction.from_json(_json))

    @staticmethod
    def from_json_string(name, _json_str):
        return Tag.from_json(name, json.loads(_json_str))


class Tagger(abc.ABC):
    @staticmethod
    def tag(tag: Tag, df: pd.DataFrame, remove_old_tags=False):
        tag_name = tag.name
        if remove_old_tags:
            Tagger._remove_tag(tag_name, df)

        rows_to_tag = Tagger.get_index_for_rule(df, tag.rule)
        Tagger._add_tag(tag_name, df, rows_to_tag)

    @staticmethod
    def get_index_for_rule(df, rule):
        rows_to_tag = df.apply(lambda x: rule.compute(x), axis=1)
        return rows_to_tag

    @staticmethod
    def filter_df_with_rule(df, rule):
        rows_to_tag = Tagger.get_index_for_rule(df, rule)
        res_df = df[rows_to_tag].reset_index(drop=True)
        return res_df

    @staticmethod
    def _already_tagged_rows(tag_name, df):  # TODO not used
        already_tagged_rows = df['tags'].apply(lambda tags_row: tag_name in tags_row.split(','))
        return already_tagged_rows

    @staticmethod
    def _remove_tag(tag_name, df):
        def _remove_tag_from_row(row):
            row_elements = row.split(',')
            filtered_element = [element for element in row_elements if element != tag_name]
            result_row = ','.join(filtered_element)
            return result_row

        df['tags'] = df['tags'].apply(_remove_tag_from_row)

    @staticmethod
    def _add_tag(tag_name, df, to_rows):
        def _add_tag_to_row(row):
            row_elements = row.split(',')
            if tag_name not in row_elements:
                row_elements.append(tag_name)
            if '' in row_elements:
                row_elements.remove('')
            result_row = ','.join(row_elements)
            return result_row

        df.loc[to_rows, 'tags'] = df.loc[to_rows, 'tags'].apply(_add_tag_to_row)
