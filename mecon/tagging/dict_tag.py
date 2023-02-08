import pandas as pd

from mecon.tagging.tag import Tag
from mecon.tagging.dict_tag_utils import field_processing_functions_dict, match_funcs_dict


class Rule:
    def __init__(self, column, preproc, cond_func, cond_value):
        self._column = column
        self._preprocessing_function = preproc
        self._condition_function = cond_func
        self._condition_value = cond_value

    def __call__(self, x):
        return self.calculate(x)

    def calculate(self, x):
        x_column = x[self._column]
        preproc_x = self.preprocessing_function(x_column)
        condition = self.condition_function(preproc_x, self._condition_value)
        return condition

    @property
    def column(self):
        return self._column

    @property
    def preprocessing_function(self):
        if self._preprocessing_function:
            if isinstance(self._preprocessing_function, str):
                res = field_processing_functions_dict[self._preprocessing_function]
            else:
                res = self._preprocessing_function
        else:
            res = lambda x: x
        return res

    @property
    def condition_function(self):
        if isinstance(self._condition_function, str):
            return match_funcs_dict[self._condition_function]
        else:
            return self._condition_function

    @property
    def condition_value(self):
        return self._condition_value

    def __repr__(self):
        return f"Rule: {self._condition_function}( {self._preprocessing_function}({self._column}) , {self._condition_value} )"


def analyse_rule_dict(rule_dict):
    rules_list = []
    for col_name_full, col_dict in rule_dict.items():
        col_name = col_name_full.split('.')[0]
        preproc_function = col_name_full.split('.')[1] if len(col_name_full.split('.'))>1 else None
        for cond_func, cond_value_list in rule_dict[col_name_full].items():
            if not isinstance(cond_value_list, list):
                cond_value_list = [cond_value_list]

            for cond_value in cond_value_list:
                rules_list.append(Rule(col_name, preproc_function, cond_func, cond_value))
    return rules_list


class DictTag(Tag):
    def __init__(self, tag_name, _json):
        super().__init__(tag_name)
        self._rule_tree = []
        self._json = _json if isinstance(_json, list) else [_json]
        for _dict in self._json:
            self._add_rules(_dict)

    @property
    def json(self):
        return self._json

    def _add_rules(self, _dict):
        rules = analyse_rule_dict(_dict)
        self._rule_tree.append(rules)

    def _calc_condition(self, _df):
        res = pd.Series([False]*len(_df))
        for or_rule in self._rule_tree:
            res_and = pd.Series([True]*len(_df))
            for and_rule in or_rule:
                res_rule = _df.apply(and_rule, axis=1)
                res_and &= res_rule
            res |= res_and
        return res