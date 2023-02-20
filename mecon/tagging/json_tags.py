import json
import os

import numpy as np

from mecon.tagging.json_tag_utils import field_processing_functions_dict, match_funcs_dict
from mecon.tagging.tag import Tag


class SelectCondition:
    def __init__(self, column, preproc, cond_func, cond_value):
        self._column = column
        self._preprocessing_function = preproc
        self._condition_function = cond_func
        self._condition_value = cond_value

    def __call__(self, x):
        return self.calculate(x)

    def calculate(self, x):
        x_column = x[self._column]
        preproc_x = x_column.apply(self.preprocessing_function)
        condition = preproc_x.apply(lambda x: self.condition_function(x, self._condition_value)).to_list()
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


class RuleDict:
    def __init__(self, select_conditions):
        self._conds = select_conditions

    @property
    def conditions(self):
        return self._conds

    def __call__(self, _df):
        return self.calculate(_df)

    def calculate(self, _df):
        return np.logical_and.reduce(self.calc_each_condition(_df))

    def calc_each_condition(self, _df):
        return np.array([cond(_df) for cond in self.conditions])

    @classmethod
    def from_dict(cls, _dict):
        return cls(cls.analyse_query_dict(_dict))

    @staticmethod
    def analyse_query_dict(query_dict):
        rules_list = []
        for col_name_full, col_dict in query_dict.items():
            col_name = col_name_full.split('.')[0]
            preproc_function = col_name_full.split('.')[1] if len(col_name_full.split('.')) > 1 else None
            for cond_func, cond_value_list in query_dict[col_name_full].items():
                if not isinstance(cond_value_list, list):
                    cond_value_list = [cond_value_list]

                for cond_value in cond_value_list:
                    rules_list.append(SelectCondition(col_name, preproc_function, cond_func, cond_value))
        return rules_list


class SelectQuery:
    def __init__(self, json_object):
        if isinstance(json_object, dict):
            json_object = [json_object]
        self._json = json_object
        self._rules = [RuleDict.from_dict(_dict) for _dict in json_object]

    @property
    def json(self):
        return self._json

    @property
    def rules(self):
        return self._rules

    def __call__(self, _df):
        return self.calculate(_df)

    def calculate(self, _df):
        rules_res = [rule(_df) for rule in self._rules]
        return np.logical_or.reduce(rules_res)

    @classmethod
    def from_json_string(cls, json_str):
        _json = json.loads(json_str)
        return SelectQuery(_json)


class JsonTag(Tag):
    @classmethod
    def from_json_file(cls, filepath):
        full_filename = os.path.basename(filepath)
        filename, ext = os.path.splitext(full_filename)
        if ext == '.json':
            with open(filepath, 'r') as fp:
                json_content = json.load(fp)
                return JsonTag(filename, json_content)
        else:
            raise ValueError(f"File has to be a JSON file. Got {ext} instead ({filepath}).")

    def __init__(self, tag_name, _json):
        super().__init__(tag_name)
        self._query = SelectQuery(_json)

    @property
    def json(self):
        return self._query.json

    def _calc_condition(self, _df):
        return self._query(_df)
