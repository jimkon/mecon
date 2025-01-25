import abc
import logging
import time
from collections import namedtuple

import pandas as pd

from mecon.data.transactions import Transactions
from mecon.tags import tagging
from mecon.tags.rule_graphs import TagGraph
from mecon.tags.tag_helpers import expand_rule_to_subrules
from mecon.tags.tagging import Tag


def timeit(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"{func.__name__} took {end_time - start_time} seconds to execute.")
        return result

    return wrapper


class TaggingSession(abc.ABC):
    def __init__(self, tags: list[Tag]):
        self.tags = tags

    @abc.abstractmethod
    def tag(self, transactions: Transactions) -> Transactions:
        pass


class LinearTagging(TaggingSession):
    @timeit
    def tag(self, transactions: Transactions) -> Transactions:
        for tag in self.tags:
            transactions = transactions.apply_tag(tag)
        return transactions


Transformation = namedtuple('Transformation', ['field', 'trans'])

class ExtendedRuleTagging(TaggingSession):
    def __init__(self, tags: list[Tag], remove_cycles: bool = True):
        super().__init__(tags)

        tg = TagGraph.from_tags(tags)
        if remove_cycles:
            tg = tg.remove_cycles()

        if len(tg.find_all_cycles()) > 0:
            # tg = tg.remove_cycles()
            raise ValueError("Cannot run ExtendedRuleTagging on a graph with cycles")
        self._levels_dict = tg.levels()

    @timeit
    def rule_priority_table(self, separate_transformations: bool = True) -> pd.DataFrame:
        rules = {tag.name: expand_rule_to_subrules(tag.rule) for tag in self.tags}
        expanded_rules = []
        for tag_name, tag_rules in rules.items():
            for rule in tag_rules:
                expanded_rules.append({'tag': tag_name, 'rule': rule})

        df = pd.DataFrame(expanded_rules)
        df['type'] = df['rule'].apply(lambda rule: type(rule).__name__)
        df['tag_level'] = df['tag'].map(self._levels_dict)
        df['rule_level'] = df['rule'].apply(lambda rule: .4 if isinstance(rule, tagging.Disjunction) else
        .2 if isinstance(rule, tagging.Conjunction) else
        .1 if rule.field == 'tags' else
        0)
        df['priority'] = df.apply(lambda row: 0 if row['rule_level'] == 0 else row['tag_level'] + row['rule_level'],
                                  axis=1)

        # TODO maybe remove composite rules with zero or one subrules
        # df['n_subrules'] = df['rule'].apply(lambda rule: len(rule.rules) if isinstance(rule, tagging.AbstractCompositeRule) else -1)

        if separate_transformations:
            df_trans = self._extract_transformation_operations(df)
            df = pd.concat([df, df_trans])

        df['rule_id'] = df['rule'].astype(str)
        df_dedup = df.groupby('rule_id').agg({'type': 'first', 'rule': 'first', 'priority': 'min'}).reset_index()
        logging.info(f"Reduce {len(df)} rules to {len(df_dedup)} unique rules")

        return df_dedup

    @timeit
    def _extract_rule_groups(self, rule_priority_table: pd.DataFrame) -> dict:
        return rule_priority_table.groupby('priority').agg({'rule': list}).to_dict('index')

    @timeit
    def _extract_transformation_operations(self, rule_priority_table: pd.DataFrame) -> pd.DataFrame:
        non_tag_conditions = rule_priority_table[rule_priority_table['type'] == 'Condition'].copy()

        non_tag_conditions['is_trans'] = True#non_tag_conditions['rule'].apply(lambda rule: rule.transformation_operation.name != 'none')
        non_tag_conditions['trans_id'] = non_tag_conditions['rule'].apply(lambda rule: f"{rule.field}.{rule.transformation_operation.name}")# if rule.transformation_operation.name != 'none' else rule.field)

        filtered_conditions = non_tag_conditions[non_tag_conditions['is_trans']].drop_duplicates(subset=['trans_id'])
        logging.info(f"Reduce {len(non_tag_conditions)} conditions to {len(filtered_conditions)} unique transformation operations")

        filtered_conditions['rule'] = filtered_conditions['rule'].apply(lambda rule: Transformation(field=rule.field, trans=rule.transformation_operation))
        filtered_conditions['type'] = 'Transformation'
        filtered_conditions['priority'] = -1


        del filtered_conditions['tag_level'], filtered_conditions['rule_level'], filtered_conditions['is_trans'], filtered_conditions['trans_id']
        return filtered_conditions

    # def _calculate_priorities(self, rule):
    #     if isinstance(rule, tagging.Condition):
    #         if rule.field == 'tags':
    #             if rule.value not in self._levels_dict:
    #                 logging.warning(f"{rule.value} not in dependency mapping while calculating hierarchy. Will be replaced with 0")
    #                 return 0
    #             return self._levels_dict[rule.value]
    #         else:
    #             return 0
    #
    #     priority = 0
    #     if isinstance(rule, tagging.Disjunction):
    #         priority = .1
    #     elif isinstance(rule, tagging.Conjunction):
    #         priority = .05
    #
    #     # subrules = expand_rule_to_subrules(rule)
    #     subrules = rule.rules
    #     if len(subrules) == 0:
    #         logging.warning(f"Composite rule {rule} is empty. Will be replaced with 0")
    #         return 0
    #
    #     subrule_priorities = [self._calculate_priorities(subrule) for subrule in subrules]
    #     subrule_max_priority = max(subrule_priorities)
    #     total_priority = subrule_max_priority+priority
    #
    #     return total_priority

    def _to_column_rule(self, rule):
        if isinstance(rule, Transformation):
            field, trans_op = rule
            return lambda df_in: df_in[field].apply(trans_op).rename(f"{field}.{trans_op.name}")  # TODO optimise, np.vectorise maybe
        elif isinstance(rule, tagging.Condition):
            comp_f = lambda a: rule.compare_operation(a, rule.value)
            return lambda df_in: df_in[f"{rule.field}.{rule.transformation_operation.name}"].apply(comp_f).rename(f"{str(rule)}") # TODO optimise, np.vectorise maybe
        elif isinstance(rule, tagging.Conjunction):
            return lambda df_in: df_in[[str(subrule) for subrule in rule.rules]].all(axis=1).rename(f"{str(rule)}")
        elif isinstance(rule, tagging.Disjunction):
            return lambda df_in: df_in[[str(subrule) for subrule in rule.rules]].any(axis=1).rename(f"{str(rule)}")
        else:
            raise ValueError(f"Unexpected rule type: {type(rule)}")

    @timeit
    def tag(self, transactions: Transactions) -> Transactions:
        from tqdm import tqdm
        rule_priority_table = self.rule_priority_table()
        rule_groups = self._extract_rule_groups(rule_priority_table)
        all_priorities = sorted(rule_groups.keys(), reverse=False)
        df = transactions.dataframe().copy()
        for priority in all_priorities:
            rules = rule_groups[priority]['rule']
            column_rules = [self._to_column_rule(rule) for rule in rules]  # TODO use unique rules
            logging.info(f"Applying {len(rules)} rules, with priority {priority}")
            group_results = [col_rule(df) for col_rule in tqdm(column_rules, desc=f"Priority {priority}")]
            df = pd.concat([df, *group_results], axis=1)

        for tag in self.tags:
            transactions = transactions.apply_tag(tag)
        return transactions
