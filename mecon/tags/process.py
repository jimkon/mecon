import abc
import logging
import time
from collections import namedtuple
from itertools import chain
from typing import Any

import numpy as np
import pandas as pd
from tqdm import tqdm
from typing_extensions import override

from mecon.data.transactions import Transactions
from mecon.tags import tagging
from mecon.tags.rule_graphs import AcyclicTagGraph
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
        for tag in tqdm(self.tags, desc='Applying tag'):
            transactions = transactions.apply_tag(tag)
        return transactions


class RuleExecutionPlanTagging(TaggingSession):
    class TagApplicator:
        def __init__(self, tag_name: str, depends_on: tagging.AbstractRule):
            self.tag_name = tag_name
            self.depends_on = depends_on

        def __repr__(self):
            return f"TagApplicator({self.tag_name})"

    def __init__(self, tags: list[Tag], remove_cycles: bool = True):
        super().__init__(tags)

        tg = AcyclicTagGraph.from_tags(tags)
        tg.add_hierarchy_levels()
        if remove_cycles:
            tg = tg.remove_cycles()

        if len(tg.find_all_cycles()) > 0:
            # tg = tg.remove_cycles()
            raise ValueError("Cannot run ExtendedRuleTagging on a graph with cycles")
        self._levels_dict = tg.levels()
        self._rule_aliases = {}
        self._op_monitoring = []
        self._df_plan = None

    @property
    def plan(self):
        return self._df_plan

    # def get_plan(self):
    #     if self._df_plan is None:
    #         self._df_plan = self.create_rule_execution_plan()
    #     return self._df_plan

    def operation_monitoring_table(self):
        return pd.DataFrame(self._op_monitoring) if self._op_monitoring else None

    def convert_rule_to_df_rule(self, rule) -> callable(pd.DataFrame):
        rule_alias = self._rule_aliases.get(rule)
        if isinstance(rule, tagging.Condition):
            def condition_op(df_in) -> pd.Series:
                comp_f = lambda df_value: rule.compare_operation(rule.transformation_operation(df_value),
                                                                 rule.value)
                res = df_in[f"{rule.field}"].apply(comp_f).rename(rule_alias)  # TODO optimise, np.vectorise maybe
                self._op_monitoring.append(
                    {'tag': rule.parent_tag, 'in': f"{rule.field}", 'out': rule_alias, 'allias': rule_alias})
                return res

            return condition_op
        elif isinstance(rule, tagging.Conjunction):
            def conjunction_op(df_in) -> pd.Series:
                in_cols = [self._rule_aliases.get(subrule) for subrule in rule.rules]
                res = df_in[in_cols].all(axis=1).rename(rule_alias)
                self._op_monitoring.append(
                    {'tag': rule.parent_tag, 'in': in_cols, 'out': rule_alias, 'allias': rule_alias})
                return res

            return conjunction_op
        elif isinstance(rule, tagging.Disjunction):
            def disjunction_op(df_in) -> pd.Series:
                in_cols = [self._rule_aliases.get(subrule) for subrule in rule.rules]
                res = df_in[in_cols].any(axis=1).rename(rule_alias)
                self._op_monitoring.append(
                    {'tag': rule.parent_tag, 'in': in_cols, 'out': rule_alias, 'allias': rule_alias})
                return res

            return disjunction_op
        elif isinstance(rule, self.TagApplicator):
            def tag_application_op(df_in) -> pd.Series:
                res = df_in[self._rule_aliases.get(rule.depends_on)].apply(lambda b: [rule.tag_name] if b else [])
                self._op_monitoring.append(
                    {'tag': rule.parent_tag, 'in': str(rule.depends_on), 'out': "tags", 'allias': rule_alias})
                return res

            return tag_application_op
        else:
            raise ValueError(f"Unexpected rule type: {type(rule)}")

    @timeit
    def create_rule_execution_plan(self) -> 'RuleExecutionPlanTagging':
        rules = {tag.name: expand_rule_to_subrules(tag.rule) for tag in self.tags}
        expanded_rules = []
        for tag_name, tag_rules in rules.items():
            tag_rules.insert(0, RuleExecutionPlanTagging.TagApplicator(tag_name, depends_on=tag_rules[0]))
            for rule in tag_rules:
                # if hasattr(rule, 'parent_tag'):
                #     raise ValueError(f"Unexpected 'parent_tag' attribute on '{rule=}'")
                # else:
                rule.parent_tag = tag_name

                expanded_rules.append({'tag': tag_name, 'rule': rule})

        df_plan = pd.DataFrame(expanded_rules)
        df_plan['type'] = df_plan['rule'].apply(lambda rule: type(rule).__name__)
        df_plan['tag_level'] = df_plan['tag'].map(self._levels_dict)
        df_plan['rule_level'] = df_plan['rule'].apply(lambda rule:
                                                      .8 if isinstance(rule, self.TagApplicator) else
                                                      .4 if isinstance(rule, tagging.Disjunction) else
                                                      .2 if isinstance(rule, tagging.Conjunction) else
                                                      .1 if rule.field == 'tags' else
                                                      0)
        df_plan['priority'] = df_plan.apply(
            lambda row: 0 if row['rule_level'] == 0 else row['tag_level'] + row['rule_level'],
            axis=1).astype(str)

        del df_plan['tag_level'], df_plan['rule_level']

        self._rule_aliases = {rule: str(rule) for rule in df_plan['rule'].to_list()}

        logging.info(f"Created {len(df_plan)} rules using {len(self._rule_aliases)} aliases.")

        self._df_plan = df_plan

        return self

    def split_in_batches(self) -> dict:
        batches = {str(priority): batch['rule'] for priority, batch in
                   self.plan.groupby('priority').agg({'rule': list}).to_dict('index').items()}
        return batches

    @staticmethod
    def prepare_transactions(transactions: Transactions) -> pd.DataFrame:
        df = transactions.dataframe().copy()[
            ['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description', 'tags']]
        df['old_tags'] = df['tags']
        df['tags'] = ''
        df['new_tags_list'] = np.empty((len(df), 0)).tolist()

        return df

    @timeit
    def tag(self, transactions: Transactions) -> Transactions:
        rule_groups = self.split_in_batches()
        all_priorities = sorted(rule_groups.keys(), reverse=False)

        df_trans = self.prepare_transactions(transactions)

        for priority in all_priorities:
            rules = rule_groups[priority]
            column_rules = [self.convert_rule_to_df_rule(rule) for rule in rules]
            logging.info(f"Applying {len(rules)} rules, with priority {priority}")
            group_results = [col_rule(df_trans) for col_rule in tqdm(column_rules, desc=f"Priority {priority}")]

            if priority.endswith('.8'):
                df_temp = pd.concat(group_results, axis=1)

                new_tags_col = f"tags_added_{priority}"
                df_trans[new_tags_col] = df_temp.apply(
                    lambda row: list(chain(*[row[col] for col in df_temp.columns])), axis=1)
                df_trans['new_tags_list'] = df_trans.apply(lambda row: row['new_tags_list'] + row[new_tags_col], axis=1)

                df_trans['tags'] = df_trans['new_tags_list'].apply(lambda tags: ','.join(tags))
                df_trans[f"tags_list_{priority}"] = df_trans['new_tags_list']
            else:
                df_trans = pd.concat([df_trans, *group_results], axis=1)
            continue

        new_transactions = Transactions(df_trans[transactions.dataframe().columns])
        # TODO to remove
        self.operation_monitoring_table().to_csv('op_monitoring.csv', index=False)
        df_trans.to_csv('transactions_with_rules.csv', index=False)
        transactions.dataframe().to_csv('transactions.csv', index=False)
        return new_transactions


class REPTagging(RuleExecutionPlanTagging):
    pass


class OptimisedRuleExecutionPlanTagging(RuleExecutionPlanTagging):
    Transformation = namedtuple('Transformation', ['field', 'trans', 'parent_tag'])

    def __init__(self, tags: list[Tag], remove_cycles: bool = True):
        super().__init__(tags, remove_cycles)

    @staticmethod
    def transformations_execution_plan(df_plan) -> pd.DataFrame:
        non_tag_conditions = df_plan[df_plan['type'] == 'Condition'].copy()

        non_tag_conditions[
            'is_trans'] = True  # non_tag_conditions['rule'].apply(lambda rule: rule.transformation_operation.name != 'none')
        non_tag_conditions['trans_id'] = non_tag_conditions['rule'].apply(lambda
                                                                              rule: f"{rule.field}.{rule.transformation_operation.name}")  # if rule.transformation_operation.name != 'none' else rule.field)

        filtered_conditions = non_tag_conditions[non_tag_conditions['is_trans']].drop_duplicates(subset=['trans_id'])
        logging.info(
            f"Reduce {len(non_tag_conditions)} conditions to {len(filtered_conditions)} unique transformation operations")

        filtered_conditions['rule'] = filtered_conditions.apply(
            lambda row: OptimisedRuleExecutionPlanTagging.Transformation(field=row['rule'].field,
                                                                         trans=row['rule'].transformation_operation,
                                                                         parent_tag=row['tag']), axis=1)
        filtered_conditions['type'] = 'Transformation'
        filtered_conditions['priority'] = '-1'

        logging.info(f"Expanded the plan with {len(filtered_conditions)} transformations")

        del filtered_conditions['is_trans'], filtered_conditions['trans_id']
        return filtered_conditions

    @staticmethod
    def remove_unnecessary_composite_rules(df_plan, alias_dict) -> tuple[pd.DataFrame, dict[Any, Any]]:
        # TODO maybe remove composite rules with zero or one subrules
        df_plan['n_subrules'] = df_plan['rule'].apply(
            lambda rule: len(rule.rules) if isinstance(rule, tagging.AbstractCompositeRule) else -1)

        conjunctions = df_plan[(df_plan['type'] == 'Conjunction') & (df_plan['n_subrules'] == 1)]['rule'].to_list()
        for composite_rule in conjunctions:
            alias_dict[composite_rule] = alias_dict[composite_rule.rules[0]]

        disjunctions = df_plan[(df_plan['type'] == 'Disjunction') & (df_plan['n_subrules'] == 1)]['rule'].to_list()
        for composite_rule in disjunctions:
            alias_dict[composite_rule] = alias_dict[composite_rule.rules[0]]

        df_plan_reduced = df_plan[df_plan['n_subrules'] != 1]
        del df_plan_reduced['n_subrules']
        logging.info(
            f"Removed {len(conjunctions)} redundant Conjunctions and {len(disjunctions)} Disjunctions, going from {len(df_plan)} to {len(df_plan_reduced)} rules (reduced to {100*len(df_plan_reduced)/len(df_plan):.0f}% of the original size).")

        # df_plan_reduced['alias'] = df_plan_reduced.apply(lambda row: alias_dict[row['rule']], axis=1)
        return df_plan_reduced, alias_dict

    @staticmethod
    def deduplicate_rule_execution_plan(df_plan) -> pd.DataFrame:
        df_plan = df_plan.copy()
        df_plan['rule_id'] = df_plan['rule'].astype(str)
        df_dedup = df_plan.groupby('rule_id').agg({'type': 'first', 'tag':'first', 'rule': 'first', 'priority': 'min'}).reset_index()

        logging.info(f"Deduplicated {len(df_plan)} rules down to {len(df_dedup)} rules")
        del df_dedup['rule_id']

        return df_dedup

    def convert_rule_to_df_rule(self, rule) -> callable(pd.DataFrame):
        rule_alias = self._rule_aliases.get(rule)
        if isinstance(rule, OptimisedRuleExecutionPlanTagging.Transformation):
            field, trans_op, parent_tag = rule

            def tranform_op(df_in) -> pd.Series:
                res = df_in[field].apply(trans_op).rename(
                    f"{field}.{trans_op.name}")  # TODO optimise, np.vectorise maybe
                self._op_monitoring.append(
                    {'tag': rule.parent_tag, 'in': field, 'out': f"{field}.{trans_op.name}", 'allias': rule_alias})
                return res

            return tranform_op
        elif isinstance(rule, tagging.Condition):
            def condition_op(df_in) -> pd.Series:
                comp_f = lambda df_value: rule.compare_operation(df_value, rule.value)
                res = df_in[f"{rule.field}.{rule.transformation_operation.name}"].apply(comp_f).rename(
                    rule_alias)  # TODO optimise, np.vectorise maybe
                self._op_monitoring.append(
                    {'tag': rule.parent_tag, 'in': f"{rule.field}.{rule.transformation_operation.name}",
                     'out': rule_alias, 'allias': rule_alias})
                return res

            return condition_op
        else:
            return super().convert_rule_to_df_rule(rule)

    @timeit
    def create_optimised_rule_execution_plan(self) -> 'OptimisedRuleExecutionPlanTagging':
        df_plan, alias_dict = self.plan, self._rule_aliases
        df_trans = self.transformations_execution_plan(df_plan)
        df_plan_rules_and_trans = pd.concat([df_plan, df_trans])

        df_plan_reduced, reduced_alias_dict = self.remove_unnecessary_composite_rules(df_plan_rules_and_trans, alias_dict)

        df_plan_opt = self.deduplicate_rule_execution_plan(df_plan_reduced)
        logging.info(
            f"Reduce {len(df_plan)} rules to {len(df_plan_opt)} unique rules and {len(set(self._rule_aliases))} aliases.")

        self._df_plan = df_plan_opt
        self._rule_aliases = reduced_alias_dict

        return self


class OptREPTagging(OptimisedRuleExecutionPlanTagging):
    pass
