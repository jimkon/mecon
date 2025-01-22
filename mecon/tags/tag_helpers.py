import logging

import pandas as pd

from mecon.data.transactions import Transactions
from mecon.tags import tagging


def add_rule_for_id(tag: tagging.Tag, ids_to_add: str | list[str]) -> tagging.Tag:
    ids_to_add = ids_to_add if isinstance(ids_to_add, list) else [ids_to_add]
    potential_id_condition = tag.rule.rules[0].rules[0]
    if potential_id_condition.field == 'id' and potential_id_condition.transformation_operation.name == 'none':
        existing_ids = set(potential_id_condition.value.split(','))
        non_existing_ids_to_add = set(ids_to_add).difference(existing_ids)

        if len(non_existing_ids_to_add) == 0:
            return tag
        merged_ids_str = ','.join(sorted(non_existing_ids_to_add))
        id_condition = tagging.Condition.from_string_values('id', 'none', 'in_csv',
                                                            f"{merged_ids_str},{potential_id_condition.value}")
        tag.rule.rules[0].rules[0] = id_condition
    else:
        merged_ids_str = ','.join(sorted(ids_to_add))
        id_condition = tagging.Condition.from_string_values('id', 'none', 'in_csv', merged_ids_str)
        tag = tagging.Tag(tag.name, tag.rule.append(tagging.Conjunction([id_condition])))
    return tag


def expand_rule_to_subrules(rule: tagging.AbstractRule) -> list[tagging.AbstractRule]:
    expanded_rules = []
    rule_to_expand = [rule]
    while len(rule_to_expand) > 0:
        rule = rule_to_expand.pop(0)
        expanded_rules.append(rule)

        if isinstance(rule, tagging.Disjunction):
            subrules = rule.rules
        elif isinstance(rule, tagging.Conjunction):
            subrules = rule.rules
        elif isinstance(rule, tagging.Condition):
            continue
        else:
            raise ValueError(f"Unexpected rule type: {type(rule)}")

        rule_to_expand.extend(subrules)

    return expanded_rules


def tag_stats_from_transactions(transactions: Transactions) -> pd.DataFrame:
    logging.info(f"Calculating tag stats from {transactions.size()} transactions.")
    from pandarallel import pandarallel
    pandarallel.initialize(progress_bar=True, use_memory_fs=False)

    tag_stats = transactions.all_tag_counts()
    tag_stats_df = pd.DataFrame({'Tag': list(tag_stats.keys()), 'Count': list(tag_stats.values())})
    tag_stats_df['Total money in'] = tag_stats_df['Tag'].parallel_apply(lambda tag_name: transactions.containing_tags(tag_name).positive_amounts().amount.sum())
    tag_stats_df['Total money out'] = tag_stats_df['Tag'].parallel_apply(lambda tag_name: transactions.containing_tags(tag_name).negative_amounts().amount.sum())
    tag_stats_df['Date created'] = tag_stats_df['Tag'].apply(lambda tag_name: 'TODO')
    return tag_stats_df
