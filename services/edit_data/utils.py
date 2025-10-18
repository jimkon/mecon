import logging

import pandas as pd

from mecon.tags import tag_helpers
from mecon.data import groupings
from mecon.data.transactions import Transactions


def save_tag_changes(changes_per_tag, data_manager):
    # TODO add unit tests, docstrings and typehints
    for tag_name, ids in changes_per_tag.items():
        tag_object = data_manager.get_tag(tag_name)
        new_tag = tag_helpers.add_rule_for_id(tag_object, ids)
        data_manager.update_tag(new_tag)


def sort_and_filter_transactions_df(transactions, order, page_number, page_size):
    if order == 'Newest transactions':
        n_week = page_number
        weekly_transactions = transactions.group(groupings.WEEK)
        target_transactions_df = weekly_transactions[n_week].dataframe().sort_values('datetime', ascending=False)
    elif order == 'Least tagged':
        n_page = page_number
        page_start_index, page_end_index = n_page * page_size, (n_page + 1) * page_size
        df = transactions.dataframe()
        df['n_tags'] = df['tags'].apply(lambda x: len(x.split(',')))
        df.sort_values('n_tags', ascending=True, inplace=True)
        del df['n_tags']
        target_transactions_df = df[page_start_index:page_end_index]
    else:
        raise ValueError(f"Invalid ordering: {order}")

    return target_transactions_df


