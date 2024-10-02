import logging

import pandas as pd

from mecon.tags import tag_helpers
from mecon.data import groupings
from mecon.data.transactions import Transactions


def save_tag_changes(added_tags, removed_tags, data_manager):
    def transform_changes_dict(_dict):
        all_changes = []
        for tid, tags in _dict.items():
            for tag in tags:
                all_changes.append({'id': tid, 'tag': tag})
        tag_oriented_changes = pd.DataFrame(all_changes).groupby(['tag']).agg({'id': list}).to_dict(orient='index')
        tag_oriented_changes = {_tag_name: _ids['id'] for _tag_name, _ids in tag_oriented_changes.items()}
        return tag_oriented_changes

    logging.info(f"Saving changes: {len(added_tags)} additions and {len(removed_tags)} removals/")

    ids_added_to_tags = transform_changes_dict(added_tags)
    for tag_name, ids in ids_added_to_tags.items():
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


