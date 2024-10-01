import logging

import pandas as pd

from mecon.tags import tag_helpers


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