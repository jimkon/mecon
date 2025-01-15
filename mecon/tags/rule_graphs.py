from collections.abc import Iterable
from itertools import chain

import pandas as pd

from mecon.tags import tagging, tag_helpers


class TagGraph:
    def __init__(self, tags: Iterable[tagging.Tag], dependency_mapping: dict):
        self._tags = tags
        self._dependency_mapping = dependency_mapping

    def tidy_table(self):
        tags = []
        for tag, info in self._dependency_mapping.items():
            depends_on = info['depends_on']
            for dep_tag in depends_on:
                tags.append({'tag': tag, 'depends_on': dep_tag})
        df = pd.DataFrame(tags)
        return df

    def tags_levels(self):
        mapping = self._dependency_mapping.copy()

        def _calc_level_rec(tag, level):
            info = mapping[tag]

            if len(info['depends_on']) == 0:
                return 0

            dep_levels = []
            for dep_tag in info['depends_on']:
                 dep_levels.append(_calc_level_rec(dep_tag, level + 1))

        for tag, info in mapping.items():
            if len(info['depends_on']) == 0:
                info['level'] = 0


    @classmethod
    def from_tags(cls, tags: Iterable[tagging.Tag]):
        dependency_mapping = TagGraph.build_dependency_mapping(tags)
        return cls(tags, dependency_mapping)

    @staticmethod
    def build_dependency_mapping(tags: Iterable[tagging.Tag]) -> dict:
        rules = {tag.name: tag_helpers.expand_rule_to_subrules(tag.rule) for tag in tags}
        expanded_rules = []
        for tag_name, tag_rules in rules.items():
            for rule in tag_rules:
                expanded_rules.append({'tag': tag_name, 'rule': rule})

        df = pd.DataFrame(expanded_rules)
        df['type'] = df['rule'].apply(lambda rule: type(rule).__name__)

        df_cnd = df[df['type'] == 'Condition'].copy()
        df_cnd['is_tag_rule'] = df_cnd['rule'].apply(lambda rule: rule.field == 'tags')

        df_tags = df_cnd[df_cnd['is_tag_rule']].copy()
        df_tags['depends_on'] = df_tags['rule'].apply(
                lambda rule: rule.value if isinstance(rule.value, list) else rule.value.split(','))

        df_tags_agg = df_tags.groupby('tag').agg({'depends_on': lambda arr: list(chain(*arr))}).reset_index()

        level_zero_tags = set(df['tag']).difference(df_tags_agg['tag'])
        df_tags_l0 = pd.DataFrame({'tag': list(level_zero_tags)})
        df_tags_l0['depends_on'] = [[]]*len(df_tags_l0)

        mapping = pd.concat([df_tags_agg, df_tags_l0]).set_index('tag').to_dict('index')

        return mapping
