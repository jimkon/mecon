import logging
from collections.abc import Iterable
from itertools import chain

import networkx as nx
import pandas as pd

from mecon.tags import tagging, tag_helpers


class TagGraph:
    def __init__(self, tags: Iterable[tagging.Tag], dependency_mapping: dict):
        self._tags = tags
        self._dependency_mapping = dependency_mapping

        # if not self.has_cycles():
        if len(self.find_all_cycles()) == 0:
            self.add_hierarchy_levels()

    def levels(self):
        if len(self.find_all_cycles()) > 0:
            raise ValueError("Cannot calculate hierarchy on a graph with cycles")
        return {tag: info['level'] for tag, info in self._dependency_mapping.items()}

    def tidy_table(self, ignore_tags_with_no_dependencies=False):
        tags = []
        for tag, info in self._dependency_mapping.items():
            row_dict = {'tag': tag}
            if 'level' in info:
                row_dict['level'] = info['level']

            depends_on = info['depends_on']
            if len(depends_on) == 0 and not ignore_tags_with_no_dependencies:
                row_dict['depends_on'] = None
                tags.append(row_dict)
            else:
                for dep_tag in depends_on:
                    tags.append(dict(**row_dict, depends_on=dep_tag))

        df = pd.DataFrame(tags)
        return df

    def add_hierarchy_levels(self):
        if len(self.find_all_cycles()) > 0:
            raise ValueError("Cannot calculate hierarchy on a graph with cycles")

        mapping = self._dependency_mapping.copy()

        def _calc_level_rec(tag):
            if tag not in mapping:
                logging.warning(f"{tag} not in dependency mapping while calculating hierarchy. Will be replaced with 0")
                return 0

            if 'level' in mapping[tag]:
                return mapping[tag]['level']

            if len(mapping[tag]['depends_on']) == 0:
                mapping[tag]['level'] = 0
                return 0

            dep_levels = []
            for dep_tag in mapping[tag]['depends_on']:
                 dep_levels.append(_calc_level_rec(dep_tag))

            tag_level = max(dep_levels)+1
            mapping[tag]['level'] = tag_level
            return tag_level


        for tag, info in mapping.items():
            _calc_level_rec(tag)

        # levels_dict = {tag: info['level'] for tag, info in mapping.items()}
        self._dependency_mapping = mapping

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

    # @deprecated
    # def has_cycles(self):
    #     df = self.tidy_table()
    #     # Create a directed graph
    #     G = nx.DiGraph()
    #
    #     # Add edges to the graph based on the DataFrame
    #     for _, row in df.iterrows():
    #         tag = row['tag']
    #         depends_on = row['depends_on']
    #
    #         if depends_on is None:
    #             continue
    #
    #         for dependency in depends_on:
    #             G.add_edge(dependency, tag)
    #
    #     # Check for cycles in the graph
    #     try:
    #         # networkx will throw an exception if a cycle exists
    #         nx.find_cycle(G, orientation="original")
    #         return True
    #     except nx.NetworkXNoCycle:
    #         return False

    def find_all_cycles(self):
        df = self.tidy_table()
        # Create a directed graph
        G = nx.DiGraph()

        # Add edges based on the DataFrame
        for _, row in df.iterrows():
            tag = row['tag']
            depends_on = row['depends_on']
            if pd.notna(depends_on):
                G.add_edge(depends_on, tag)

        # Find all simple cycles
        cycles = list(nx.simple_cycles(G))

        return cycles

    def create_plotly_graph(self, k=.5, levels_col=None):
        from mecon.data.graphs import create_plotly_graph
        df = self.tidy_table()
        return create_plotly_graph(df, from_col='tag', to_col='depends_on', k=k, levels_col=levels_col)

    def remove_cycles(self) -> 'TagGraph':
        edges = self.tidy_table().values.tolist()
        cycles = self.find_all_cycles()

        cleaned_edges = [edge for edge in edges if edge not in cycles]

        new_df = pd.DataFrame(cleaned_edges, columns=['tag', 'depends_on']).groupby('tag').agg({'depends_on': list}).reset_index()
        new_df['depends_on'] = new_df['depends_on'].apply(lambda arr: arr if arr is not None and len(arr) > 0 and arr[0] is not None else list())

        # new_dep_mapping = {k: v['depends_on'] for k, v in new_df.set_index('tag').to_dict('index').items()}
        new_dep_mapping = new_df.set_index('tag').to_dict('index')

        new_tg = TagGraph(self._tags, new_dep_mapping)
        return new_tg


if __name__ == '__main__':
    from mecon import config
    from mecon.app.file_system import WorkingDataManager, WorkingDatasetDir
    datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
    if not datasets_dir.exists():
        raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

    datasets_obj = WorkingDatasetDir()
    datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
    dataset = datasets_obj.working_dataset
    data_manager = WorkingDataManager()

    transaction = data_manager.get_transactions()
    print(transaction.size())

    tags = data_manager.all_tags()
    tg = TagGraph.from_tags(tags)
    # tg.hierarchy()
    print(f"{tg.find_all_cycles()=}")
    tg2 = tg.remove_cycles()
    tg2.create_plotly_graph(k=.5, levels_col='level')

    t = 0
