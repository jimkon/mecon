import json
import pathlib

import pandas as pd

from mecon.tag_tools import tagging


# TODO test all


class TagRuleMonitor:
    def __init__(self, tag: tagging.Tag):
        self.tag = tag

        self.cnt_dict = {}

    def observe(self, rule, element, outcome):
        rule_key = rule
        if rule_key not in self.cnt_dict:
            self.cnt_dict[rule_key] = 0

        if outcome:
            self.cnt_dict[rule_key] += 1


class TaggingReport:
    _filename = 'tag_report.csv'

    def __init__(self, df):
        self._df = df

    @classmethod
    def load(cls, dataser_dir: pathlib.Path):
        fullpath = dataser_dir / cls._filename
        if fullpath.exists():
            df = pd.read_csv(fullpath, index_col=None)
            return TaggingReport(df)
        return None

    def store(self, dataser_dir: pathlib.Path):
        fullpath = dataser_dir / self._filename
        self._df.to_csv(fullpath, index=False)

    def dataframe(self) -> pd.DataFrame:
        return self._df

    def unsatisfied_rules_df(self) -> pd.DataFrame:
        return self._df[self._df['count'] == 0]

    def unsatisfied_tagging_rules_df(self) -> pd.DataFrame:
        tag_rules = self._df.groupby(by='tag').agg({'rule': 'last', 'rule_type': 'last', 'count': 'last'}).reset_index()
        return tag_rules[tag_rules['count'] == 0]


class TaggingStatsMonitoringSystem:  # TODO Disjunction dict appears twice in the report
    def __init__(self, tags: list[tagging.Tag]):
        self._tags = tags

        self._tag_monitors = []
        for tag in self._tags:
            rule_monitoring = TagRuleMonitor(tag)
            self._tag_monitors.append(rule_monitoring)
            tag.rule.add_observers_recursively(rule_monitoring.observe)

    def produce_report(self) -> TaggingReport:
        report_json = []
        for tag, monitor in zip(self._tags, self._tag_monitors):
            for rule, cnt in monitor.cnt_dict.items():
                try:
                    row = {
                        'tag': tag.name,
                        'rule': json.dumps(rule.to_json()),
                        'rule_type': rule.__class__.__name__,
                        'count': cnt
                    }
                except Exception as e:
                    row = {
                        'tag': tag.name,
                        'rule': 'ERROR',
                        'rule_type': rule.__class__.__name__,
                        'count': -1
                    }
                report_json.append(row)
        report_df = pd.DataFrame(report_json)
        tagging_report = TaggingReport(report_df)

        return tagging_report
