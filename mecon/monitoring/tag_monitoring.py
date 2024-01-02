import json

import pandas as pd

from mecon.tag_tools import tagging


class TagRuleMonitor:
    def __init__(self, tag: tagging.Tag):
        self.tag = tag

        self.cnt_dict = {}

    def observe(self, rule, element, outcome):
        if outcome:
            rule_key = rule
            if rule_key not in self.cnt_dict:
                self.cnt_dict[rule_key] = 0
            self.cnt_dict[rule_key] += 1


class TaggingStatsMonitoringSystem:  # TODO Disjunction dict appears twice in the report
    def __init__(self, tags: list[tagging.Tag]):
        self._tags = tags

        self._tag_monitors = []
        for tag in self._tags:
            rule_monitoring = TagRuleMonitor(tag)
            self._tag_monitors.append(rule_monitoring)
            tag.rule.add_observers_recursively(rule_monitoring.observe)

    def produce_report(self) -> pd.DataFrame:
        report_json = []
        for tag, monitor in zip(self._tags, self._tag_monitors):
            for rule, cnt in monitor.cnt_dict.items():
                try:
                    row = {
                        'tag': tag.name,
                        'rule': json.dumps(rule.to_json()),
                        'count': cnt
                    }
                except Exception as e:
                    row = {
                        'tag': tag.name,
                        'rule': 'ERROR',
                        'count': -1
                    }
                report_json.append(row)
        report_df = pd.DataFrame(report_json)
        return report_df
