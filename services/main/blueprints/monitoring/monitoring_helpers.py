import pandas as pd

from mecon.utils.dataframe_transformers import DataframeTransformer
from mecon.tag_tools import tagging
from mecon.monitoring.log_data import PerformanceData


def performance_stats_dict(perf_data: PerformanceData):
    tag_perf_stats = {}

    for tag in perf_data.all_tags().keys():
        rule = tagging.TagMatchCondition(tag)
        tag_perf_data = perf_data.apply_rule(rule)
        non_zero_perf_data = tag_perf_data.finished()
        tag_perf_stats[tag] = non_zero_perf_data.execution_time

    stats = {}
    total_time = 0

    for func, times in tag_perf_stats.items():
        total_time += times.sum()

        stats[func] = {
            'Min': times.min(),
            'Average': times.mean(),
            'Max': times.max(),
            'Total': times.sum(),
            'Count': len(times),
        }

    # Calculate percentage of total execution time
    for func, times in tag_perf_stats.items():
        stats[func]['Percentage'] = (times.sum() / total_time) * 100

    return stats