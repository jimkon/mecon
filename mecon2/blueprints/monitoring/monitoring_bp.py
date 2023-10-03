import pathlib

import pandas as pd
from flask import Blueprint, render_template, request

from mecon2 import tagging
from mecon2.monitoring.logs import get_log_files, read_logs_as_df
from mecon2.utils import html_pages
from mecon2.monitoring.log_data import LogData, PerformanceData
from mecon2.blueprints.reports import graphs

monitoring_bp = Blueprint('monitoring', __name__, template_folder='templates')


def get_logs(selected_log_file=None):
    if request.method == 'POST':
        selected_log_file = request.form['log_files']
    else:  # if request.method == 'GET':
        selected_log_file = 'logs\logs_raw.csv' if selected_log_file is None else selected_log_file

    log_files = get_log_files(historic_logs=True) if selected_log_file == 'All' else [pathlib.Path(selected_log_file)]
    df_logs = read_logs_as_df(log_files)

    log_data = LogData.from_raw_logs(df_logs)

    return log_data, selected_log_file


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


def performance_stats_page(perf_data: PerformanceData):
    perf_stats = performance_stats_dict(perf_data)
    graph_html = graphs.performance_stats_graph_html(perf_stats)
    table_html = pd.DataFrame.from_dict(perf_stats, orient='index').sort_values('Total', ascending=False).to_html()
    res_html = f"""{graph_html}<br><br>{table_html}"""
    return res_html


def codeflow_timeline_graph_html(perf_data: PerformanceData):
    graph_html = graphs.codeflow_timeline_graph_html(perf_data.functions, perf_data.start_datetime,
                                        perf_data.end_datetime)
    table_html = perf_data.dataframe().sort_values('datetime', ascending=False).to_html()
    res_html = f"""{graph_html}<br><br>{table_html}"""
    return res_html


@monitoring_bp.route('/', methods=['POST', 'GET'])
def home():
    all_log_filenames = [str(file) for file in get_log_files(historic_logs=True)]
    log_data, selected_log_file = get_logs()
    table_html = log_data.dataframe().sort_values(['datetime'], ascending=False).to_html()
    perf_data = PerformanceData.from_log_data(log_data=log_data)

    tabs = html_pages.TabsHTML() \
        .add_tab('Performance', performance_stats_page(perf_data)) \
        .add_tab('Timeline', codeflow_timeline_graph_html(perf_data)) \
        .add_tab('Console', table_html).html()
    return render_template('monitoring_home.html', **locals(), **globals())
