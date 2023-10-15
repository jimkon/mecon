import logging
import re

import requests
from flask import Blueprint, render_template, request, url_for
from json2html import json2html

from aggregators import CustomisedAmountTransactionAggregator
from mecon2 import reports
from mecon2.blueprints.reports import graphs
from mecon2.data.db_controller import data_access
from mecon2.groupings import LabelGrouping
from mecon2.transactions import Transactions
from mecon2.utils import html_pages
from mecon2.monitoring import logs

reports_bp = Blueprint('reports', __name__, template_folder='templates')


def _split_tags(input_string):
    # return input_string.replace(' ', '').split(',')

    # Define the regex pattern to match words separated by commas and optional whitespace
    pattern = r'\s*,\s*'
    # Use re.split() to split the input_string based on the pattern
    words = re.split(pattern, input_string)
    return set(words)


def _filter_tags(tags_set):
    if '' in tags_set:
        tags_set.remove('')
    return tags_set


def fetch_graph_html(graph_url):
    try:
        graph_url = f"http://127.0.0.1:5000{graph_url}"
        response = requests.get(graph_url)
        response.raise_for_status()  # Raise an exception if there's an HTTP error
        graph_html_result = response.text
        return graph_html_result
    except requests.exceptions.RequestException as e:
        # Handle request-related exceptions, e.g., connection errors, timeout, etc.
        return f"Failed to fetch URL: {str(e)}"


def produce_href_for_custom_graph(plot_type, start_date=None, end_date=None, tags_str=None, grouping_key=None,
                                  aggregation_key=None):
    title = plot_type.replace('_', '-').capitalize()
    rel_url = url_for('reports.custom_graph',
                      plot_type=plot_type,
                      start_date=start_date,
                      end_date=end_date,
                      tags_str=tags_str,
                      grouping=grouping_key,
                      aggregation=aggregation_key)
    url = f"http://127.0.0.1:5000{rel_url}"
    href = f"{title} <a href={url}><small><i>(more)</i></small></a>"
    return href


@logs.codeflow_log_wrapper('#data#transactions#load')
def get_transactions() -> Transactions:
    data_df = data_access.transactions.get_transactions()
    transactions = Transactions(data_df)
    return transactions


@logs.codeflow_log_wrapper('#data#transactions#process')
def get_filtered_transactions(start_date, end_date, tags_str, grouping_key, aggregation_key,
                              fill_dates_before_groupagg=False,
                              fill_dates_after_groupagg=False) -> Transactions:
    tags = _split_tags(tags_str)
    transactions = get_transactions() \
        .containing_tag(tags) \
        .select_date_range(start_date, end_date)

    if grouping_key != 'none':
        if fill_dates_before_groupagg:
            transactions = transactions.fill_values(grouping_key)

        transactions = transactions.groupagg(
            LabelGrouping.from_key(grouping_key),
            CustomisedAmountTransactionAggregator(aggregation_key, grouping_key)
        )

        if fill_dates_after_groupagg:
            transactions = transactions.fill_values(grouping_key)

    return transactions


def get_filter_values(tag_name, start_date=None, end_date=None, tags_str=None, grouping=None, aggregation=None):
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        tags_str = request.form['tags_text_box']
        tags = _split_tags(tags_str).union({tag_name})
        tags_str = ', '.join(sorted(_filter_tags(tags)))
        grouping = request.form['groups']
        aggregation = request.form['aggregations'] if grouping != 'none' else 'none'

        transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, aggregation)
    else:  # if request.method == 'GET':
        tags = {tag_name}
        tags_str = ', '.join(sorted(_filter_tags(tags))) if tags_str is None else tags_str
        grouping = 'month' if grouping is None else grouping
        aggregation = 'sum' if aggregation is None else aggregation
        transactions = get_transactions().containing_tag(tag_name)
        transactions_start_date, transactions_end_date = transactions.date_range()
        start_date = transactions_start_date if start_date is None else start_date
        end_date = transactions_end_date if end_date is None else end_date

        transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, aggregation)

    logging.info(
        f"get_filter_values -> {transactions.size()=} {start_date=}, {end_date=}, {tags_str=}, {grouping=}, {aggregation=}")
    return transactions, start_date, end_date, tags_str, grouping, aggregation


@reports_bp.route('/')
@logs.codeflow_log_wrapper('#api')
def reports_menu():
    return 'reports menu'


@reports_bp.route('/graph/amount_freq/dates:<start_date>_<end_date>,tags:<tags_str>,group:<grouping>')
@logs.codeflow_log_wrapper('#api')
def amount_freq_timeline_graph(start_date, end_date, tags_str, grouping):
    total_amount_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'sum',
                                                          fill_dates_after_groupagg=True)

    if grouping != 'none':
        freq_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'count',
                                                      fill_dates_after_groupagg=True)
    else:
        freq_transactions = None

    graph_html = graphs.amount_and_freq_timeline_html(
        total_amount_transactions.datetime,
        total_amount_transactions.amount,
        freq_transactions.amount if freq_transactions is not None else None
    )

    return graph_html


@reports_bp.route('/graph/balance/dates:<start_date>_<end_date>,tags:<tags_str>,group:<grouping>')
@logs.codeflow_log_wrapper('#api')
def balance_graph(start_date, end_date, tags_str, grouping):
    total_amount_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'sum')

    graph_html = graphs.balance_graph_html(
        total_amount_transactions.datetime,
        total_amount_transactions.amount,
    )

    return graph_html


@reports_bp.route('/graph/histogram/dates:<start_date>_<end_date>,tags:<tags_str>,group:<grouping>')
@logs.codeflow_log_wrapper('#api')
def histogram_graph(start_date, end_date, tags_str, grouping):
    total_amount_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'sum')

    graph_html = graphs.histogram_and_contributions(
        total_amount_transactions.amount,
    )

    return graph_html


@reports_bp.route(
    '/custom_graph/<plot_type>,dates:<start_date>_<end_date>,tags:<tags_str>,group:<grouping>,agg:<aggregation>',
    methods=['POST', 'GET'])
@logs.codeflow_log_wrapper('#api')
def custom_graph(plot_type, start_date, end_date, tags_str, grouping, aggregation):
    transactions, start_date, end_date, tags_str, grouping, aggregation = get_filter_values('Tap',
                                                                                            start_date=start_date,
                                                                                            end_date=end_date,
                                                                                            tags_str=tags_str,
                                                                                            grouping=grouping,
                                                                                            aggregation=aggregation)

    # if statement here is for security (so we cannot call any function by changing the url)
    if plot_type == 'amount_freq':
        url = 'reports.amount_freq_timeline_graph'
    elif plot_type == 'balance':
        url = 'reports.balance_graph'
    elif plot_type == 'histogram':
        url = 'reports.histogram_graph'
    else:
        raise ValueError(f"Unknown plot type ({plot_type}) requested.")

    graph_html = fetch_graph_html(url_for(url,
                                          start_date=start_date,
                                          end_date=end_date,
                                          tags_str=tags_str,
                                          grouping=grouping))

    return render_template('custom_graph.html', **locals(), **globals())


@reports_bp.route('/tag_info/<tag_name>', methods=['POST', 'GET'])
@logs.codeflow_log_wrapper('#api')
def tag_info(tag_name):
    transactions, start_date, end_date, tags_str, grouping, aggregation = get_filter_values(tag_name)

    data_df = transactions.dataframe()
    table_html = transactions.to_html()
    transactions_stats_json = json2html.convert(json=reports.transactions_stats(transactions))

    html_tabs = html_pages.TabsHTML()

    for _plot_type, _route in [
        ('amount_freq', 'reports.amount_freq_timeline_graph'),
        ('balance', 'reports.balance_graph'),
        ('histogram', 'reports.histogram_graph'),
    ]:
        _graph = fetch_graph_html(url_for(_route,
                                          start_date=start_date,
                                          end_date=end_date,
                                          tags_str=tags_str,
                                          grouping=grouping))
        _href = produce_href_for_custom_graph(plot_type=_plot_type,
                                              start_date=start_date,
                                              end_date=end_date,
                                              tags_str=tags_str,
                                              grouping_key=grouping,
                                              aggregation_key=aggregation
                                              )
        html_tabs.add_tab(_href, _graph)

    graph_html = html_tabs.html()
    return render_template('tag_info.html', **locals(), **globals())
