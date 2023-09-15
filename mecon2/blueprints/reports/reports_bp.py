import io
import re

import requests
from flask import Blueprint, render_template, request, url_for
from json2html import json2html

from aggregators import CustomisedAmountTransactionAggregator
from mecon2.groupings import LabelGrouping
from mecon2 import reports
from mecon2.blueprints.reports import graphs
from mecon2.data.db_controller import data_access
from mecon2.transactions import Transactions
from mecon2.utils import html_pages

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
        return '#not implemented', graph_html_result
    except requests.exceptions.RequestException as e:
        # Handle request-related exceptions, e.g., connection errors, timeout, etc.
        return 'ERROR', f"Failed to fetch URL: {str(e)}"



def get_transactions() -> Transactions:
    data_df = data_access.transactions.get_transactions().sort_values(by='datetime', ascending=False).reset_index(
        drop=True)
    transactions = Transactions(data_df)
    return transactions


def get_filtered_transactions(start_date, end_date, tags_str, grouping_key, aggregation_key) -> Transactions:
    tags = _split_tags(tags_str)
    transactions = get_transactions() \
        .contains_tag(tags) \
        .select_date_range(start_date, end_date)

    if grouping_key != 'none':
        transactions = transactions.groupagg(
            LabelGrouping.from_key(grouping_key),
            CustomisedAmountTransactionAggregator(aggregation_key, grouping_key)
        )
    # TODO week agg doesn't work properly
    # TODO filling days/weeks/etc after grouping
    return transactions


def get_filter_values(tag_name):
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        tags_str = request.form['tags_text_box']
        tags = _split_tags(tags_str).union({tag_name})
        # tags_str = ','.join(tags)
        tags_str = ', '.join(sorted(_filter_tags(tags)))
        grouping = request.form['groups']
        aggregation = request.form['aggregations'] if grouping != 'none' else 'none'

        transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, aggregation)
    else:  # if request.method == 'GET':
        tags = {tag_name}
        tags_str = ', '.join(sorted(_filter_tags(tags)))
        grouping = 'month'
        aggregation = 'sum'
        transactions = get_transactions().contains_tag(tag_name)
        start_date, end_date = transactions.date_range()
        transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, aggregation)

    return transactions, start_date, end_date, tags_str, grouping, aggregation


@reports_bp.route('/')
def reports_menu():
    return 'reports menu'


@reports_bp.route('/graph/amount_freq/dates:<start_date>_<end_date>,tags:<tags_str>,group:<grouping>')
def amount_freq_timeline_graph(start_date, end_date, tags_str, grouping):
    total_amount_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'sum')

    if grouping != 'none':
        freq_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'count')
    else:
        freq_transactions = None

    graph_html = graphs.amount_and_freq_timeline_html(
        total_amount_transactions.datetime,
        total_amount_transactions.amount,
        freq_transactions.amount if freq_transactions is not None else None
    )

    return graph_html


@reports_bp.route('/graph/balance/dates:<start_date>_<end_date>,tags:<tags_str>,group:<grouping>')
def balance_graph(start_date, end_date, tags_str, grouping):
    total_amount_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'sum')

    graph_html = graphs.balance_graph_html(
        total_amount_transactions.datetime,
        total_amount_transactions.amount,
    )

    return graph_html


@reports_bp.route('/graph/histogram/dates:<start_date>_<end_date>,tags:<tags_str>,group:<grouping>')
def histogram_graph(start_date, end_date, tags_str, grouping):
    total_amount_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'sum')

    graph_html = graphs.histogram_graph_html(
        total_amount_transactions.amount,
    )

    return graph_html


# TODO redo
# @reports_bp.route('/custom_graph/<plot_type>', methods=['POST', 'GET'])
# def custom_graph(plot_type):
#     transactions, start_date, end_date, tags_str, grouping, aggregation = get_filter_values('Tap')
#
#     if plot_type == 'timeline':
#         graph_html = graphs.timeline_fig(transactions)
#     elif plot_type == 'balance':
#         graph_html = graphs.balance_fig(transactions)
#     elif plot_type == 'histogram':
#         graph_html = graphs.histogram_fig(transactions)
#     elif plot_type == 'frequency':
#         graph_html = amount_freq_timeline_graph(start_date, end_date, tags_str, grouping)
#     else:
#         raise ValueError(f"Unknown plot type ({plot_type}) requested.")
#     return render_template('custom_graph.html', **locals(), **globals())


@reports_bp.route('/tag_info/<tag_name>', methods=['POST', 'GET'])
def tag_info(tag_name):
    transactions, start_date, end_date, tags_str, grouping, aggregation = get_filter_values(tag_name)


    data_df = transactions.dataframe()
    table_html = data_df.to_html()
    transactions_stats_json = json2html.convert(json=reports.transactions_stats(transactions))

    html_tabs = html_pages.TabsHTML()
    _href, _graph = fetch_graph_html(url_for('reports.amount_freq_timeline_graph',
                                             start_date=start_date,
                                             end_date=end_date,
                                             tags_str=tags_str,
                                             grouping=grouping))
    html_tabs.add_tab('Amount-Freq', _graph)

    _href, _graph = fetch_graph_html(url_for('reports.balance_graph',
                                             start_date=start_date,
                                             end_date=end_date,
                                             tags_str=tags_str,
                                             grouping=grouping))
    html_tabs.add_tab('Balance', _graph)

    _href, _graph = fetch_graph_html(url_for('reports.histogram_graph',
                                             start_date=start_date,
                                             end_date=end_date,
                                             tags_str=tags_str,
                                             grouping=grouping))
    html_tabs.add_tab('Histogram', _graph)

    graph_html = html_tabs.html()
    return render_template('tag_info.html', **locals(), **globals())
