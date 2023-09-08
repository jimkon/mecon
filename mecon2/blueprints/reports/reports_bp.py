from flask import Blueprint, render_template, request, redirect, url_for
from json2html import json2html
import re
import requests

from mecon2.blueprints.reports import graphs
from mecon2.transactions import Transactions
from mecon2.data.db_controller import data_access, reset_tags
from mecon2 import reports
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


def get_transactions() -> Transactions:
    data_df = data_access.transactions.get_transactions().sort_values(by='datetime', ascending=False).reset_index(
        drop=True)
    transactions = Transactions(data_df)
    return transactions


def get_filtered_transactions(start_date, end_date, tags_str, grouping, aggregation):
    tags = _split_tags(tags_str)
    transactions = get_transactions() \
        .contains_tag(tags) \
        .select_date_range(start_date, end_date)
    return transactions


def get_filter_values(tag_name):
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        tags_str = request.form['tags_text_box']
        tags = _split_tags(tags_str).union({tag_name})
        tags_str = ','.join(tags)
        grouping = request.form['groups']
        aggregation = request.form['aggregations'] if grouping != 'none' else 'none'

        transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, aggregation)
    else:  # if request.method == 'GET':
        tags = {tag_name}
        transactions = get_transactions().contains_tag(tag_name)
        start_date, end_date = transactions.date_range()
        grouping = 'none'
        aggregation = 'none'

    tags_str = ', '.join(sorted(_filter_tags(tags)))

    return transactions, start_date, end_date, tags_str, grouping, aggregation


@reports_bp.route('/')
def reports_menu():
    return 'reports menu'


@reports_bp.route('/graph/<plot_type>/dates:<start_date>_<end_date>,tags:<tags_str>,group:<grouping>,agg:<aggregation>')
def graph(plot_type, start_date, end_date, tags_str, grouping, aggregation):

    transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, aggregation)

    if plot_type == 'timeline':
        graph_html = graphs.timeline_fig(transactions, figsize=(10, 4))
    elif plot_type == 'balance':
        graph_html = graphs.balance_fig(transactions, figsize=(10, 4))
    elif plot_type == 'histogram':
        graph_html = graphs.histogram_fig(transactions, figsize=(10, 4))
    else:
        graph_html = graphs.timeline_fig(transactions, figsize=(10, 4))
    return graph_html


@reports_bp.route('/custom_graph/<plot_type>', methods=['POST', 'GET'])
def custom_graph(plot_type):
    transactions, start_date, end_date, tags_str, grouping, aggregation = get_filter_values('Tap')

    if plot_type == 'timeline':
        graph_html = graphs.timeline_fig(transactions)
    elif plot_type == 'balance':
        graph_html = graphs.balance_fig(transactions)
    elif plot_type == 'histogram':
        graph_html = graphs.histogram_fig(transactions)
    else:
        graph_html = graphs.timeline_fig(transactions)
    return render_template('custom_graph.html', **locals(), **globals())


@reports_bp.route('/tag_info/<tag_name>', methods=['POST', 'GET'])
def tag_info(tag_name):
    transactions, start_date, end_date, tags_str, grouping, aggregation = get_filter_values(tag_name)

    def custom_graph_url_and_html(plot_type):  # TODO asyncio
        title = plot_type.capitalize()
        rel_graph_url = url_for('reports.graph',
                                plot_type=plot_type,
                                start_date=start_date,
                                end_date=end_date,
                                tags_str=tags_str,
                                grouping=grouping,
                                aggregation=aggregation)

        try:
            graph_url = f"http://127.0.0.1:5000{rel_graph_url}"
            response = requests.get(graph_url)
            response.raise_for_status()  # Raise an exception if there's an HTTP error
            graph_html_result = response.text
            custom_graph_url = f"http://127.0.0.1:5000{url_for('reports.custom_graph', plot_type=plot_type)}"
            href = f"<a href={custom_graph_url}>{title}</a>"
            return href, graph_html_result
        except requests.exceptions.RequestException as e:
            # Handle request-related exceptions, e.g., connection errors, timeout, etc.
            return title, f"Failed to fetch URL: {str(e)}"

    data_df = transactions.dataframe()
    table_html = data_df.to_html()
    transactions_stats_json = json2html.convert(json=reports.transactions_stats(transactions))

    html_tabs = html_pages.TabsHTML()
    for plot_type in ['timeline', 'balance', 'histogram']:
        _href, _graph = custom_graph_url_and_html(plot_type)
        html_tabs.add_tab(_href, _graph)

    graph_html = html_tabs.html()
    return render_template('tag_info.html', **locals(), **globals())
