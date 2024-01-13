import logging
import re

import requests
from flask import Blueprint, render_template, request, url_for
from json2html import json2html

import monitoring.logging_utils
from mecon.app.blueprints.reports import graphs
from mecon.app.data_manager import GlobalDataManager
from mecon.data import reports
from mecon.data.aggregators import CustomisableAmountTransactionAggregator
from mecon.data.groupings import LabelGrouping
from mecon.data.transactions import Transactions
from mecon.utils import html_pages, calendar_utils

reports_bp = Blueprint('reports', __name__, template_folder='templates')

_data_manager = GlobalDataManager()


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


@monitoring.logging_utils.codeflow_log_wrapper('#data#transactions#load')
def get_transactions() -> Transactions:
    transactions = _data_manager.get_transactions()
    return transactions


@monitoring.logging_utils.codeflow_log_wrapper('#data#transactions#process')
def get_filtered_transactions(start_date, end_date, tags_str, grouping_key, aggregation_key,
                              fill_dates_before_groupagg=False,
                              fill_dates_after_groupagg=False) -> Transactions:
    tags = _split_tags(tags_str)
    transactions = get_transactions().containing_tag(tags)

    transactions = transactions.select_date_range(start_date, end_date)

    if grouping_key != 'none':
        if fill_dates_before_groupagg:
            transactions = transactions.fill_values(grouping_key)

        transactions = transactions.groupagg(
            LabelGrouping.from_key(grouping_key),
            CustomisableAmountTransactionAggregator(aggregation_key, grouping_key)
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
        if transactions is None:
            transactions_start_date, transactions_end_date = None, None
        else:
            transactions_start_date, transactions_end_date = transactions.date_range()
        start_date = transactions_start_date if start_date is None else start_date
        end_date = transactions_end_date if end_date is None else end_date

        transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, aggregation)

    logging.info(
        f"get_filter_values -> {transactions.size() if transactions else 0=} {start_date=}, {end_date=}, {tags_str=}, {grouping=}, {aggregation=}")
    return transactions, start_date, end_date, tags_str, grouping, aggregation


@reports_bp.route('/')
@monitoring.logging_utils.codeflow_log_wrapper('#api')
def reports_menu():
    return 'reports menu'


@reports_bp.route('/graph/amount_freq')
@monitoring.logging_utils.codeflow_log_wrapper('#api')
def amount_freq_timeline_graph():
    start_date = request.args['start_date']
    end_date = request.args['end_date']
    tags_str = request.args['tags_str']
    grouping = request.args['grouping']
    # aggregation = request.args['aggregation']

    total_amount_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'sum',
                                                          fill_dates_after_groupagg=False)

    total_amount_transactions = total_amount_transactions.fill_values(grouping if grouping != 'none' else 'day')

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


@reports_bp.route('/graph/balance')
@monitoring.logging_utils.codeflow_log_wrapper('#api')
def balance_graph():
    start_date = request.args['start_date']
    end_date = request.args['end_date']
    tags_str = request.args['tags_str']
    grouping = request.args['grouping']
    # aggregation = request.args['aggregation']

    total_amount_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'sum')

    graph_html = graphs.balance_graph_html(
        total_amount_transactions.datetime,
        total_amount_transactions.amount,
    )

    return graph_html


@reports_bp.route('/graph/histogram')
@monitoring.logging_utils.codeflow_log_wrapper('#api')
def histogram_graph():
    start_date = request.args['start_date']
    end_date = request.args['end_date']
    tags_str = request.args['tags_str']
    grouping = request.args['grouping']
    # aggregation = request.args['aggregation']

    total_amount_transactions = get_filtered_transactions(start_date, end_date, tags_str, grouping, 'sum')

    graph_html = graphs.histogram_and_contributions(
        total_amount_transactions.amount,
    )

    return graph_html


@reports_bp.route(
    '/graph/tags_split')
@monitoring.logging_utils.codeflow_log_wrapper('#api')
def tags_split_graph():
    start_date = request.args['start_date']
    end_date = request.args['end_date']
    tags_str = request.args['tags_str']
    grouping = request.args['grouping']
    # aggregation = request.args['aggregation']
    tags_split_str = request.args['tags_split_str']
    split_tags = tags_split_str.split(',')

    timeline = None
    tag_total_amounts_list = []
    for tag in split_tags:
        tags = f"{tags_str},{tag}"
        total_amount_transactions = get_filtered_transactions(start_date, end_date, tags, grouping, 'sum')
        if grouping != 'none':
            total_amount_transactions = total_amount_transactions.fill_values(fill_unit=grouping,
                                                                              start_date=calendar_utils.to_date(start_date),
                                                                              end_date=calendar_utils.to_date(end_date))

        amount_series = total_amount_transactions.amount
        amount_series.name = tag
        timeline = total_amount_transactions.datetime
        # breakpoint()
        tag_total_amounts_list.append(amount_series)

    graph_html = graphs.lines_graph_html(
        timeline,
        tag_total_amounts_list,
    )

    return graph_html


@reports_bp.route(
    '/custom_graph/<plot_type>',
    methods=['POST', 'GET'])
@monitoring.logging_utils.codeflow_log_wrapper('#api')
def custom_graph(plot_type):
    start_date = request.args['start_date']
    end_date = request.args['end_date']
    tags_str = request.args['tags_str']
    grouping = request.args['grouping']
    aggregation = request.args['aggregation']

    filter_kwargs = request.args.copy()

    transactions, start_date, end_date, tags_str, grouping, aggregation = get_filter_values('', **filter_kwargs)
    # start_date=start_date,
    # end_date=end_date,
    # tags_str=tags_str,
    # grouping=grouping,
    # aggregation=aggregation)
    filter_kwargs['start_date'] = start_date
    filter_kwargs['end_date'] = end_date
    filter_kwargs['tags_str'] = tags_str
    filter_kwargs['grouping'] = grouping
    filter_kwargs['aggregation'] = aggregation

    # if statement here is for security (so we cannot call any function by changing the url)
    if plot_type == 'amount_freq':
        url = 'reports.amount_freq_timeline_graph'
    elif plot_type == 'balance':
        url = 'reports.balance_graph'
    elif plot_type == 'histogram':
        url = 'reports.histogram_graph'
    elif plot_type == 'tags_split':
        url = 'reports.tags_split_graph'
    else:
        raise ValueError(f"Unknown plot type ({plot_type}) requested.")

    graph_html = fetch_graph_html(url_for(url, **filter_kwargs))

    return render_template('custom_graph.html', **locals(), **globals())


@reports_bp.route('/tag_info/<tag_name>', methods=['POST', 'GET'])
@monitoring.logging_utils.codeflow_log_wrapper('#api')
def tag_info(tag_name):
    transactions, start_date, end_date, tags_str, grouping, aggregation = get_filter_values(tag_name)

    if transactions is not None:
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
    else:
        table_html = "<h4>'No transaction data left after filtering'</h4>"
        transactions_stats_json = "<h4>'No stats'</h4>"
        graph_html = "<h4>'No data to plot'</h4>"
    return render_template('tag_info.html', **locals(), **globals())


@reports_bp.route('/overall', methods=['POST', 'GET'])
@monitoring.logging_utils.codeflow_log_wrapper('#api')
def overall_report():
    # percentages
    transactions, start_date, end_date, tags_str, grouping, aggregation = get_filter_values('All')

    html_tabs = html_pages.TabsHTML()
    _graph = fetch_graph_html(url_for('reports.tags_split_graph',
                                      start_date=start_date,
                                      end_date=end_date,
                                      tags_str=tags_str,
                                      grouping=grouping,
                                      tags_split_str='MoneyIn,MoneyOut'))
    html_tabs.add_tab('Money In/Out', _graph)

    bank_in_graph = fetch_graph_html(url_for('reports.tags_split_graph',
                                      start_date=start_date,
                                      end_date=end_date,
                                      tags_str=tags_str+',MoneyIn',
                                      grouping=grouping,
                                      tags_split_str='Revolut,HSBC,Monzo'))

    bank_out_graph = fetch_graph_html(url_for('reports.tags_split_graph',
                                      start_date=start_date,
                                      end_date=end_date,
                                      tags_str=tags_str + ',MoneyOut',
                                      grouping=grouping,
                                      tags_split_str='Revolut,HSBC,Monzo'))
    html_tabs.add_tab('Bank', f"<div><h2>Money in</h2>{bank_in_graph}<br><h2>Money out</h2>{bank_out_graph}</div>")

    _graph = fetch_graph_html(url_for('reports.tags_split_graph',
                                      start_date=start_date,
                                      end_date=end_date,
                                      tags_str=tags_str,
                                      grouping=grouping,
                                      tags_split_str='Income,ITV income,Deloitte income'))
    html_tabs.add_tab('Income', _graph)

    _graph = fetch_graph_html(url_for('reports.tags_split_graph',
                                      start_date=start_date,
                                      end_date=end_date,
                                      tags_str=tags_str,
                                      grouping=grouping,
                                      tags_split_str='Commute,TFL,BorisBike,LimeBike,Uber Taxi'))
    html_tabs.add_tab('Commute', _graph)

    _graph = fetch_graph_html(url_for('reports.tags_split_graph',
                                      start_date=start_date,
                                      end_date=end_date,
                                      tags_str=tags_str,
                                      grouping=grouping,
                                      tags_split_str='Food,Food delivery,Super Market,Food out'))
    html_tabs.add_tab('Food', _graph)

    graph_html = html_tabs.html()

    return render_template('overall_report.html', **locals(), **globals())
