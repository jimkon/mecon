from flask import Blueprint, render_template, request, redirect, url_for
from json2html import json2html
import re

from mecon2.transactions import Transactions
from mecon2.data.db_controller import data_access, reset_tags
from mecon2 import reports


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
    data_df = data_access.transactions.get_transactions().sort_values(by='datetime', ascending=False).reset_index(drop=True)
    transactions = Transactions(data_df)
    return transactions


def get_filtered_transactions(tag_name):
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        tags_str = request.form['tags_text_box']
        tags = _split_tags(tags_str).union({tag_name})
        transactions = get_transactions().contains_tag(tags)
    else:  # if request.method == 'GET':
        tags = tag_name
        transactions = get_transactions().contains_tag(tag_name)
        start_date, end_date = transactions.date_range()

    tags_str = ', '.join(sorted(_filter_tags(tags)))
    return transactions, start_date, end_date, tags_str


@reports_bp.route('/')
def reports_menu():
    return 'reports menu'


@reports_bp.route('/custom_graph')
def custom_graph():
    return render_template('data_filter.html', **locals(), **globals())


@reports_bp.route('/tag_info/<tag_name>', methods=['POST', 'GET'])
def tag_info(tag_name):
    transactions, start_date, end_date, tags = get_filtered_transactions(tag_name)
    data_df = transactions.dataframe()
    table_html = data_df.to_html()
    transactions_stats_json = json2html.convert(json=reports.transactions_stats(transactions))
    return render_template('tag_info.html', **locals(), **globals())
    # return redirect(url_for('tags.tag_edit', tag_name=tag_name))  # TODO change when info page is built





