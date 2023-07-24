from flask import Blueprint, redirect, url_for, render_template

from mecon2.data.db_controller import data_access
from mecon2.tagging import Tag
from mecon2.transactions import Transactions

tags_bp = Blueprint('tags', __name__, template_folder='templates')


def get_transactions() -> Transactions:
    data_df = data_access.transactions.get_transactions().sort_values(by='datetime', ascending=False).reset_index(drop=True)
    transactions = Transactions(data_df)
    return transactions


def render_tag_page(title='Tag page', tag_name='', tag_json_str="{}", rename_flag=False):
    title = 'Create a new tag'
    rename_flag = False
    tag_name = 'test_tag'
    tag_json_str = '{"amount.int": {"greater": 1000}}'
    # tag_json_str = '{}'
    tag = Tag.from_json_string(tag_name, tag_json_str)
    transactions = get_transactions().apply_rule(tag.rule)
    data_df = transactions.filter_by().dataframe()
    table_html = None if data_df is None else data_df.to_html()
    number_of_rows = 0 if data_df is None else len(data_df)
    return render_template('tag_page.html', **locals(), **globals())


@tags_bp.route('/')
def tags_menu():
    return render_template('tags_menu.html', **locals(), **globals())


@tags_bp.route('/new')
def tags_new():
    return render_tag_page(title='Create a new tag')


@tags_bp.get('/edit/<tag_name>')
def tags_edit_get(tag_name):
    return f'tags_edit_get {tag_name=}'


@tags_bp.post('/edit/<tag_name>')
def tags_edit_post(tag_name):
    return f'tags_edit_post {tag_name=}'
