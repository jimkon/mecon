from flask import Blueprint, render_template, request, redirect, url_for
from mecon2.data.db_controller import data_access
from mecon2.tagging import Tag
from mecon2.transactions import Transactions

tags_bp = Blueprint('tags', __name__, template_folder='templates')


def get_transactions() -> Transactions:
    data_df = data_access.transactions.get_transactions().sort_values(by='datetime', ascending=False).reset_index(drop=True)
    transactions = Transactions(data_df)
    return transactions


def render_tag_page(title='Tag page',
                    tag_name='',
                    tag_json_str=None,
                    rename_flag=False,
                    rename_button_flag=False,
                    message_text=''):
    title = 'Create a new tag'
    tag_json_str = '{}' if tag_json_str is None else tag_json_str
    # tag_json_str = '{"amount.int": {"greater": 1000}}'
    tag = Tag.from_json_string(tag_name, tag_json_str)
    transactions = get_transactions().apply_rule(tag.rule)
    data_df = transactions.filter_by().dataframe()
    table_html = None if data_df is None else data_df.to_html()
    number_of_rows = 0 if data_df is None else len(data_df)
    return render_template('tag_page.html', **locals(), **globals())


@tags_bp.route('/')
def tags_menu():
    try:
        all_tags = sorted(data_access.tags.all_tags(), key=lambda x: x['name'])
    except Exception as e:
        all_tags = f"Error: {e}"
    else:
        for tag in all_tags:
            tag['n_rows'] = 0

    return render_template('tags_menu.html', **locals(), **globals())


@tags_bp.route('/new', methods = ['POST', 'GET'])
def tags_new():
    rename_flag = True
    if request.method == 'POST':
        if "refresh" in request.form:
            tag_json_str = request.form.get('query_text_input')
        elif "reset" in request.form:
            tag_json_str = None
        elif "save" in request.form or "save_and_close" in request.form:
            tag_name = request.form.get('tag_name_input')
            tag_json_str = request.form.get('query_text_input')

            if data_access.tags.get_tag(tag_name) is not None:
                message_text = f"Tag '{tag_name}' already exists. Please use another name"
            else:
                try:
                    data_access.tags.set_tag(tag_name, tag_json_str)
                except Exception as e:
                    message_text = f"Error: {e}"
                else:
                    if "save_and_close" in request.form:
                        return redirect(url_for('tags.tag_info', tag_name=tag_name))
                    else:
                        return redirect(url_for('tags.tag_edit_get', tag_name=tag_name))

    return render_tag_page(title='Create a new tag', **locals())


@tags_bp.get('/edit/<tag_name>')
def tag_edit_get(tag_name):
    return f'tags_edit_get {tag_name=}'


@tags_bp.post('/edit/<tag_name>')
def tag_edit_post(tag_name):
    return f'tags_edit_post {tag_name=}'

@tags_bp.route('/info/<tag_name>', methods = ['POST', 'GET'])
def tag_info(tag_name):
    return f'tags_info {tag_name=}'

