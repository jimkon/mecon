import json

from flask import Blueprint, render_template, request, redirect, url_for

from mecon2.data.db_controller import data_access, reset_tags
from mecon2.tagging import Tag
from mecon2.transactions import Transactions

tags_bp = Blueprint('tags', __name__, template_folder='templates')


def _json_from_str(json_str):
    _json = json.loads(json_str)
    return _json


def _reformat_json_str(json_str):
    json_str = json_str.replace("'", '"')
    json_str = json.dumps(json.loads(json_str), indent=4)
    return json_str


def get_transactions() -> Transactions:
    data_df = data_access.transactions.get_transactions().sort_values(by='datetime', ascending=False).reset_index(drop=True)
    transactions = Transactions(data_df)
    return transactions


def recalculate_tags():
    transactions = get_transactions().reset_tags()

    for tag_dict in data_access.tags.all_tags():
        curr_tag_name, curr_tag_json = tag_dict['name'], tag_dict['conditions_json']
        tag = Tag.from_json_string(curr_tag_name, curr_tag_json)
        print(f"Applying {curr_tag_name} tag to transaction.")
        transactions = transactions.apply_tag(tag)

    data_df = transactions.dataframe()
    print(f"Updating transactions in DB.")
    data_access.transactions.update_tags(data_df)


def save_and_recalculate_tags(tag_name, tag_json_str):
    data_access.tags.set_tag(tag_name, _json_from_str(tag_json_str))
    recalculate_tags()


def render_tag_page(title='Tag page',
                    tag_name='',
                    tag_json_str='[{"description":{"contains":"something"}}]',
                    create_flag=False,
                    rename_flag=False,
                    rename_button_flag=False,
                    message_text='',
                    delete_button=False,
                    confirm_delete=False):
    try:
        # _json_from_str(tag_json_str)
        tag = Tag.from_json_string(tag_name, tag_json_str)
        transactions = get_transactions().apply_rule(tag.rule)
        data_df = transactions.dataframe()
        tagged_table_html = data_df.to_html()  # TODO add also the other untagged rows
        untagged_table_html = get_transactions().apply_negated_rule(tag.rule).dataframe().to_html()
        number_of_rows = 0 if data_df is None else len(data_df)
        tag_json_str = _reformat_json_str(tag_json_str)
    except json.decoder.JSONDecodeError as json_error:
        message_text = f"JSON Syntax error: {json_error}"
    except Exception as e:
        message_text = f"General exception: {e}"

    return render_template('tag_page.html', **locals(), **globals())


@tags_bp.route('/')
def tags_menu():
    try:
        all_tags = sorted(data_access.tags.all_tags(), key=lambda x: x['name'])
    except Exception as e:
        all_tags = f"Error: {e}"
    else:
        transactions = get_transactions()
        for tag in all_tags:
            tag['n_rows'] = transactions.contains_tag(tag['name']).size()

    return render_template('tags_menu.html', **locals(), **globals())


@tags_bp.route('/new', methods=['POST', 'GET'])
def tags_new():
    rename_flag = True
    if request.method == 'POST':
        if "refresh" in request.form:
            tag_name = request.form.get('tag_name_input')
            tag_json_str = request.form.get('query_text_input')
            tag_json_str = _reformat_json_str(tag_json_str)
        elif "reset" in request.form:
            tag_json_str = None

        elif "save" in request.form or "save_and_close" in request.form:
            tag_name = request.form.get('tag_name_input')
            tag_json_str = request.form.get('query_text_input')

            if data_access.tags.get_tag(tag_name) is not None:
                message_text = f"Tag '{tag_name}' already exists. Please use another name"
            else:
                try:
                    save_and_recalculate_tags(tag_name, tag_json_str)
                except Exception as e:
                    message_text = f"Error: {e}"
                else:
                    if "save_and_close" in request.form:
                        return redirect(url_for('tags.tags_menu'))
                    else:
                        return redirect(url_for('tags.tag_edit', tag_name=tag_name))

    return render_tag_page(title='Create a new tag', create_flag=True, **locals())


@tags_bp.route('/edit/<tag_name>', methods=['POST', 'GET'])
def tag_edit(tag_name):
    rename_flag = False
    delete_button = True
    # rename_button_flag = True
    if request.method == 'POST':
        if "refresh" in request.form:
            tag_json_str = request.form.get('query_text_input')
            tag_json_str = _reformat_json_str(tag_json_str)

        elif "reset" in request.form:
            tag_json_str = data_access.tags.get_tag(tag_name)['conditions_json']
        elif "save" in request.form or "save_and_close" in request.form:
            tag_json_str = request.form.get('query_text_input')

            try:
                save_and_recalculate_tags(tag_name, tag_json_str)
            except Exception as e:
                message_text = f"Error: {e}"
            else:
                if "save_and_close" in request.form:
                    return redirect(url_for('tags.tags_menu'))
                else:
                    return redirect(url_for('tags.tag_edit', tag_name=tag_name))

        elif "delete" in request.form:
            confirm_delete = True
            tag_json_str = request.form.get('query_text_input')
            render_tag_page(title=f'Edit tag "{tag_name}"', **locals())

        elif "confirm_delete" in request.form:
            data_access.tags.delete_tag(tag_name)
            return redirect(url_for('tags.tags_menu'))

    else:
        tag_json_str = data_access.tags.get_tag(tag_name)['conditions_json']
    return render_tag_page(title=f'Edit tag "{tag_name}"', **locals())


@tags_bp.post('/edit/<tag_name>')
def tag_edit_post(tag_name):
    return f'tags_edit_post {tag_name=}'


@tags_bp.post('/reset')
def tags_reset():
    reset_tags()
    return redirect(url_for('tags.tags_menu'))


@tags_bp.post('/recalculate')
def tags_recalculate():
    recalculate_tags()
    return redirect(url_for('tags.tags_menu'))



