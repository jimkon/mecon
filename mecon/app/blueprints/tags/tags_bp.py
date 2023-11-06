import json
import logging

from flask import Blueprint, render_template, request, redirect, url_for

import tag_tools
from tag_tools import comparisons, transformations
from mecon.import_data.db_controller import data_access, reset_tags
from mecon.tag_tools.tagging import Tag
from data.transactions import Transactions
from mecon.monitoring import logs

tags_bp = Blueprint('tags', __name__, template_folder='templates')


def _json_from_str(json_str):
    _json = json.loads(json_str)
    return _json


def _reformat_json_str(json_str):
    json_str = json_str.replace("'", '"')
    json_str = json.dumps(json.loads(json_str), indent=4)
    return json_str


@logs.codeflow_log_wrapper('#import_data#transactions#load')
def get_transactions() -> Transactions:
    data_df = data_access.transactions.get_transactions()
    transactions = Transactions(data_df)
    return transactions


@logs.codeflow_log_wrapper('#import_data#tags#process')
def recalculate_tags():
    transactions = get_transactions().reset_tags()

    for tag_dict in data_access.tags.all_tags():
        curr_tag_name, curr_tag_json = tag_dict['name'], tag_dict['conditions_json']
        tag = Tag.from_json_string(curr_tag_name, curr_tag_json)
        logging.info(f"Applying {curr_tag_name} tag to transaction.")
        transactions = transactions.apply_tag(tag)

    data_df = transactions.dataframe()
    logging.info(f"Updating transactions in DB.")
    data_access.transactions.update_tags(data_df)


@logs.codeflow_log_wrapper('#import_data#tags#store')
def save_and_recalculate_tags(tag_name, tag_json_str):
    data_access.tags.set_tag(tag_name, _json_from_str(tag_json_str))
    recalculate_tags()


def render_tag_page(title='Tag page',
                    tag_name='',
                    tag_json_str='[{"description":{"contains":"something"}}]',
                    message_text='',
                    confirm_delete=False):
    # TODO:v3 maybe add rename function

    try:
        tag = Tag.from_json_string(tag_name, tag_json_str)
        transactions = get_transactions().apply_rule(tag.rule)
        tagged_table_html = transactions.to_html()
        untagged_transactions = get_transactions().apply_negated_rule(tag.rule)
        untagged_table_html = untagged_transactions.to_html()
        tag_json_str = _reformat_json_str(tag_json_str)
        transformations_list = [trans.name for trans in transformations.TransformationFunction.all_instances()]
        comparisons_list = [comp.name for comp in comparisons.CompareOperator.all_instances()]

        # del untagged_transactions
    except json.decoder.JSONDecodeError as json_error:
        message_text = f"JSON Syntax error: {json_error}"
    # except Exception as e:
    #     message_text = f"General exception: {e}"

    return render_template('tag_page.html', **locals(), **globals())


@tags_bp.route('/', methods=['POST', 'GET'])
@logs.codeflow_log_wrapper('#api')
def tags_menu():
    if request.method == 'POST':
        if "recalculate_tags" in request.form:
            recalculate_tags()
        elif "reset_tags" in request.form:
            reset_tags()
        elif "create_tag_button" in request.form:
            tag_name = request.form.get('tag_name_input')
            tag_json_str = '[{"description":{"contains":"something"}}]'
            if len(tag_name) == 0:
                create_tag_error = f"Tag name cannot not be empty."
            elif data_access.tags.get_tag(tag_name) is not None:
                create_tag_error = f"Tag '{tag_name}' already exists. Please use another name."
            else:
                data_access.tags.set_tag(tag_name, _json_from_str(tag_json_str))
                return redirect(url_for('tags.tag_edit', tag_name=tag_name))

    try:
        all_tags = sorted(data_access.tags.all_tags(), key=lambda x: x['name'])
    except Exception as e:
        all_tags = f"Error: {e}"
    else:
        transactions = get_transactions()
        for tag in all_tags:
            tag['n_rows'] = transactions.containing_tag(tag['name']).size()

    return render_template('tags_menu.html', **locals(), **globals())


@tags_bp.route('/edit/<tag_name>', methods=['POST', 'GET'])
@logs.codeflow_log_wrapper('#api')
def tag_edit(tag_name):
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
                return redirect(url_for('tags.tag_edit', tag_name=tag_name))
        elif "delete" in request.form:
            confirm_delete = True
            tag_json_str = request.form.get('query_text_input')
            render_tag_page(title=f'Edit tag "{tag_name}"', **locals())

        elif "confirm_delete" in request.form:
            data_access.tags.delete_tag(tag_name)
            try:
                recalculate_tags()
            except Exception as e:
                message_text = f"Error: {e}"
            else:
                return redirect(url_for('tags.tags_menu'))
        elif "add_condition" in request.form:
            disjunction = tag_tools.Disjunction.from_json(_json_from_str(request.form.get('query_text_input')))

            condition = tag_tools.Condition.from_string_values(
                field=request.form['field'],
                transformation_op_key=request.form['transform'],
                compare_op_key=request.form['comparison'],
                value=request.form['value'],
            )

            new_tag_json = disjunction.append(condition).to_json()
            tag_json_str = _reformat_json_str(json.dumps(new_tag_json))
            del condition, disjunction, new_tag_json  # otherwise **locals() will break render_tag_page
        elif "add_id" in request.form:
            id = int(request.form['input_id'])

            disjunction = tag_tools.Disjunction.from_json(_json_from_str(request.form.get('query_text_input')))

            condition = tag_tools.Condition.from_string_values(
                field='id',
                transformation_op_key="none",
                compare_op_key="equal",
                value=id,
            )
            rows = get_transactions().apply_rule(condition).dataframe().to_dict('records')
            if len(rows) > 1:
                message_text = f"More than one ({len(rows)}) have the same id ({id})"
                tag_json_str = request.form.get('query_text_input')
            else:
                row = rows[0]

                conj = tag_tools.Conjunction([
                    tag_tools.Condition.from_string_values('datetime', 'str', 'equal', str(row['datetime'])),
                    tag_tools.Condition.from_string_values('amount', 'none', 'equal', row['amount']),
                    tag_tools.Condition.from_string_values('currency', 'str', 'equal', row['currency']),
                    tag_tools.Condition.from_string_values('description', 'none', 'equal', row['description']),
                    # tag_tools.Condition.from_string_values(field, 'none', 'equal', value) for field, value in row.items()
                ])
                new_tag_json = disjunction.append(conj).to_json()
                tag_json_str = _reformat_json_str(json.dumps(new_tag_json))
                del condition, disjunction, new_tag_json, id, rows, row, conj  # otherwise **locals() will break render_tag_page

    else:
        tag_json_str = data_access.tags.get_tag(tag_name)['conditions_json']
    return render_tag_page(title=f'Edit tag "{tag_name}"', **locals())
