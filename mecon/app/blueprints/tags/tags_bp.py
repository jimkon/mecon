import json

from flask import Blueprint, render_template, request, redirect, url_for

from mecon.app.data_manager import DBDataManager
from mecon.data.transactions import Transactions
from mecon.monitoring import logs
from mecon.tag_tools import tagging, comparisons, transformations
from mecon.tag_tools.tagging import Tag
from mecon.app.app_utils import ManualTaggingHTMLTableFormat

tags_bp = Blueprint('tags', __name__, template_folder='templates')

_data_manager = DBDataManager()


def _reformat_json_str(json_str):
    json_str = json_str.replace("'", '"')
    json_str = json.dumps(json.loads(json_str), indent=4)
    return json_str


def _rule_for_id(_id):
    condition = tagging.Condition.from_string_values(
        field='id',
        transformation_op_key="none",
        compare_op_key="equal",
        value=_id,
    )
    transactions_with_id = get_transactions().apply_rule(condition)
    if transactions_with_id.size() > 1:
        # message_text = f"More than one ({len(transactions_with_id.size())}) have the same id ({_id})"
        # tag_json_str = request.form.get('query_text_input')
        raise ValueError(f"More than one ({len(transactions_with_id.size())}) have the same id ({_id})")
    else:
        df = transactions_with_id.dataframe()
        disjunction = tagging.Disjunction.from_dataframe(df, exclude_cols=['id', 'tags'])
        return disjunction


@logs.codeflow_log_wrapper('#data#transactions#load')
def get_transactions() -> Transactions:
    transactions = _data_manager.get_transactions()
    return transactions


def render_tag_page(title='Tag page',
                    tag_name='',
                    tag_json_str='[{"description":{"contains":"something"}}]',
                    message_text='',
                    confirm_delete=False,
                    **kwargs):
    # TODO:v3 maybe add rename function

    try:
        tag = Tag.from_json_string(tag_name, tag_json_str)
        transactions = get_transactions().apply_rule(tag.rule)
        tagged_table_html = transactions.to_html() if transactions else "<h4>'No transaction data after applying rule'</h4>"
        untagged_transactions = get_transactions().apply_negated_rule(tag.rule)
        untagged_table_html = untagged_transactions.to_html()
        tag_json_str = _reformat_json_str(tag_json_str)
        transformations_list = [trans.name for trans in transformations.TransformationFunction.all_instances()]
        comparisons_list = [comp.name for comp in comparisons.CompareOperator.all_instances()]
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
            _data_manager.reset_transaction_tags()
        elif "reset_tags" in request.form:
            _data_manager.reset_transaction_tags()
        elif "create_tag_button" in request.form:
            tag_name = request.form.get('tag_name_input')
            tag_json_str = '[{"description":{"contains":"something"}}]'
            if len(tag_name) == 0:
                create_tag_error = f"Tag name cannot not be empty."
            elif _data_manager.get_tag(tag_name) is not None:
                create_tag_error = f"Tag '{tag_name}' already exists. Please use another name."
            else:
                tag = Tag.from_json_string(tag_name, tag_json_str)
                _data_manager.update_tag(tag, update_tags=False)
                return redirect(url_for('tags.tag_edit', tag_name=tag_name))

    try:
        all_tags = [{'name': tag.name} for tag in sorted(_data_manager.all_tags(), key=lambda _tag: _tag.name)]
    except Exception as e:
        all_tags = f"Error: {e}"

    transactions = get_transactions()
    for tag in all_tags:
        tagged_transactions = transactions.containing_tag(tag['name'])
        tag['n_rows'] = tagged_transactions.size() if tagged_transactions else 0

    return render_template('tags_menu.html', **locals(), **globals())


@tags_bp.route('/edit/<tag_name>', methods=['POST', 'GET'])
@logs.codeflow_log_wrapper('#api')
def tag_edit(tag_name):
    if request.method == 'POST':
        if "refresh" in request.form:
            tag_json_str = request.form.get('query_text_input')
            tag_json_str = _reformat_json_str(tag_json_str)

        elif "reset" in request.form:
            tag_json_str = json.dumps(_data_manager.get_tag(tag_name).rule.to_json())
        elif "save" in request.form or "save_and_close" in request.form:
            tag_json_str = request.form.get('query_text_input')
            try:
                tag = Tag.from_json_string(tag_name, tag_json_str)
                _data_manager.update_tag(tag, update_tags=True)
            except Exception as e:
                message_text = f"Error: {e}"
            else:
                return redirect(url_for('tags.tag_edit', tag_name=tag_name))
        elif "delete" in request.form:
            confirm_delete = True
            tag_json_str = request.form.get('query_text_input')
            render_tag_page(title=f'Edit tag "{tag_name}"', **locals())

        elif "confirm_delete" in request.form:
            _data_manager.delete_tag(tag_name)
            try:
                _data_manager.reset_transaction_tags()
            except Exception as e:
                message_text = f"Error: {e}"
            else:
                return redirect(url_for('tags.tags_menu'))
        elif "add_condition" in request.form:
            disjunction = tagging.Disjunction.from_json_string(request.form.get('query_text_input'))

            condition = tagging.Condition.from_string_values(
                field=request.form['field'],
                transformation_op_key=request.form['transform'],
                compare_op_key=request.form['comparison'],
                value=request.form['value'],
            )

            new_tag_json = disjunction.append(condition).to_json()
            tag_json_str = _reformat_json_str(json.dumps(new_tag_json))
        elif "add_id" in request.form:
            _id = int(request.form['input_id'])
            disjunction = _rule_for_id(_id)
            old_disjunction = tagging.Disjunction.from_json_string(request.form.get('query_text_input'))
            new_tag_json = old_disjunction.extend(disjunction.rules).to_json()
            tag_json_str = _reformat_json_str(json.dumps(new_tag_json))
    else:
        tag_json_str = json.dumps(_data_manager.get_tag(tag_name).rule.to_json())
    return render_tag_page(title=f'Edit tag "{tag_name}"', **locals())


@tags_bp.route("/manual_tagging/order=<order_by>", methods=['POST', 'GET'])
def manual_tagging(order_by):
    if request.method == 'POST':
        if "save_changes" in request.form:
            tag_events = [{'tag': name.split('_for_')[0], 'id': name.split('_for_')[1]}
                                 for name, action
                                 in request.form.to_dict().items()
                                 if action == 'on']

            for event in tag_events:
                tag_added, _id = event['tag'], int(event['id'])
                disjunction = _rule_for_id(_id)
                tag_object = _data_manager.get_tag(tag_added)
                old_disjunction = tag_object.rule
                new_tag = tagging.Tag(tag_added, old_disjunction.extend(disjunction.rules))
                _data_manager.update_tag(new_tag)
            # TODO Send new tag events to data manager
        elif "reset" in request.form:
            pass

    transactions = get_transactions()
    all_tags = sorted([tag.name for tag in _data_manager.all_tags()])
    df = transactions.dataframe(
        df_transformer=ManualTaggingHTMLTableFormat(all_tags, order_by=order_by)
    )[:10]

    table_html = df.to_html(render_links=True, escape=False)

    return render_template('manual_tagging.html', **locals())
