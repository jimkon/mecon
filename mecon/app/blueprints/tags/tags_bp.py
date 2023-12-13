import json

from flask import Blueprint, render_template, request, redirect, url_for

from mecon.app.data_manager import DBDataManager
from mecon.data.transactions import Transactions
from mecon.monitoring import logs
from mecon.tag_tools import tagging, comparisons, transformations
from mecon.tag_tools.tagging import Tag

tags_bp = Blueprint('tags', __name__, template_folder='templates')

_data_manager = DBDataManager()


def _json_from_str(json_str):
    _json = json.loads(json_str)
    return _json


def _reformat_json_str(json_str):
    json_str = json_str.replace("'", '"')
    json_str = json.dumps(json.loads(json_str), indent=4)
    return json_str


@logs.codeflow_log_wrapper('#data#transactions#load')
def get_transactions() -> Transactions:
    transactions = _data_manager.get_transactions()
    return transactions


def render_tag_page(title='Tag page',
                    tag_name='',
                    tag_json_str='[{"description":{"contains":"something"}}]',
                    message_text='',
                    confirm_delete=False):
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
            _data_manager.reset_tags()
        elif "reset_tags" in request.form:
            _data_manager.reset_tags()
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
            tag_json_str = _data_manager.get_tag(tag_name).rule.to_json()
        elif "save" in request.form or "save_and_close" in request.form:
            tag_json_str = request.form.get('query_text_input')

            try:
                tag = Tag.from_json_string(tag_name, _json_from_str(tag_json_str))
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
                _data_manager.reset_tags()
            except Exception as e:
                message_text = f"Error: {e}"
            else:
                return redirect(url_for('tags.tags_menu'))
        elif "add_condition" in request.form:
            disjunction = tagging.Disjunction.from_json(_json_from_str(request.form.get('query_text_input')))

            condition = tagging.Condition.from_string_values(
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

            disjunction = tagging.Disjunction.from_json(_json_from_str(request.form.get('query_text_input')))

            condition = tagging.Condition.from_string_values(
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

                conj = tagging.Conjunction([  # TODO Conjunction.from_df_row()
                    tagging.Condition.from_string_values('datetime', 'str', 'equal', str(row['datetime'])),
                    tagging.Condition.from_string_values('amount', 'none', 'equal', row['amount']),
                    tagging.Condition.from_string_values('currency', 'str', 'equal', row['currency']),
                    tagging.Condition.from_string_values('description', 'none', 'equal', row['description']),
                    # tagging.Condition.from_string_values(field, 'none', 'equal', value) for field, value in row.items()
                ])
                new_tag_json = disjunction.append(conj).to_json()
                tag_json_str = _reformat_json_str(json.dumps(new_tag_json))
                del condition, disjunction, new_tag_json, id, rows, row, conj  # otherwise **locals() will break render_tag_page

    else:
        tag_json_str = json.dumps(_data_manager.get_tag(tag_name).rule.to_json())
    return render_tag_page(title=f'Edit tag "{tag_name}"', **locals())


@tags_bp.route("/manual_tagging/order=<order_by>", methods=['POST', 'GET'])
def manual_tagging(order_by):
    def tags_form(row):
        id = row[0]
        tags = row[6].split(',')
        tag_checkboxes = """
        <div class="dropdown">
        <button class="dropbtn">Select Tags</button>
        <div class="dropdown-content">
        """
        for tag in all_tags:
            checkbox = f"""
            <label >{tag}<input type="checkbox" name="{tag}_for_{id}" {'checked disabled' if tag in tags else ''}></label>
            """
            tag_checkboxes += checkbox
        tag_checkboxes = tag_checkboxes + "</div></div>"
        tags_container = """
        <div class="labelContainer">"""
        for tag in tags:
            tags_container += f"""<label class="tagLabel">{tag}</label>"""

        tags_container += """</div>"""

        result = tag_checkboxes + tags_container
        return result.replace('\n', '')

    if request.method == 'POST':
        if "save_changes" in request.form:
            tag_events = [{'tag': name.split('_for_')[0], 'id': name.split('_for_')[1]}
                          for name, action
                          in request.form.to_dict().items()
                          if action == 'on']
            # TODO Send new tag events to data manager
        elif "reset" in request.form:
            pass

    transactions = get_transactions()
    all_tags = list(transactions.all_tags().keys())
    df = transactions.dataframe()

    if order_by == 'newest':
        df = df.sort_values(['datetime'], ascending=False)
    elif order_by == 'least tagged':
        df['n_tags'] = df['tags'].apply(lambda s: len(s.split(',')))
        df = df.sort_values(['n_tags'], ascending=True)
        del df['n_tags']

    df['Edit'] = df.apply(tags_form, axis=1)
    df = df[[df.columns[-1]] + list(df.columns[:-1])]
    table_html = df.to_html(render_links=True, escape=False)

    return render_template('manual_tagging.html', **locals())
