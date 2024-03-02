import json
import logging

from flask import Blueprint, render_template, request, redirect, url_for, flash

import monitoring.logging_utils
from mecon import config
from mecon.app import WorkingDatasetDir
from mecon.app.app_utils import TransactionsDataTransformationToManualTaggingHTMLTable
from mecon.app.data_manager import GlobalDataManager
from mecon.data.transactions import Transactions
from mecon.monitoring import tag_monitoring
from mecon.monitoring.tag_monitoring import TaggingReport
from mecon.tag_tools import tag_helpers
from mecon.tag_tools import tagging, comparisons, transformations
from mecon.tag_tools.tagging import Tag

tags_bp = Blueprint('tags', __name__, template_folder='templates')

_data_manager = GlobalDataManager()


def _reformat_json_str(json_str):
    json_str = json_str.replace("'", '"')
    json_str = json.dumps(json.loads(json_str), indent=4)
    return json_str


def create_alerts_from_tagging_report(tagging_report):
    df = tagging_report.unsatisfied_tagging_rules_df()
    for index, row in df.iterrows():
        tag_edit_url = f"""<a href={url_for('tags.tag_edit', tag_name=row['tag'])}>{row['tag']}</a>"""
        flash(f"Unsatisfied tagging rule in <strong>{tag_edit_url}</strong> tag! {row['rule_type']}: {row['rule']}",
              category="error")

    df = tagging_report.unsatisfied_rules_df()
    for index, row in df.iterrows():
        tag_edit_url = f"""<a href={url_for('tags.tag_edit', tag_name=row['tag'])}>{row['tag']}</a>"""
        flash(f"Unsatisfied rule in <strong>{tag_edit_url}</strong> tag! {row['rule_type']}: {row['rule']}",
              category="warning")


@monitoring.logging_utils.codeflow_log_wrapper('#data#transactions#load')  # TODO remove
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
        if config.TAG_MONITORING:
            tagging_monitor = tag_monitoring.TaggingStatsMonitoringSystem([tag])

        transactions = get_transactions().apply_rule(tag.rule)

        if config.TAG_MONITORING:
            tagging_report = tagging_monitor.produce_report()
            create_alerts_from_tagging_report(tagging_report)

        tagged_table_html = transactions.to_html() if transactions else "<h4>'No transaction data after applying rule'</h4>"
        untagged_transactions = get_transactions().apply_negated_rule(tag.rule)

        all_fields = transactions.dataframe().columns.to_list() if transactions else untagged_transactions.dataframe().columns.to_list()
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
@monitoring.logging_utils.codeflow_log_wrapper('#api')
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

    if config.TAG_MONITORING:
        tagging_report = TaggingReport.load(WorkingDatasetDir.get_instance().working_dataset.path)
        if tagging_report is None:
            logging.warning(f"Tagging report file {TaggingReport._filename} was not found.")
        else:
            create_alerts_from_tagging_report(tagging_report)

    return render_template('tags_menu.html', **locals(), **globals())


@tags_bp.route('/edit/<tag_name>', methods=['POST', 'GET'])
@monitoring.logging_utils.codeflow_log_wrapper('#api')
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
            _id = request.form['input_id']
            new_tag = tag_helpers.add_rule_for_id(
                tag=tagging.Tag(tag_name, tagging.Disjunction.from_json_string(request.form.get('query_text_input'))),
                _id_str=_id)
            new_tag_json = new_tag.rule.to_json()
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

            for tag_name in set([event['tag'] for event in tag_events]):
                ids = ','.join([event['id'] for event in tag_events if event['tag'] == tag_name])
                tag_object = _data_manager.get_tag(tag_name)
                new_tag = tag_helpers.add_rule_for_id(tag_object, ids)
                _data_manager.update_tag(new_tag)
        elif "reset" in request.form:
            pass

    all_tags = sorted([tag.name for tag in _data_manager.all_tags()])

    transactions = get_transactions()
    n_transactions = transactions.size()

    chunk_size = config.TRANSACTIONS_CHUNK_SIZE
    index_start = int(request.args['index_start']) if 'index_start' in request.args else 0
    index_end = int(request.args['index_end']) if 'index_end' in request.args else (chunk_size - 1)

    table_html = transactions.to_html(
        df_transformer=TransactionsDataTransformationToManualTaggingHTMLTable(
            all_tags,
            order_by=order_by,
            index_start=index_start,
            index_end=index_end
        ))

    return render_template('manual_tagging.html', **locals())
