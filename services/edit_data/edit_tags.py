import json
import logging
from urllib.parse import urlparse, parse_qs

from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon import config
from mecon.app.file_system import WorkingDataManager, WorkingDatasetDir
from mecon.data import reports
from mecon.data.transactions import Transactions
from mecon.tags import tagging, process
from mecon.tags import transformations, comparisons, tag_helpers
from mecon.tags.process import RuleExecutionPlanMonitor

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

datasets_obj = WorkingDatasetDir()
datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
dataset = datasets_obj.working_dataset

if dataset is None:
    raise ValueError(f"Unable to locate working dataset: {datasets_obj.working_dataset=}")

app_ui = ui.page_fluid(
    ui.tags.title("μEcon"),
    ui.navset_pill(
        ui.nav_control(ui.tags.a("Main page", href=f"http://127.0.0.1:8000/")),
        ui.nav_control(ui.tags.a("Reports", href=f"http://127.0.0.1:8001/reports/")),
        ui.nav_control(ui.tags.a("Edit data", href=f"http://127.0.0.1:8002/edit_data/")),
        ui.nav_control(ui.tags.a("Monitoring", href=f"http://127.0.0.1:8003/")),
        ui.nav_control(ui.input_dark_mode(id="light_mode")),
    ),
    ui.hr(),

    ui.page_fillable(
        ui.h1(ui.output_text(id='title_output_text')),
        ui.h3(ui.output_ui(id='tag_info_link')),
        ui.input_task_button(id='save_button', label='Save'),
        ui.input_task_button(id='reset_button', label='Reset', label_busy='Loading...'),
        ui.layout_columns(
            ui.card(
                ui.navset_tab(
                    ui.nav_panel(
                        "JSON",
                        ui.card(
                            ui.row(
                                ui.input_task_button(id='recalculate_button', label='Recalculate', width='25%'),
                                ui.tooltip(ui.input_task_button(
                                    id='check_diffs_button',
                                    label='Check differences... (!!)',
                                    width='25%',
                                    style="color: #fff; background-color: #aaa; border-color: #000"),
                                    'It will recalculate all tags for the current version of the tag and the last save.')
                            ),
                            ui.input_text_area(
                                id="tag_json_text",
                                label=ui.markdown("JSON text"),
                                value="{}",
                                # autoresize=True,
                                # resize='both',
                                width='100%',
                                height='700px'
                            ),
                            id='tag_json_text_card',
                            style='background-color:green')
                    ),
                    ui.nav_panel(
                        "Add ID",
                        ui.card(
                            ui.input_selectize(
                                id='id_add_selectize',
                                label='Select one or more IDs',
                                choices=[],
                                multiple=True,
                                width='100%'
                            ),
                            ui.input_task_button(id='id_add_button', label='Add IDs', width='25%')
                        )
                    ),
                    ui.nav_panel(
                        "Add condition",
                        ui.card(
                            ui.input_select(
                                id='condition_field_select',
                                label='Field',
                                choices=Transactions.columns,
                                multiple=False
                            ),
                            ui.input_select(
                                id='condition_transformation_select',
                                label='Transformation',
                                choices=[trans.name for trans in
                                         transformations.TransformationFunction.all_instances()],
                                selected='none'
                            ),
                            ui.input_select(
                                id='condition_compare_select',
                                label='Operation',
                                choices=[comp.name for comp in comparisons.CompareOperator.all_instances()]
                            ),
                            ui.input_text(id='condition_value_input_text', label='Value'),
                            ui.input_task_button(id='condition_add_button', label='Add condition', width='25%')
                        )
                    ),
                    ui.nav_panel(
                        "Rules",
                        ui.input_task_button(id='acc_rules_apply_button', label='Apply', disabled=True),
                        ui.accordion(
                            id="rules_accordion",
                            multiple=True
                        ),
                    ),
                )
            ),
            ui.card(
                ui.navset_tab(
                    ui.nav_panel(
                        "Tagged stats",
                        ui.output_ui(
                            id='tagged_transactions_stats',
                        )
                    ),
                    ui.nav_panel(
                        "Tagged transactions",
                        ui.output_data_frame(
                            id='tagged_transactions_output_df',
                        )
                    ),
                    ui.nav_panel(
                        "Untagged transactions",
                        ui.output_data_frame(
                            id='untagged_transactions_output_df',
                        )
                    )
                ),
                height='100%'
            ),
            col_widths=[6, 6],
            height='100%'
        )
    )
)

styles = [
    {
        "location": "body",
        "style": {
            "background-color": "grey",
            "border": "0.5px solid black",
            'font-size': '14px',
            'color': 'black'
        },
    },
    {
        "location": "body",
        "cols": [0],
        "style": {
            'font-size': '8px',
        },
    },
    {
        "location": "body",
        "cols": [1],
        "style": {
            'width': '400px',
        },
    }
]


def rule_to_ui(rule: tagging.AbstractRule):
    import uuid
    if isinstance(rule, tagging.Condition):
        condition_id = 'condition_id' + uuid.uuid4().hex
        condition_ui = ui.accordion_panel(
            f"Condition: {rule}",
            ui.input_selectize(
                id=f"condition_field_select_{condition_id}",
                label='Field',
                choices=['datetime', 'amount', 'currency', 'amount_cur', 'description', 'tags'],
                selected=rule.field
            ),
            ui.input_selectize(
                id=f"condition_transformation_select_{condition_id}",
                label='Transformation',
                choices=[trans.name for trans in transformations.TransformationFunction.all_instances()],
                selected=rule.transformation_operation.name
            ),
            ui.input_selectize(
                id=f"condition_compare_select_{condition_id}",
                label='Operation',
                choices=[comp.name for comp in comparisons.CompareOperator.all_instances()],
                selected=rule.compare_operation.name
            ),
            ui.input_text(
                id=f"condition_value_input_text_{condition_id}",
                label='Value',
                value=rule.value
            ),
        )
        return condition_ui
    elif isinstance(rule, tagging.Conjunction):
        rule_id = 'conjunction_id' + uuid.uuid4().hex
        inner_rules = [rule_to_ui(rule) for rule in rule.rules]
        comp_rule = ui.accordion_panel(
            f"Composite (AND): {rule}",
            ui.accordion(
                *inner_rules,
                id=rule_id,
                # multiple=False,
                open=True
            )
        )
        return comp_rule
    elif isinstance(rule, tagging.Disjunction):
        rule_id = 'disjunction_id' + uuid.uuid4().hex
        inner_rules = [rule_to_ui(rule) for rule in rule.rules]
        comp_rule = ui.accordion_panel(
            f"Composite (OR): {rule}",
            ui.accordion(
                *inner_rules,
                id=rule_id,
                # multiple=False,
                open=True
            )
        )
        return comp_rule


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = WorkingDataManager()
    all_tags = data_manager.all_tags()

    current_tag_value = reactive.Value(None)

    @reactive.calc
    def url_params() -> dict:
        logging.info(input['.clientdata_url_search'])
        urlparse_result = urlparse(input['.clientdata_url_search'].get())  # TODO move to a reactive.calc func
        params = parse_qs(urlparse_result.query)
        logging.info(f"Input params: {params}")
        return params

    @reactive.calc
    def fetch_tag_name():
        params = url_params()
        tag_name = params['filter_in_tags'][0]
        return tag_name

    @reactive.calc
    def fetch_tag():
        params = url_params()
        tag_name = params['filter_in_tags'][0]
        logging.info(f"Fetching tag '{tag_name}' from the DB...")
        tag = data_manager.get_tag(tag_name)
        if tag is None:
            ValueError(f"Tag {params['filter_in_tags']} does not exists")

        return tag

    def calculate_transaction_for_tag(tag):
        logging.info(f"Fetching transactions from the DB...")
        transactions = data_manager.get_transactions()
        df_trans = transactions.dataframe()

        logging.info(f"Re-applying tag '{tag.name}' on transactions...")
        tagging.Tagger.tag(tag, df_trans, remove_old_tags=True)
        new_transactions = Transactions(df_trans)
        logging.info(f"Re-applying tag '{tag.name}' on transactions... Done")
        return new_transactions

    @reactive.effect
    def load():
        if current_tag_value.get() == None:
            logging.info(f"Loading tag {fetch_tag_name()} from DB...")
            tag = fetch_tag()
            current_tag_value.set(tag)
        logging.info(f"Updating UI according to tag '{current_tag_value.get().name}'...")
        ui.update_text_area(id='tag_json_text', value=get_target_tag_json())
        ui.update_selectize(id='id_add_selectize',
                            choices={_id: _id for _id in untagged_transactions().dataframe()['id']})

        transactions = data_manager.get_transactions()
        invalid_transactions = transactions.invalid_transactions()
        if invalid_transactions is not None:
            m = ui.modal(
                ui.output_data_frame(id='invalid_transactions_output_df'),
                # ui.HTML(diff_df.to_html()),
                title=f"Warning: Invalid transactions",
                easy_close=True,
                # footer=ui.input_task_button(id='confirm_save_button', label='Confirm', label_buzy='Saving...'),
                size='xl'
            )
            ui.modal_show(m)

    @render.data_frame
    def invalid_transactions_output_df():
        transactions = data_manager.get_transactions()
        invalid_transactions = transactions.invalid_transactions()
        return render.DataTable(
            invalid_transactions.dataframe(),
            selection_mode="none",
            filters=True,
            styles=styles
        )

    @reactive.calc
    def current_transactions():
        new_transactions = calculate_transaction_for_tag(current_tag_value.get())
        return new_transactions

    @reactive.calc
    def tagged_transactions():
        return current_transactions().containing_tags(current_tag_value.get().name)

    @reactive.calc
    def untagged_transactions():
        return current_transactions().not_containing_tags(current_tag_value.get().name)

    def new_transactions_and_monitor():
        transactions = data_manager.get_transactions()
        monitor = RuleExecutionPlanMonitor(dataset)
        tag_name, tag_json_str = fetch_tag_name(), input.tag_json_text()
        new_tag = tagging.Tag.from_json_string(tag_name, tag_json_str)
        new_tags = [new_tag] + [tag for tag in all_tags if tag.name != tag_name]

        orep = process.OptimisedRuleExecutionPlanTagging(new_tags)
        orep.create_rule_execution_plan()
        orep.create_optimised_rule_execution_plan()
        new_trans = orep.tag(transactions, monitor=monitor)

        return new_trans, monitor

    @reactive.calc
    def changed_transactions():
        transactions = data_manager.get_transactions()
        new_trans, monitor = new_transactions_and_monitor()

        diff = transactions.tags_diff(new_trans, target_tags=[fetch_tag_name()])
        return diff, monitor

    @reactive.calc
    def get_target_tag_json():
        return json.dumps(current_tag_value.get().rule.to_json(), indent=4)

    @render.text
    def title_output_text():
        return f"Editing tag: {fetch_tag_name()}"

    @render.ui
    def tag_info_link():
        return ui.tags.a("Info page", href=f"http://127.0.0.1:8001/reports/tags/?filter_in_tags={fetch_tag_name()}")

    def format_dt(dt):
        date_str, time = dt.date().strftime('%a %d %b, %Y'), dt.time()
        formatted_date_str = f"📅{date_str}\t🕑{time}"
        return formatted_date_str

    @render.data_frame
    def tagged_transactions_output_df():
        df = tagged_transactions().dataframe().copy()
        df['datetime'] = df['datetime'].apply(format_dt)
        return render.DataTable(
            df,
            selection_mode="none",
            filters=True,
            styles=styles
        )

    @render.data_frame
    def untagged_transactions_output_df():
        df = untagged_transactions().dataframe().copy()
        df['datetime'] = df['datetime'].apply(format_dt)
        return render.DataTable(
            df,
            selection_mode="none",
            filters=True,
            styles=styles
        )

    @render.text
    def tagged_transactions_stats():
        _tagged_transactions = tagged_transactions()
        if _tagged_transactions.size() == 0:
            raise ValueError(f"Empty tagged_transactions")
        return ui.markdown(reports.transactions_stats_markdown(tagged_transactions()))

    @reactive.effect
    @reactive.event(input.reset_button)
    def _():
        logging.info("Reset")
        current_tag_value.set(None)

    @reactive.effect
    @reactive.event(input.recalculate_button)
    def _():
        logging.info("Recalculate")
        tag_name, tag_json_str = fetch_tag_name(), input.tag_json_text()
        try:
            new_tag = tagging.Tag.from_json_string(tag_name, tag_json_str)
            current_tag_value.set(new_tag)
        except json.decoder.JSONDecodeError as e:
            import traceback
            m = ui.modal(
                f"{traceback.format_exc()=}",
                title=f"Invalid JSON: {e}",
                easy_close=True,
                footer=None,
                size='l'
            )
            ui.modal_show(m)

    @reactive.effect
    @reactive.event(input.save_button)
    def _():
        logging.info("Save")
        tag_json_str = get_target_tag_json()
        warning = f"Warning: There is unsaved progress!!!" if tag_json_str != input.tag_json_text() else ''
        m = ui.modal(
            ui.markdown(
                f"# {warning}\n   "
                f"Tag {fetch_tag_name()} is about to be saved to the DB:   "
                f"   \n"
                f"   \n"
                f"{tag_json_str}"
            ),
            title=f"Saving {fetch_tag_name()}",
            easy_close=False,
            footer=ui.input_task_button(id='confirm_save_button', label='Confirm', label_buzy='Saving...'),
            size='xl'
        )
        ui.modal_show(m)

    @reactive.effect
    @reactive.event(input.confirm_save_button)
    def _():
        logging.info("Saving")
        data_manager.update_tag(current_tag_value.get(), update_tags=True)
        ui.modal_remove()

    @reactive.effect
    @reactive.event(input.id_add_button)
    def _():
        """ TODO error while adding id
        INFO:root:Adding IDs (1): ['RVLTd20220810t190439an57500i32747']
mecon-edit_data_app-1: Traceback (most recent call last):
mecon-edit_data_app-1:   File "/usr/local/lib/python3.11/site-packages/shiny/reactive/_reactives.py", line 584, in _run
mecon-edit_data_app-1:     await self._fn()
mecon-edit_data_app-1:   File "/usr/local/lib/python3.11/site-packages/shiny/_utils.py", line 273, in fn_async
mecon-edit_data_app-1:     return fn(*args, **kwargs)
mecon-edit_data_app-1:            ^^^^^^^^^^^^^^^^^^^
mecon-edit_data_app-1:   File "/usr/local/lib/python3.11/site-packages/shiny/reactive/_reactives.py", line 901, in new_user_fn
mecon-edit_data_app-1:     return user_fn()
mecon-edit_data_app-1:            ^^^^^^^^^
mecon-edit_data_app-1:   File "/mecon/services/edit_data/edit_tags.py", line 454, in _
mecon-edit_data_app-1:     new_tag = tag_helpers.add_rule_for_id(
mecon-edit_data_app-1:               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
mecon-edit_data_app-1:   File "/mecon/mecon/tags/tag_helpers.py", line 11, in add_rule_for_id
mecon-edit_data_app-1:     potential_id_condition = tag.rule.rules[0].rules[0]
mecon-edit_data_app-1:                              ^^^^^^^^^^^^^^^^^^^^^^^
mecon-edit_data_app-1: AttributeError: 'Condition' object has no attribute 'rules'
mecon-edit_data_app-1: /usr/local/lib/python3.11/site-packages/shiny/reactive/_reactives.py:566: ReactiveWarning: Error in Effect: 'Condition' object has no attribute 'rules'
mecon-edit_data_app-1:   await self._run()
mecon-edit_data_app-1: Unhandled error: 'Condition' object has no attribute 'rules'
mecon-edit_data_app-1: INFO:     connection closed
mecon-edit_data_app-1: INFO:     172.18.0.1:43444 - "GET /edit_data/tags/edit/?filter_in_tags=Rent HTTP/1.1" 200 OK
        """
        ids_to_add = list(input.id_add_selectize())
        logging.info(f"Adding IDs ({len(ids_to_add)}): {ids_to_add}")
        new_tag = tag_helpers.add_rule_for_id(
            tag=current_tag_value.get(),
            ids_to_add=ids_to_add)
        current_tag_value.set(new_tag)

    @reactive.effect
    @reactive.event(input.condition_add_button)
    def _():
        # TODO it doesn;t remove the empty disjunctions
        value_str = input.condition_value_input_text()
        value = value_str if not value_str.isnumeric() else int(value_str) if value_str.isdigit() else float(value_str)

        condition_to_add = tagging.Condition.from_string_values(
            field=input.condition_field_select(),
            transformation_op_key=input.condition_transformation_select(),
            compare_op_key=input.condition_compare_select(),
            value=value,
        )
        logging.info(f"Adding condition: {condition_to_add}")

        new_rule = current_tag_value.get().rule.append(condition_to_add)
        new_tag = tagging.Tag(fetch_tag_name(), new_rule)
        current_tag_value.set(new_tag)

    @reactive.effect
    @reactive.event(input.check_diffs_button)
    def _():
        # TODO rows added and rows removed are the same, have to change .diff to account for that
        diff, monitor = changed_transactions()
        all_monitored_tags = sorted(monitor.all_monitored_tag_names())
        diff_df = diff.dataframe()
        logging.info(f"Diff: {diff_df.shape=}")
        m = ui.modal(
            ui.navset_tab(
                ui.nav_panel(f"{len(diff_df)} rows added (regarding to '{fetch_tag_name()}' tag)", ui.output_data_frame(id='transactions_diff_added_output_df')),
                ui.nav_panel(f"{len(diff_df)} rows removed (regarding to '{fetch_tag_name()}' tag)", ui.output_data_frame(id='transactions_diff_removed_output_df')),
                ui.nav_panel('Calcs',
                             ui.input_select(
                                 id='tag_select_for_calc_monitor',
                                 label='Tags',
                                 choices=all_monitored_tags,
                                 selected=fetch_tag_name()
                             ),
                             ui.output_data_frame(id='calculation_monitor_output_df')),
            ),
            # ui.HTML(diff_df.to_html()),
            title=f"Recalculated transaction tags",
            easy_close=True,
            # footer=ui.input_task_button(id='confirm_save_button', label='Confirm', label_buzy='Saving...'),
            size='xl'
        )
        ui.modal_show(m)

    @render.data_frame
    def calculation_monitor_output_df():
        tag_name = input.tag_select_for_calc_monitor()
        new_trans, monitor = new_transactions_and_monitor()
        df = monitor.get_tag_calculations(tag_name).copy()
        df.replace([False, True], value=['False', 'True'], inplace=True)
        logging.info(f"{df.columns=}")
        return render.DataTable(
            df,
            selection_mode='none',
            filters=True,
            styles=styles
        )

    @render.data_frame
    def transactions_diff_added_output_df():
        diff, monitor = changed_transactions()
        diff_df = diff.dataframe()
        logging.info(f"Diff: {diff_df.shape=}")
        return render.DataTable(
            diff_df,
            selection_mode="none",
            filters=True,
            styles=styles
        )

    @render.data_frame
    def transactions_diff_removed_output_df():
        transactions = data_manager.get_transactions()
        new_trans, monitor = new_transactions_and_monitor()

        diff = new_trans.tags_diff(transactions, target_tags=[fetch_tag_name()])
        diff_df = diff.dataframe()
        logging.info(f"Diff: {diff_df.shape=}")
        return render.DataTable(
            diff_df,
            selection_mode="none",
            filters=True,
            styles=styles
        )



edit_tags_app = App(app_ui, server)
