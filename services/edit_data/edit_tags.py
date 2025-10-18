import json
import logging
from urllib.parse import urlparse, parse_qs

from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app import shiny_app
from mecon.data import reports
from mecon.data.transactions import Transactions
from mecon.tags import tagging, process
from mecon.tags import transformations, comparisons, tag_helpers
from mecon.tags.process import RuleExecutionPlanMonitor

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def fetch_tag_from_manager(data_manager, tag_name: str):
    tag = data_manager.get_tag(tag_name)
    if tag is None:
        raise ValueError(f"Tag '{tag_name}' does not exist")
    return tag


def calculate_transactions_for_tag(data_manager, tag: tagging.Tag) -> Transactions:
    transactions = data_manager.get_transactions()
    df_trans = transactions.dataframe()
    tagging.Tagger.tag(tag, df_trans, remove_old_tags=True)
    return Transactions(df_trans)


def load_monitor(dataset):
    monitor = RuleExecutionPlanMonitor(dataset)
    monitor.load()
    return monitor


def build_new_transactions_and_monitor(data_manager, dataset, all_tags, tag_name: str, tag_json_str: str):
    new_tag = parse_tag_from_json(tag_name, tag_json_str)
    new_tags = [new_tag] + [tag for tag in all_tags if tag.name != tag_name]

    orep = process.OptimisedRuleExecutionPlanTagging(new_tags)
    orep.create_rule_execution_plan()
    orep.create_optimised_rule_execution_plan()

    transactions = data_manager.get_transactions()
    monitor = load_monitor(dataset)
    new_trans = orep.tag(transactions, monitor=monitor)
    return new_trans, monitor


def compute_transactions_diff(original_transactions, new_transactions, tag_name: str):
    return original_transactions.tags_diff(new_transactions, target_tags=[tag_name])


def serialise_tag_to_json(tag: tagging.Tag) -> str:
    return json.dumps(tag.rule.to_json(), indent=4)


def format_transaction_datetime(dt):
    date_str, time = dt.date().strftime('%a %d %b, %Y'), dt.time()
    return f"📅{date_str}\t🕑{time}"


def build_invalid_transactions_modal():
    return ui.modal(
        ui.output_data_frame(id='invalid_transactions_output_df'),
        title="Warning: Invalid transactions",
        easy_close=True,
        size='xl'
    )


def build_save_confirmation_modal(tag_name: str, warning: str, tag_json_str: str):
    return ui.modal(
        ui.markdown(
            f"# {warning}\n   "
            f"Tag {tag_name} is about to be saved to the DB:   "
            f"   \n"
            f"   \n"
            f"{tag_json_str}"
        ),
        title=f"Saving {tag_name}",
        easy_close=True,
        footer=ui.input_task_button(id='confirm_save_button', label='Confirm', label_buzy='Saving...'),
        size='xl'
    )


def build_recalculation_modal(tag_name: str, diff_df, monitor):
    all_monitored_tags = sorted(monitor.all_monitored_tag_names())
    return ui.modal(
        ui.navset_tab(
            ui.nav_panel(f"{len(diff_df)} rows added (regarding to '{tag_name}' tag)",
                         ui.output_data_frame(id='transactions_diff_added_output_df')),
            ui.nav_panel(f"{len(diff_df)} rows removed (regarding to '{tag_name}' tag)",
                         ui.output_data_frame(id='transactions_diff_removed_output_df')),
            ui.nav_panel('Calcs',
                         ui.input_select(
                             id='tag_select_for_calc_monitor',
                             label='Tags',
                             choices=all_monitored_tags,
                             selected=tag_name
                         ),
                         ui.output_data_frame(id='calculation_monitor_output_df')),
        ),
        title="Recalculated transaction tags",
        easy_close=True,
        size='xl'
    )


def build_unsaved_warning(original_json: str, current_json: str) -> str:
    if original_json == current_json:
        return ''
    unsaved_rules = ''.join([c1 for c1, c2 in zip(original_json, current_json) if c1 != c2])
    if len(unsaved_rules) == 0:
        return ''
    return f"Warning: There is unsaved progress!!!\n\n{unsaved_rules}"


def add_ids_to_tag(tag: tagging.Tag, ids_to_add):
    return tag_helpers.add_rule_for_id(tag=tag, ids_to_add=ids_to_add)


def append_condition_to_tag(tag: tagging.Tag, *, field: str, transformation_key: str, compare_key: str, value):
    condition_to_add = tagging.Condition.from_string_values(
        field=field,
        transformation_op_key=transformation_key,
        compare_op_key=compare_key,
        value=value,
    )
    new_rule = tag.rule.append(condition_to_add)
    return tagging.Tag(tag.name, new_rule)


def parse_tag_from_json(tag_name: str, tag_json_str: str) -> tagging.Tag:
    return tagging.Tag.from_json_string(tag_name, tag_json_str)


def parse_condition_value(value_str: str):
    if not value_str.isnumeric():
        return value_str
    if value_str.isdigit():
        return int(value_str)
    return float(value_str)


app_ui = shiny_app.app_ui_factory(
    ui.page_fillable(
        ui.h1(ui.output_text(id='title_output_text')),
        ui.h3(ui.output_ui(id='tag_info_link')),
        ui.input_task_button(id='save_button', label='Save'),
        ui.input_task_button(id='reset_button', label='Reset', label_busy='Loading...'),
        ui.input_task_button(id='recalculate_button', label='Recalculate'),
        ui.tooltip(ui.input_task_button(
            id='check_diffs_button',
            label='Check differences... (!!)',
            style="color: #fff; background-color: #aaa; border-color: #000"),
            'It will recalculate all tags for the current version of the tag and the last save.'),
        ui.layout_columns(
            ui.card(
                ui.navset_tab(
                    ui.nav_panel(
                        "JSON",
                        ui.card(
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
                    # ui.nav_panel( # TODO enable or remove
                    #     "Rules",
                    #     ui.input_task_button(id='acc_rules_apply_button', label='Apply', disabled=True),
                    #     ui.accordion(
                    #         id="rules_accordion",
                    #         multiple=True
                    #     ),
                    # ),
                    ui.nav_panel(
                        "Rule calculations",
                        ui.input_checkbox(id='rule_calculations_show_tagged', label='Show only tagged', value=True),
                        ui.output_data_frame(
                            id="rule_calculations_table",
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
                    ),
                    ui.nav_panel(
                        "Condition   stats",
                        ui.output_data_frame(
                            id='condition_stats_output_df',
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
    dataset = shiny_app.get_working_dataset()
    data_manager = shiny_app.create_data_manager()
    all_tags = data_manager.all_tags()

    current_tag_value = reactive.Value(None)

    get_url_params = shiny_app.url_params_function_factory(
        input,
        output,
        session,
        data_manager,
        ensure_exists=['filter_in_tags'])

    @reactive.calc
    def fetch_tag_name():
        return get_url_params()['filter_in_tags'][0]

    @reactive.calc
    def fetch_tag():
        tag_name = fetch_tag_name()
        logging.info(f"Fetching tag '{tag_name}' from the DB...")
        return fetch_tag_from_manager(data_manager, tag_name)

    @reactive.effect
    def load():
        if current_tag_value.get() is None:
            logging.info(f"Loading tag {fetch_tag_name()} from DB...")
            current_tag_value.set(fetch_tag())
        logging.info(f"Updating UI according to tag '{current_tag_value.get().name}'...")
        ui.update_text_area(id='tag_json_text', value=get_target_tag_json())
        ui.update_selectize(
            id='id_add_selectize',
            choices={_id: _id for _id in untagged_transactions().dataframe()['id']}
        )

        invalid_transactions = data_manager.get_transactions().invalid_transactions()
        if invalid_transactions is not None:
            ui.modal_show(build_invalid_transactions_modal())

    @render.data_frame
    def invalid_transactions_output_df():
        transactions = data_manager.get_transactions()
        invalid_transactions = transactions.invalid_transactions()
        return render.DataTable(
            invalid_transactions.dataframe(),
            selection_mode="none",
            filters=True,
            styles=shiny_app.datatable_styles
        )

    @reactive.calc
    def current_transactions():
        tag = current_tag_value.get()
        return calculate_transactions_for_tag(data_manager, tag)

    @reactive.calc
    def tagged_transactions():
        return current_transactions().containing_tags(current_tag_value.get().name)

    @reactive.calc
    def untagged_transactions():
        return current_transactions().not_containing_tags(current_tag_value.get().name)

    def new_transactions_and_monitor():
        tag_name, tag_json_str = fetch_tag_name(), input.tag_json_text()
        return build_new_transactions_and_monitor(data_manager, dataset, all_tags, tag_name, tag_json_str)

    @reactive.calc
    def changed_transactions():
        transactions = data_manager.get_transactions()
        new_trans, monitor = new_transactions_and_monitor()
        diff = compute_transactions_diff(transactions, new_trans, fetch_tag_name())
        return diff, monitor

    @reactive.calc
    def get_target_tag_json():
        return serialise_tag_to_json(current_tag_value.get())

    @render.text
    def title_output_text():
        return f"Editing tag: {fetch_tag_name()}"

    @render.ui
    def tag_info_link():
        return ui.tags.a("Tag info", href=shiny_app.url_for_tag_report(filter_in_tags=fetch_tag_name()))

    @render.text
    def tagged_transactions_stats():
        _tagged_transactions = tagged_transactions()
        if _tagged_transactions.size() == 0:
            raise ValueError(f"Empty tagged_transactions")
        return ui.markdown(reports.transactions_stats_markdown(tagged_transactions()))

    @render.data_frame
    def tagged_transactions_output_df():
        df = tagged_transactions().dataframe().copy()
        df['datetime'] = df['datetime'].apply(format_transaction_datetime)
        return shiny_app.render_table_standard(df, empty_message='No tagged transactions')

    @render.data_frame
    def untagged_transactions_output_df():
        df = untagged_transactions().dataframe().copy()
        df['datetime'] = df['datetime'].apply(format_transaction_datetime)
        return shiny_app.render_table_standard(df, empty_message='No UNtagged transactions')

    @render.data_frame
    def condition_stats_output_df():
        monitor = load_monitor(dataset)
        df = monitor.get_conditions_stats(tag_name=fetch_tag_name())
        return shiny_app.render_table_standard(df,
                                               format_columns=True,
                                               format_boolean_values=True,
                                               empty_message='No conditions stats')

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
            current_tag_value.set(parse_tag_from_json(tag_name, tag_json_str))
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
        target_json = get_target_tag_json()
        warning = build_unsaved_warning(target_json, input.tag_json_text())
        ui.modal_show(build_save_confirmation_modal(fetch_tag_name(), warning, target_json))

    @reactive.effect
    @reactive.event(input.confirm_save_button)
    def _():
        logging.info("Saving")
        data_manager.update_tag(current_tag_value.get(), update_tags=True)
        ui.modal_remove()

    @reactive.effect
    @reactive.event(input.id_add_button)
    def _():
        ids_to_add = list(input.id_add_selectize())
        logging.info(f"Adding IDs ({len(ids_to_add)}): {ids_to_add}")
        current_tag_value.set(add_ids_to_tag(current_tag_value.get(), ids_to_add))

    @reactive.effect
    @reactive.event(input.condition_add_button)
    def _():
        # TODO it doesn't remove the empty disjunctions
        value_str = input.condition_value_input_text()
        value = parse_condition_value(value_str)
        logging.info("Adding condition")
        current_tag_value.set(
            append_condition_to_tag(
                current_tag_value.get(),
                field=input.condition_field_select(),
                transformation_key=input.condition_transformation_select(),
                compare_key=input.condition_compare_select(),
                value=value,
            )
        )

    @reactive.effect
    @reactive.event(input.check_diffs_button)
    def _():
        # TODO rows added and rows removed are the same, have to change .diff to account for that
        diff, monitor = changed_transactions()
        diff_df = diff.dataframe()
        logging.info(f"Diff: {diff_df.shape=}")
        ui.modal_show(build_recalculation_modal(fetch_tag_name(), diff_df, monitor))

    @render.data_frame
    def calculation_monitor_output_df():
        tag_name = input.tag_select_for_calc_monitor()
        new_trans, monitor = new_transactions_and_monitor()
        df = monitor.get_tag_calculations(tag_name).copy()
        df.replace([False, True], value=['False', 'True'], inplace=True)
        logging.info(f"{df.columns=}")
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def transactions_diff_added_output_df():
        diff, _ = changed_transactions()
        diff_df = diff.dataframe()
        logging.info(f"Diff: {diff_df.shape=}")
        return shiny_app.render_table_standard(diff_df)

    @render.data_frame
    def transactions_diff_removed_output_df():
        transactions = data_manager.get_transactions()
        new_trans, monitor = new_transactions_and_monitor()

        diff = compute_transactions_diff(new_trans, transactions, fetch_tag_name())
        diff_df = diff.dataframe()
        logging.info(f"Diff: {diff_df.shape=}")
        return shiny_app.render_table_standard(diff_df)

    @render.data_frame
    def rule_calculations_table():
        new_trans, monitor = new_transactions_and_monitor()
        df = monitor.get_tag_calculations(fetch_tag_name())
        if input.rule_calculations_show_tagged():
            idx_true = Transactions(df).contains_tags(fetch_tag_name())
            df = df[idx_true].copy()
        df.replace([False, True], value=['False', 'True'], inplace=True)
        return shiny_app.render_table_standard(df)


edit_tags_app = App(app_ui, server)
