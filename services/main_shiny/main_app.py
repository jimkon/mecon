import logging

import pandas as pd
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon import config
from mecon.app import shiny_app
from mecon.app.current_data import WorkingDatasetDir, WorkingDataManagerInfo, WorkingDataManager
from mecon.data.data_management import CachedFileDataManager
# from mecon.app.current_data import WorkingDatasetDirInfo
from mecon.etl import transformers
from mecon.tags.process import RuleExecutionPlanMonitor

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# TODO settings are not refreshed if i change something manually. maybe shiny is caching stuff, because something similar happens to the data in the reports
# TODO need to rework the etl. statements should be treated as unique, don't check for duplicate rows between different statements. also, i should instantly convert them to transactions and add them to transactions table, skipping the bank statement tables entirely. the will reduce the db size, and complexity, and it will allow any data to be added by only adding the parser/etl converted

datasets_dir = config.DEFAULT_DATASETS_DIR_PATH  #
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")
datasets_obj = WorkingDatasetDir()
datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
dataset = datasets_obj.working_dataset


# dataset = shiny_app.get_working_dataset()
# datasets_dict = {dataset.name: dataset.name for dataset in shiny_app.get_all_datasets()}


def source_info_df(source):
    df = dataset.statement_files_info_df()
    df_res = df[df['source'] == source]
    return df_res


# def source_panel_factory(source_name, *args):
#     element_name_id = source_name.lower().replace(' ', '_')
#     """
#     @render.data_frame
#     def 'element_name_id'_api_source_info_text():
#         df = source_info_df(source_name)
#         logging.info(f"Source: {df=}")
#         return shiny_app.render_table_standard(df)
#     """
#
#     return ui.nav_panel(
#         source_name,
#         ui.card(
#             ui.output_data_frame(f"{element_name_id}_source_info_text"),
#             *args
#         )
#     )


app_ui = shiny_app.app_ui_factory(
    ui.card(
        ui.navset_tab(
            ui.nav_panel("Home",
                         ui.h3('Menu'),
                         ui.card(ui.output_text('links_output_text')),
                         ),
            ui.nav_panel("Datasets",
                         ui.card(
                             ui.h3("Working Directory"),
                             ui.output_text(id="current_dataset_directory"),
                             ui.input_action_button("change_dataset_dir_button", "Change working directory...",
                                                    disabled=True, width='300px'),
                         ),
                         ui.card(
                             ui.h3("Datasets"),
                             ui.input_select(
                                 id="dataset_select",
                                 label="Select dataset:",
                                 choices=datasets_dict,
                                 selected=datasets_obj.working_dataset.name if datasets_obj and datasets_obj.working_dataset and not datasets_obj.is_empty() else 'Something went wrong',
                                 multiple=False,
                             ),
                             ui.input_action_button("import_dataset_button", "Import dataset...", disabled=True,
                                                    width='300px'),
                         )),
            ui.nav_panel(
                'Data Flow',
                ui.input_task_button(id='fetch_data_button', label='Fetch new transaction data from providers'),
                ui.input_task_button(id='reset_button', label='Reset data from statements'),
                ui.accordion(
                    ui.accordion_panel('Sources', ui.card(
                        ui.output_ui('statements_info_text'),
                        ui.navset_tab(
                            ui.nav_panel("All", ui.output_data_frame('all_sources_info_text')),
                            ui.nav_panel("HSBC", ui.card(ui.output_data_frame('hsbc_source_info_text'))),
                            ui.nav_panel("Monzo",
                                         ui.card(
                                             ui.h3("Monzo Export"),
                                             ui.output_data_frame('monzo_export_source_info_text'),
                                         )),
                            ui.nav_panel("MonzoAPI",
                                         ui.card(
                                             ui.h3("Monzo API (*not integrated yet)"),
                                             ui.tags.a('Monzo authentication and fetching...',
                                                       href="http://127.0.0.1:8000/auth/monzo/"),
                                             ui.output_data_frame('monzo_api_source_info_text'),
                                         ),
                                         ),
                            ui.nav_panel("Revolut",
                                         ui.card(ui.output_data_frame('revo_source_info_text'))),
                            ui.nav_panel("HSBC Savings account",
                                         ui.card(ui.output_data_frame('hsbcsvr_source_info_text'))),
                            ui.nav_panel("Trading 212",
                                         ui.card(ui.output_data_frame('trd212_source_info_text'))),
                            ui.nav_panel("Invest Engine",
                                         ui.card(ui.output_data_frame('inveng_source_info_text'))),
                        )
                    )),
                    ui.accordion_panel('Statements', ui.card(
                        ui.output_data_frame("statements_info_dataframe")
                    )),
                    ui.accordion_panel('Transactions', ui.card(
                        ui.output_data_frame("transactions_info_dataframe")
                    )),
                    ui.accordion_panel('Tags', ui.card(
                        ui.card(
                            ui.h3("Tagging report"),
                            ui.output_data_frame("tags_info_dataframe"),
                        ),
                        ui.card(
                            ui.h3("Problematic Tagging conditions stats"),
                            ui.input_checkbox('compact_tag_conditions_stats_dataframe_checkbox',
                                              label='Compact table (grouped by Tag)',
                                              value=True),
                            ui.output_data_frame("tag_conditions_stats_dataframe"),
                        )
                    )),
                    ui.accordion_panel('Tagged Transactions', ui.card(
                        ui.output_data_frame("tagged_transactions_info_dataframe")
                    )),
                    id='data_flow_acc',
                    open=False
                )
            )
        )
    )
)


def create_tag_conditions_stats_dataframe(compact=True):
    monitor = RuleExecutionPlanMonitor(dataset)
    monitor.load()

    dm = CachedFileDataManager(dataset)
    df_stats = monitor.get_conditions_stats()
    df_types = dm.all_tags_df[['name', 'type']].rename(columns={'name': 'tag', 'type': 'tag_type'})
    df_merged = df_stats.merge(df_types, how='left', on='tag')
    df_merged['tag_type'].fillna('Unknown', inplace=True)

    df_sel = df_merged[
        (df_merged['tag_type'] != 'Built-in') & ((df_merged['all_true']) | (df_merged['all_false']))].copy()
    df_sel.replace([False, True], value=['False', 'True'], inplace=True)

    # df_sel['tag'] = df_sel['tag'].apply(lambda tag_name: f'<a href="{shiny_app.url_for_tag_edit(filter_in_tags=tag_name)}" target="_blank">Edit {tag_name}</a>')
    def make_link(tag_name: str):
        href = shiny_app.url_for_tag_edit(filter_in_tags=tag_name)
        # rel=noopener is a small security best-practice with target=_blank
        return ui.HTML(f'<a href="{href}" target="_blank" rel="noopener">Edit \'{tag_name}\'</a>')

    # Make sure every row becomes HTML (fill NAs if needed)
    df_sel["actions"] = df_sel["tag"].apply(lambda t: ui.HTML("") if t is None else make_link(t))

    if compact:
        def agg_strings_in_bulletpoints(arr):
            arr_str = [str(el).replace('<', '').replace('>', '') for el in arr]
            html_list = f"<ol><li>{'</li><li>'.join(arr_str)}</li></ol>"
            return ui.HTML(html_list)


        df_sel['all_true'] = df_sel['all_true'].replace({'True': 1, 'False': 0})
        df_sel['all_false'] = df_sel['all_false'].replace({'True': 1, 'False': 0})
        df_compact = df_sel.groupby('tag').agg({
            'type': lambda x: agg_strings_in_bulletpoints([f"{v}x {k}{'s' if v>1 else ''}" for k, v in pd.Series.value_counts(x).to_dict().items()]),
            'all_true': 'sum',
            'all_false': 'sum',
            'depending on': agg_strings_in_bulletpoints,
            'rule': agg_strings_in_bulletpoints,
            'priority': agg_strings_in_bulletpoints,
        }).reset_index()
        df_compact['actions'] = df_compact["tag"].apply(lambda t: ui.HTML("") if t is None else make_link(t))
        df_res = df_compact[['tag', 'actions', 'type', 'all_true', 'all_false', 'depending on', 'rule', 'priority']]
    else:
        df_res = df_sel

    logging.info(f"tag_conditions_stats_dataframe-> {df_res.shape=}, {compact=}")
    return df_res


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = shiny_app.create_data_manager()

    @render.text
    def links_output_text():
        # can also be a collapsable list (ui.accordion, ui.accordion_panel)
        markdown_text = ""
        dataset = shiny_app.get_working_dataset()
        links = dataset.settings.get('links', {})

        if len(links) == 0:
            return "No links found in dataset settings"

        for link_category, link_spec in links.items():
            markdown_text += f"### {link_category}\n"
            for link_name, link_url in link_spec.items():
                encode_url = link_url.replace(' ', '%20')
                logging.info(f"Link: {link_name} -> {encode_url}")
                markdown_text += f"* [{link_name}]({encode_url})\n"

        ui.insert_ui(
            ui=ui.markdown(markdown_text),
            selector='#links_output_text',
            where='beforeEnd'
        )
        return 'links'

    @render.text
    def current_dataset_directory() -> object:
        return f"Current directory: " + str(datasets_dir) if datasets_dir else 'No working directory found'

    @reactive.effect
    @reactive.event(input.dataset_select)
    def dataset_input_select_click_event():
        datasets_obj.set_working_dataset(input.dataset_select())
        datasets_obj.settings['CURRENT_DATASET'] = input.dataset_select()

    @render.text
    def db_info_text():
        info_json = WorkingDataManagerInfo().transactions_info()
        return info_json

    @reactive.effect
    @reactive.event(input.reset_db_button)
    def reset_db():
        WorkingDataManager().reset()

    @render.ui
    def statements_info_text():
        # TODO df['rows'].sum() is LESS than the numbers of transactions tagged as 'All', how?
        df = WorkingDatasetDir().working_dataset.statement_files_info_df()

        text = ui.HTML(
            f"""<p>Found <b>{len(df)} files</b>, containing <b>{df['rows'].sum()} rows</b> (* rows might not be 100% accurate).
             in total and <b>{df['source'].nunique()} different sources</b></p>""")
        return text

    @render.data_frame
    def all_sources_info_text():
        df = WorkingDatasetDir().working_dataset.statement_files_info_df()
        df_agg = df.groupby('source').agg({'filename': 'count', 'rows': 'sum'}).reset_index()
        return shiny_app.render_table_standard(df_agg)

    @render.data_frame
    def hsbc_source_info_text():
        df = source_info_df('HSBC')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def monzo_export_source_info_text():
        df = source_info_df('Monzo')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def monzo_api_source_info_text():
        df = source_info_df('MonzoAPI')
        logging.info(f"Source: {df=}")
        return shiny_app.render_table_standard(df)

    @reactive.effect
    @reactive.event(input.monzo_source_radio)
    def monzo_source_radio_change():
        logging.info(f"new monzo_source_radio={input.monzo_source_radio()}")
        curr_dataset = WorkingDatasetDir().working_dataset
        curr_dataset.settings['sources']['Monzo'] = input.monzo_source_radio()
        curr_dataset.settings.save()
        logging.info(
            f"new monzo_source_radio={input.monzo_source_radio()} saved to curr_dataset.settings['sources']['Monzo']")

    @render.data_frame
    def revo_source_info_text():
        df = source_info_df('Revolut')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def hsbcsvr_source_info_text():
        df = source_info_df('HSBCSVR')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def trd212_source_info_text():
        df = source_info_df('TRD212')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def inveng_source_info_text():
        df = source_info_df('INVENG')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def statements_info_dataframe():
        df = WorkingDatasetDir().working_dataset.statement_files_info_df()
        res = render.DataGrid(df, selection_mode="row")
        return res

    @reactive.effect
    def selected_statement():
        statements_selected = statements_info_dataframe.data_view(selected=True).to_dict('records')
        logging.info(f"Selected statement: {statements_selected}")

        if len(statements_selected) > 0:
            statement_dict = statements_selected[0]
            m = ui.modal(
                ui.output_data_frame(id='showing_statement_output_df'),
                title=f"Statement {statement_dict['filename']}",
                easy_close=True,
                size='xl'
            )
            ui.modal_show(m)

            @render.data_frame
            def showing_statement_output_df():
                df_stat = pd.read_csv(statement_dict['path'], index_col=None)
                return df_stat

    @render.data_frame
    def transactions_info_dataframe():
        df_trans = data_manager.get_transactions().dataframe()
        df_info = df_trans.describe(include='all').reset_index()
        res = render.DataGrid(df_info, selection_mode="row")
        return res

    @render.data_frame
    def tags_info_dataframe():
        df_tags_info = data_manager.get_tags_metadata()
        # res = render.DataGrid(df_tags_info, selection_mode="row")
        return shiny_app.render_table_standard(df_tags_info, format_columns=True)

    @render.data_frame
    def tag_conditions_stats_dataframe():
        df = create_tag_conditions_stats_dataframe(compact=input.compact_tag_conditions_stats_dataframe_checkbox())
        return shiny_app.render_table_standard(df, format_columns=True)

    @render.data_frame
    def tagged_transactions_info_dataframe():
        df_tags_info = pd.DataFrame.from_dict(data_manager.get_tagged_transactions().all_tag_counts(),
                                              orient='index').reset_index()
        df_tags_info.columns = ['tag', 'name']
        res = render.DataGrid(df_tags_info, selection_mode="row")
        return res

    @reactive.effect
    @reactive.event(input.fetch_data_button)
    def _():
        logging.info(f"Fetch data button")
        am = data_manager.get_statement_manager()
        fsources = am.get_sources_with_fetch_operation()
        for source in fsources:
            try:
                source.fetch()
            except Exception as e:
                logging.error(f"Failed to fetch source {source}: {e}")


    @reactive.effect
    @reactive.event(input.reset_button)
    def _():
        logging.info(f"Reset data")
        filepaths = data_manager.get_statement_filepaths()
        statement_source = set(filepaths.keys())
        transformer_sources = set(transformers.StatementTransformer.SOURCES)
        sources_with_no_transformers = statement_source.difference(transformer_sources)
        if len(sources_with_no_transformers) > 0:
            message = f"No parser for sources: {sources_with_no_transformers}\n"
            message += '\n'.join(
                [f" -> Skipping {len(filepaths[source])} statement file from  source '{source}'" for source in
                 sources_with_no_transformers])
            logging.info(f"App warning while resetting the data: {sources_with_no_transformers}, {message=}")
            ui.notification_show(
                f"WARNING:\n{message}",
                type="warning",
                duration=10,
                close_button=True
            )
        unparsed_sources = transformer_sources.difference(statement_source)
        if len(unparsed_sources) > 0:
            message = f"Source not parsed: {unparsed_sources}\n"
            # message += '\n'.join(
            #     [f" -> Skipping {len(filepaths[source])} statement file from  source '{source}'" for source in
            #      unparsed_sources])
            logging.info(f"App warning while resetting the data: {unparsed_sources}, {message=}")
            ui.notification_show(
                f"WARNING:\n{message}",
                type="warning",
                duration=10,
                close_button=True
            )

        data_manager.reset()


main_app = App(app_ui, server)
