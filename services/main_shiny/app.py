import logging

import pandas as pd
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon import config
from mecon.app import shiny_app
from mecon.app.current_data import WorkingDatasetDirInfo, WorkingDatasetDir, WorkingDataManagerInfo, WorkingDataManager
from mecon.etl import transformers

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# TODO settings are not refreshed if i change something manually. maybe shiny is caching stuff, because something similar happens to the data in the reports
# TODO need to rework the etl. statements should be treated as unique, don't check for duplicate rows between different statements. also, i should instantly convert them to transactions and add them to transactions table, skipping the bank statement tables entirely. the will reduce the db size, and complexity, and it will allow any data to be added by only adding the parser/etl converted

datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")
datasets_obj = WorkingDatasetDir()
datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
# dataset = datasets_obj.working_dataset


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
            # ui.nav_panel("DB", ui.page_fluid(
            #     ui.h3('Database content'),
            #     ui.output_text(id="db_info_text"),
            #     ui.input_task_button("reset_db_button", "Reset", label_busy='Might take up to a minute...',
            #                          width='300px'),
            # )),
            # ui.nav_panel("Statements", ui.page_fluid(
            #     ui.h3("DataFrame as HTML Table"),
            #     # ui.HTML(df)
            #     ui.output_data_frame("statements_info_dataframe")
            # )),
            # ui.nav_panel("Load statements", ui.page_fluid(
            #     ui.h3("Manually"),
            #     ui.card('Not implemented yet'),
            #     # ui.card(ui.input_file('import_statements_button',
            #     #                       button_label='Import statement',
            #     #                       accept=['.csv'],
            #     #                       # disabled=True,
            #     #                       width='300px')),
            #     ui.h3("From Monzo API"),
            #     ui.card('Not implemented yet'),
            # )),
            ui.nav_panel(
                'Data Flow',
                ui.input_task_button(id='reset_button', label='Reset data from statements'),
                ui.accordion(
                    ui.accordion_panel('Sources', ui.card(
                        ui.output_ui('statements_info_text'),
                        ui.navset_tab(
                            ui.nav_panel("All", ui.output_data_frame('all_sources_info_text')),
                            ui.nav_panel("HSBC", ui.card(ui.output_data_frame('hsbc_source_info_text'))),
                            ui.nav_panel("Monzo", ui.card(ui.output_data_frame('monzo_source_info_text'))),
                            ui.nav_panel("Revolut", ui.card(ui.output_data_frame('revo_source_info_text'))),
                            ui.nav_panel("HSBC Savings account", ui.card(ui.output_data_frame('hsbcsvr_source_info_text'))),
                            ui.nav_panel("Trading 212", ui.card(ui.output_data_frame('trd212_source_info_text'))),
                            ui.nav_panel("Invest Engine", ui.card(ui.output_data_frame('inveng_source_info_text'))),
                        )
                    )),
                    ui.accordion_panel('Statements', ui.card(
                        ui.output_data_frame("statements_info_dataframe")
                    )),
                    ui.accordion_panel('Transactions', ui.card(
                        ui.output_data_frame("transactions_info_dataframe")
                    )),
                    ui.accordion_panel('Tags', ui.card(
                        ui.output_data_frame("tags_info_dataframe")
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


def source_info_df(source):
    df = WorkingDatasetDirInfo().statement_files_info_df()
    df_res = df[df['source'] == source]
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
        df = WorkingDatasetDirInfo().statement_files_info_df()

        text = ui.HTML(
            f"""<p>Found <b>{len(df)} files</b>, containing <b>{df['rows'].sum()} rows</b> (* rows might not be 100% accurate).
             in total and <b>{df['source'].nunique()} different sources</b></p>""")
        return text

    @render.data_frame
    def all_sources_info_text():
        df = WorkingDatasetDirInfo().statement_files_info_df()
        df_agg = df.groupby('source').agg({'filename': 'count', 'rows': 'sum'}).reset_index()
        return shiny_app.render_table_standard(df_agg)

    @render.data_frame
    def hsbc_source_info_text():
        df = source_info_df('HSBC')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def monzo_source_info_text():
        df = source_info_df('Monzo')
        return shiny_app.render_table_standard(df)

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
        df = WorkingDatasetDirInfo().statement_files_info_df()
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
        res = render.DataGrid(df_tags_info, selection_mode="row")
        return res

    @render.data_frame
    def tagged_transactions_info_dataframe():
        df_tags_info = pd.DataFrame.from_dict(data_manager.get_tagged_transactions().all_tag_counts(),
                                              orient='index').reset_index()
        df_tags_info.columns = ['tag', 'name']
        res = render.DataGrid(df_tags_info, selection_mode="row")
        return res

    @reactive.effect
    @reactive.event(input.reset_button)
    def _():
        logging.info(f"Reset data")
        filepaths = data_manager.get_statement_filepaths()
        statement_source = set(filepaths.keys())
        transformer_sources = set(transformers.StatementTransformer.SOURCES)
        unparsed_sources = statement_source.difference(transformer_sources)
        if len(unparsed_sources) > 0:
            message = f"No parser for sources: {unparsed_sources}\n"
            message += '\n'.join(
                [f" -> Skipping {len(filepaths[source])} statement file from  source '{source}'" for source in
                 unparsed_sources])
            logging.info(f"Unparsed sources: {unparsed_sources}, {message=}")
            ui.notification_show(
                f"WARNING:\n{message}",
                type="warning",
                duration=10,
                close_button=True
            )
        data_manager.reset()


app = App(app_ui, server)
