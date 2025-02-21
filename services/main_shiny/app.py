import logging
from itertools import chain

import pandas as pd
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon import config
from mecon.app.current_data import WorkingDatasetDir, WorkingDatasetDirInfo, WorkingDataManagerInfo, WorkingDataManager
from mecon.etl import transformers
from mecon.etl.fetch_statement_files import transform_dates

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

# TODO settings are not refreshed if i change something manually. maybe shiny is caching stuff, because something similar happens to the data in the reports
# TODO need to rework the etl. statements should be treated as unique, don't check for duplicate rows between different statements. also, i should instantly convert them to transactions and add them to transactions table, skipping the bank statement tables entirely. the will reduce the db size, and complexity, and it will allow any data to be added by only adding the parser/etl converted

data_manager = WorkingDataManager()

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
                                 selected=datasets_obj.working_dataset.name if datasets_obj and not datasets_obj.is_empty() else 'Something went wrong',
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
                    # ui.accordion_panel('Import statements'),
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
                    id='data_flow_acc'
                )
            )
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @render.text
    def links_output_text():
        # can also be a collapsable list (ui.accordion, ui.accordion_panel)
        markdown_text = ""
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
        df_tags_info = pd.DataFrame.from_dict(data_manager.get_tagged_transactions().all_tag_counts(), orient='index').reset_index()
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
        unparsed_sources = statement_source.difference(transformer_sources) | {'Revolut'}
        if len(unparsed_sources) > 0:
            message = f"No parser for sources: {unparsed_sources}\n"
            message += '\n'.join([f" -> Skipping {len(filepaths[source])} statement file from  source '{source}'" for source in unparsed_sources])
            logging.info(f"Unparsed sources: {unparsed_sources}, {message=}")
            ui.notification_show(
                f"WARNING:\n{message}",
                type="warning",
                duration=10,
                close_button=True
            )
        data_manager.reset()


app = App(app_ui, server)
