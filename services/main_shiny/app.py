# setup_logging()
import json
import logging
import time

import pandas as pd
from json2html import json2html
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app.datasets import WorkingDatasetDir, WorkingDatasetDirInfo, WorkingDataManagerInfo, WorkingDataManager
from mecon.settings import Settings

# from mecon.monitoring.logs import setup_logging

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

settings = Settings()

datasets_obj = WorkingDatasetDir(path=settings['DATASETS_DIR']) if 'DATASETS_DIR' in settings else None

datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}

# df = WorkingDatasetDirInfo().statement_files_info_df()  # .to_html(classes='table table-striped', border=0)

app_ui = ui.page_fluid(
    ui.navset_tab(
        ui.nav_panel("Home", 'Nothing here...'),
        ui.nav_panel("Datasets",
                     ui.card(
                         ui.h2("Working Directory"),
                         ui.output_text(id="current_dataset_directory"),
                         ui.input_action_button("change_dataset_dir_button", "Change working directory...",
                                                disabled=True, width='300px'),
                     ),
                     ui.card(
                         ui.h2("Datasets"),
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
        ui.nav_panel("DB", ui.page_fluid(
            ui.h2('Database content'),
            ui.output_text(id="db_info_text"),
            ui.input_task_button("reset_db_button", "Reset", label_busy='Might take up to a minute...', width='300px'),
        )),
        ui.nav_panel("Statements", ui.page_fluid(
            ui.h2("DataFrame as HTML Table"),
            # ui.HTML(df)
            ui.output_data_frame("statements_info_dataframe")
        )),
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @render.text
    def current_dataset_directory() -> object:
        return f"Current directory: " + settings['DATASETS_DIR'] if datasets_obj else 'No working directory found'

    @reactive.effect
    @reactive.event(input.dataset_select)
    def dataset_input_select_click_event():
        datasets_obj.set_working_dataset(input.dataset_select())
        datasets_obj.settings['CURRENT_DATASET'] = input.dataset_select()

    @render.text
    def db_info_text():
        info_json = WorkingDataManagerInfo().db_transactions_info()
        return info_json

    @reactive.effect
    @reactive.event(input.reset_db_button)
    def reset_db():
        WorkingDataManager().reset_db()

    @render.data_frame
    def statements_info_dataframe():
        return WorkingDatasetDirInfo().statement_files_info_df()

app = App(app_ui, server)
