import logging
import pathlib
from urllib.parse import quote

from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app.datasets import WorkingDatasetDir, WorkingDatasetDirInfo, WorkingDataManagerInfo, WorkingDataManager
from mecon.settings import Settings

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

datasets_dir = pathlib.Path(__file__).parent.parent.parent / 'datasets'
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")


settings = Settings()
settings['DATASETS_DIR'] = str(datasets_dir)

datasets_obj = WorkingDatasetDir()

datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}

dataset = WorkingDatasetDir().working_dataset


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
            ui.nav_panel("DB", ui.page_fluid(
                ui.h3('Database content'),
                ui.output_text(id="db_info_text"),
                ui.input_task_button("reset_db_button", "Reset", label_busy='Might take up to a minute...',
                                     width='300px'),
            )),
            ui.nav_panel("Statements", ui.page_fluid(
                ui.h3("DataFrame as HTML Table"),
                # ui.HTML(df)
                ui.output_data_frame("statements_info_dataframe")
            )),
            ui.nav_panel("Load statements", ui.page_fluid(
                ui.h3("Manually"),
                ui.card('Not implemented yet'),
                # ui.card(ui.input_file('import_statements_button',
                #                       button_label='Import statement',
                #                       accept=['.csv'],
                #                       # disabled=True,
                #                       width='300px')),
                ui.h3("From Monzo API"),
                ui.card('Not implemented yet'),
            )),
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @render.text
    def links_output_text():
        # can also be a collapsable list (ui.accordion, ui.accordion_panel)
        markdown_text = ""
        links = dataset.settings.get('links', {})
        for link_category, link_spec in links.items():
            markdown_text += f"### {link_category}\n"
            for link_name, link_url in link_spec.items():
                encode_url = quote(link_url)
                markdown_text += f"* [{link_name}]({encode_url})\n"

        ui.insert_ui(
            ui=ui.markdown(markdown_text),
            selector='#links_output_text',
            where='beforeEnd'
        )

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
