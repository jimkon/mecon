from mecon.app.datasets import WorkingDatasetDir
from mecon.settings import Settings
# from mecon.monitoring.logs import setup_logging

from shiny import App, Inputs, Outputs, Session, render, ui

# setup_logging()
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

settings = Settings()

datasets_obj = WorkingDatasetDir(path=settings['DATASETS_DIR']) if 'DATASETS_DIR' in settings else None

datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.output_text(
            id="current_dataset_directory",
        ),
        # ui.input_file(
        #     id="dataset_dir_input",
        #     label="Choose working dir:",
        #     # accept=["/"],
        #     multiple=False
        # ),
        ui.input_select(
            id="dataset_select",
            label="Select dataset:",
            choices=datasets_dict,
            selected=datasets_obj.working_dataset.name if datasets_obj and not datasets_obj.is_empty() else 'Something went wrong',
            multiple=False,
        ),
        # ui.output_text(
        #     id="current_dataset_label"
        # ),
    ),
    ui.page_fillable(
        ui.output_text(
            id="current_dataset_label"
        ),
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @render.text
    def current_dataset_directory() -> object:
        return f"Working directory: "+settings['DATASETS_DIR'] if datasets_obj else 'No working directory found'

    @render.text
    def current_dataset_label() -> object:
        if datasets_obj is None:
            return 'Choose a working directory'

        datasets_obj.set_working_dataset(input.dataset_select())
        return f"Current dataset: {datasets_obj.working_dataset.name if datasets_obj.working_dataset else 'Dataset directory is empty.'}"
        # return 'current_dataset_label_example'

    @render.text
    def dataset_page():
        return 'dataset_page_example'


app = App(app_ui, server)
