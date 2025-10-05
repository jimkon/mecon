import logging

from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon import config
from mecon.app import shiny_app
from mecon.app.current_data import WorkingDatasetDir


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")
datasets_obj = WorkingDatasetDir()
datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}


def format_current_dataset_directory_text(path) -> str:
    return f"Current directory: {path}" if path else 'No working directory found'


def update_working_dataset_selection(datasets_obj, dataset_name: str):
    datasets_obj.set_working_dataset(dataset_name)
    datasets_obj.settings['CURRENT_DATASET'] = dataset_name


app_ui = shiny_app.app_ui_factory(
    ui.card(
        ui.h3("Working Directory"),
        ui.output_text(id="current_dataset_directory"),
        ui.input_action_button("change_dataset_dir_button", "Change working directory...", disabled=True, width='300px'),
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
        ui.input_action_button("import_dataset_button", "Import dataset...", disabled=True, width='300px'),
    ),
)


def server(input: Inputs, output: Outputs, session: Session):
    @render.text
    def current_dataset_directory() -> object:
        return format_current_dataset_directory_text(datasets_dir)

    @reactive.effect
    @reactive.event(input.dataset_select)
    def dataset_input_select_click_event():
        update_working_dataset_selection(datasets_obj, input.dataset_select())


datasets_app = App(app_ui, server)

