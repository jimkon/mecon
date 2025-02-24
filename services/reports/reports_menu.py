import logging

from shiny import App, Inputs, Outputs, Session, ui

from mecon import config
from mecon.app import shiny_modules
from mecon.app.current_data import WorkingDatasetDir, WorkingDataManager

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


dm = WorkingDataManager()
all_tags = dm.all_tags()


app_ui = ui.page_fluid(
    shiny_modules.title,
    shiny_modules.navbar,
    ui.hr(),

    ui.page_fluid(
        ui.h1('Not implemented yet')
    )

)


def server(input: Inputs, output: Outputs, session: Session):
    pass


reports_menu_app = App(app_ui, server)
