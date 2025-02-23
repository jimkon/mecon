import logging
import pathlib

from shiny import App, Inputs, Outputs, Session, ui

from mecon.app.current_data import WorkingDataManager
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

dm = WorkingDataManager()
all_tags = dm.all_tags()


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

    ui.page_fluid(
        ui.h1('Not implemented yet')
    )

)


def server(input: Inputs, output: Outputs, session: Session):
    pass


reports_menu_app = App(app_ui, server)
