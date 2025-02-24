import logging

from shiny import App, Inputs, Outputs, Session, ui

from mecon.app import shiny_app
from mecon.app.current_data import WorkingDataManager

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

dataset = shiny_app.get_working_dataset()


dm = WorkingDataManager()
all_tags = dm.all_tags()


app_ui = shiny_app.app_ui_factory(
    ui.page_fluid(
        ui.h1('Not implemented yet')
    )

)


def server(input: Inputs, output: Outputs, session: Session):
    pass


reports_menu_app = App(app_ui, server)
