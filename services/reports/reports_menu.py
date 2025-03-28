import logging

from shiny import App, Inputs, Outputs, Session, ui

from mecon.app import shiny_app

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

dataset = shiny_app.get_working_dataset()




app_ui = shiny_app.app_ui_factory(
    ui.page_fluid(
        ui.h1('Not implemented yet')
    )

)


def server(input: Inputs, output: Outputs, session: Session):
    dm = shiny_app.create_data_manager()
    all_tags = dm.all_tags()
    pass


reports_menu_app = App(app_ui, server)
