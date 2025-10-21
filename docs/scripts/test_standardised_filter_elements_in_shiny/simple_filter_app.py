# setup_logging()
import logging

from shiny import App, Inputs, Outputs, Session, render, ui

from mecon.app import shiny_app
from mecon.data.data_management import CachedFileDataManager
from mecon.etl.dataset import Dataset

# from mecon.monitoring.logs import setup_logging

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

DEFAULT_PERIOD = 'Last year'
DEFAULT_TIME_UNIT = 'month'

app_ui = shiny_app.app_ui_factory(
    ui.layout_sidebar(
        ui.sidebar(
            shiny_app.transactions_intersection_filtered_factory()
        ),
        ui.page_fluid(
            ui.navset_tab(
                ui.nav_panel(
                    "Get filter params",
                    ui.output_ui(id="get_filter_params_text"),
                ),
                ui.nav_panel(
                    "Default transactions",
                    ui.output_data_frame(id="default_transactions_table"),
                ),
                ui.nav_panel(
                    "Filtered transactions",
                    ui.output_data_frame(id="filtered_transactions_table"),
                ),
            )
        )
    )
)

data_manager = CachedFileDataManager(
    Dataset(r"C:\Users\dimitris\PycharmProjects\mecon\tests\tests_with_datasets\datasets\test_full"))


def server(input: Inputs, output: Outputs, session: Session):
    (get_filter_params,
     default_transactions,
     init,
     filtered_transactions) = shiny_app.filter_funcs_factory(
        input,
        output,
        session,
        data_manager)

    @render.text
    def get_filter_params_text():
        return get_filter_params()

    @render.data_frame
    def default_transactions_table():
        df = default_transactions().dataframe()
        logging.info(f"default_transactions_table: {df.shape}")
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def filtered_transactions_table():
        df = filtered_transactions().dataframe()
        logging.info(f"filtered_transactions_table: {df.shape}")
        return shiny_app.render_table_standard(df)


app = App(app_ui, server)

if __name__ == "__main__":
    app.run(port=1234, launch_browser=True)
