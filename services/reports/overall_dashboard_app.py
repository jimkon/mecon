# setup_logging()
import logging

from shiny import App, Inputs, Outputs, Session, render, ui
import pandas as pd

from mecon.app import shiny_app
from mecon.app.current_data import WorkingDataManager
from mecon.data import additional_tags

# from mecon.monitoring.logs import setup_logging

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

DEFAULT_PERIOD = 'Last year'
DEFAULT_TIME_UNIT = 'month'

app_ui = shiny_app.app_ui_factory(
    ui.h3("Main Dashboard"),
    ui.layout_sidebar(
        ui.sidebar(
            shiny_app.transactions_intersection_filtered_factory()
        ),
        ui.page_fluid(
            ui.navset_tab(
                ui.nav_panel(
                    "General",
                    ui.h3("Monthly totals, Activities, Tags summary"),
                ),
                ui.nav_panel(
                    "Data sources",
                    ui.output_data_frame('sources_table'),
                    ui.output_data_frame('providers_table')
                ),
                ui.nav_panel(
                    "Finance",
                    ui.h3("Investments, Locked profits, ROI, Savings"),
                ),
                ui.nav_panel(
                    "Monthly Basics",
                    ui.h3("Rent, Home bills, Subscriptions, Super Markets"),
                ),
                ui.nav_panel(
                    "Monthly extras",
                    ui.h3("Eating out, Entertainment, Drinks, Online orders"),
                ),
                ui.nav_panel(
                    "Holidays",
                    ui.h3("Holidays, locations, etc"),
                )
            )
        )
    )
)

data_manager = WorkingDataManager()
dataset = data_manager.dataset
transactions = data_manager.transactions.build_tags_lookup()
providers_details = additional_tags.get_providers_details(dataset)


def server(input: Inputs, output: Outputs, session: Session):
    filter_url_params = shiny_app.filter_url_params_function_factory(
        input,
        output,
        session,
        data_manager)

    (get_filter_params,
     default_transactions,
     init,
     filtered_transactions) = shiny_app.filter_funcs_factory(
        input,
        output,
        session,
        data_manager)

    @render.data_frame
    def sources_table():
        logging.info(f"Calculation sources table...")
        source_tag_names = {v['original_provider'] for k, v in providers_details.items()}
        source_sums = {tag: [filtered_transactions().containing_tags(tag).amount.sum()]
                       for tag in source_tag_names}
        results = pd.DataFrame(source_sums)
        logging.info(f"Calculation sources table...Done, {len(source_tag_names)} tags found.")
        return results

    @render.data_frame
    def providers_table():
        logging.info(f"Calculation providers table...")
        df = pd.DataFrame(providers_details).T
        df['tag_name'] = df['dir_name'].apply(lambda s: f"Source {s}")
        df['total_amount'] = df['tag_name'].apply(lambda tag: filtered_transactions().containing_tags(tag).amount.sum())
        logging.info(f"Calculation providers table...Done, {len(df)} tags found.")
        return shiny_app.render_table_standard(df, format_boolean_values=True)


dashboard_app = App(app_ui, server)
