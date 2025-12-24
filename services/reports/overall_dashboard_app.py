# setup_logging()
import logging

from shiny import App, Inputs, Outputs, Session, render, ui, reactive
from shiny.express.ui import accordion
import pandas as pd

from mecon.app import shiny_app
from mecon.app.current_data import WorkingDataManager
from mecon.data import additional_tags, graphs

import overall_dashboard_app_uitls as utils
from shinywidgets import render_widget, output_widget

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
                    ui.accordion(
                        ui.accordion_panel(
                            f"Aggregated",
                            ui.h3("stacked bard plot with all expenses"),
                            ui.navset_tab(
                                ui.nav_panel('Timeline', output_widget('banks_agg_graph')),
                                ui.nav_panel('Table', ui.output_data_frame('banks_agg_table'))
                            ),
                        ),
                        ui.accordion_panel(
                            f"Timelines",
                            ui.h3("separate plot of all expenses"),
                        ),
                        # id="monthly_basics_acc",
                        open=None,
                        multiple=True
                    ),
                    ui.h3('Sources'),
                    ui.output_data_frame('sources_table'),
                    ui.h3('All providers'),
                    ui.output_data_frame('providers_table'),
                ),
                ui.nav_panel(
                    "Finance",
                    ui.h3("Investments, Locked profits, ROI, Savings"),
                    ui.accordion(
                        ui.accordion_panel(
                            f"Aggregated",
                            ui.h3("stacked bard plot with all expenses"),
                            ui.navset_tab(
                                ui.nav_panel('Timeline', output_widget('finance_agg_graph')),
                                ui.nav_panel('Table', ui.output_data_frame('finance_agg_table'))
                            ),
                        ),
                        ui.accordion_panel(
                            f"Timelines",
                            ui.h3("separate plot of all expenses"),
                        ),
                        # id="monthly_basics_acc",
                        open=None,
                        multiple=True
                    ),
                ),
                ui.nav_panel(
                    "Monthly Basics",
                    ui.h3("Rent, Home bills, Subscriptions, Super Markets"),
                    ui.accordion(
                        ui.accordion_panel(
                            f"Aggregated",
                            ui.h3("stacked bard plot with all expenses"),
                            ui.navset_tab(
                                ui.nav_panel('Timeline', output_widget('monthly_basics_agg_graph')),
                                ui.nav_panel('Table', ui.output_data_frame('monthly_basics_agg_table'))
                            ),
                        ),
                        ui.accordion_panel(
                            f"Timelines",
                            ui.h3("separate plot of all expenses"),
                        ),
                        # id="monthly_basics_acc",
                        open=None,
                        multiple=True
                    ),

                ),
                ui.nav_panel(
                    "Monthly extras",
                    ui.h3("Eating out, Entertainment, Drinks, Online orders"),
                    ui.accordion(
                        ui.accordion_panel(
                            f"Aggregated",
                            ui.h3("stacked bard plot with all expenses"),
                            ui.navset_tab(
                                ui.nav_panel('Timeline', output_widget('monthly_extras_agg_graph')),
                                ui.nav_panel('Table', ui.output_data_frame('monthly_extras_agg_table'))
                            ),
                        ),
                        ui.accordion_panel(
                            f"Timelines",
                            ui.h3("separate plot of all expenses"),
                        ),
                        # id="monthly_basics_acc",
                        open=None,
                        multiple=True
                    ),
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

banks_tags = ['Monzo', 'HSBC', 'Revolut']
monthly_basics_tags = ['Rent', 'Home Bills', 'Subscription', 'Super Market']
monthly_extras_tags = ["Eating out", "Entertainment", "Drinks", "Online orders", 'Therapy']
finance_tags = ["Investments", "Savings", 'Interest']


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
        logging.info(f"Calculating sources table...")
        source_tag_names = {v['original_provider'] for k, v in providers_details.items()}
        source_sums = {tag: [transactions.containing_tags(tag).amount.sum()]
                       for tag in source_tag_names}
        results = pd.DataFrame(source_sums)
        logging.info(f"Calculating sources table...Done, {len(source_tag_names)} tags found.")
        return results

    @render.data_frame
    def providers_table():
        logging.info(f"Calculating providers table...")
        df = pd.DataFrame(providers_details).T
        df['tag_name'] = df['dir_name'].apply(lambda s: f"Source {s}")
        df['total_amount'] = df['tag_name'].apply(lambda tag: transactions.containing_tags(tag).amount.sum())
        logging.info(f"Calculating providers table...Done, {len(df)} tags found.")
        return shiny_app.render_table_standard(df, format_boolean_values=True)

    @reactive.calc
    def monthly_basics_agg_table_calc():
        logging.info(f"Calculating monthly basics agg table...")
        df = utils.tag_sums_table(
            transactions,
            tags=monthly_basics_tags
        )
        logging.info(f"Calculating monthly basics agg table... Done!")
        return df

    @render.data_frame
    def monthly_basics_agg_table():
        table = monthly_basics_agg_table_calc()
        return shiny_app.render_table_standard(table, format_boolean_values=True)

    @render_widget
    def monthly_basics_agg_graph():
        table = monthly_basics_agg_table_calc()
        return utils.tag_sums_graph(table, monthly_basics_tags)

    @reactive.calc
    def monthly_extras_agg_table_calc():
        logging.info(f"Calculating monthly extras agg table...")
        df = utils.tag_sums_table(
            transactions,
            tags=monthly_extras_tags
        )
        logging.info(f"Calculating monthly extras agg table... Done!")
        return df

    @render.data_frame
    def monthly_extras_agg_table():
        table = monthly_extras_agg_table_calc()
        return shiny_app.render_table_standard(table, format_boolean_values=True)

    @render_widget
    def monthly_extras_agg_graph():
        table = monthly_extras_agg_table_calc()
        return utils.tag_sums_graph(table, monthly_extras_tags)

    @reactive.calc
    def finance_agg_table_calc():
        logging.info(f"Calculating finance agg table...")
        df = utils.tag_sums_table(
            transactions,
            tags=finance_tags
        )
        logging.info(f"Calculating finance agg table... Done!")
        return df

    @render.data_frame
    def finances_agg_table():
        table = finance_agg_table_calc()
        return shiny_app.render_table_standard(table, format_boolean_values=True)

    @render_widget
    def finance_agg_graph():
        table = finance_agg_table_calc()
        return utils.tag_sums_graph(table, finance_tags)

    @reactive.calc
    def banks_agg_table_calc():
        logging.info(f"Calculating banks agg table...")
        df = utils.tag_sums_table(
            transactions,
            tags=banks_tags
        )
        logging.info(f"Calculating banks agg table... Done!")
        return df

    @render.data_frame
    def banks_agg_table():
        table = banks_agg_table_calc()
        return shiny_app.render_table_standard(table, format_boolean_values=True)

    @render_widget
    def banks_agg_graph():
        table = banks_agg_table_calc()
        return utils.tag_sums_graph(table, banks_tags)


dashboard_app = App(app_ui, server)
