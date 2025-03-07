# setup_logging()
import datetime
import logging
from urllib.parse import urlparse, parse_qs

from shiny import App, Inputs, Outputs, Session, render, ui, reactive
from shinywidgets import output_widget, render_widget

from mecon.app import shiny_app
from mecon.app.current_data import WorkingDataManager
from mecon.app import shiny_app
from mecon.data import graphs
from mecon.data import reports

# from mecon.monitoring.logs import setup_logging

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

DEFAULT_PERIOD = 'Last year'
DEFAULT_TIME_UNIT = 'month'

app_ui = shiny_app.app_ui_factory(
    ui.h5(ui.output_text('title_output')),
    ui.layout_sidebar(
        ui.sidebar(
            shiny_app.transactions_intersection_filted_factory()
        ),
        ui.page_fluid(
            ui.navset_tab(
                ui.nav_panel("Info",
                             ui.output_ui(id="info_stats"),
                             ),
                ui.nav_panel(
                    "Time",
                    ui.accordion(
                        ui.accordion_panel(
                            f"Timeline",
                            output_widget(id="amount_freq_plot")
                        ),
                        ui.accordion_panel(
                            f"Aggregated",
                            ui.card(
                                ui.input_radio_buttons(
                                    id='total_amount_or_count_radio',
                                    label='Aggregate on:',
                                    choices={'sum': 'Amount', 'count': 'Count'},
                                ),
                                output_widget(id="agg_amount_freq_plot"),
                            )
                        ),
                        id="total_amount_or_count_acc",
                        open=None,
                        multiple=True
                    ),
                ),
                ui.nav_panel("Balance",
                             output_widget(id="balance_plot"),
                             ),
                ui.nav_panel("Histogram",
                             ui.input_checkbox(
                                 id='show_bin_edges_flag',
                                 label='Show bin edges',
                                 value=False,
                             ),
                             output_widget(id="histogram_plot"),
                             ),
                ui.nav_panel("Table",
                             ui.output_data_frame(id="transactions_table"),
                             ),
                ui.nav_panel("Manage",
                             ui.h4(ui.output_ui(id='tag_edit_link')),
                             ui.card(
                                 ui.card_header("Make a custom report page"),
                                 ui.input_text(id='save_report_name', label='New report name'),
                                 ui.input_action_button(id='save_report_button', label='Save this report...')
                             ),
                             ),
            )
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = WorkingDataManager()

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

    @reactive.effect
    @reactive.event(input.save_report_button)
    def save_report_button_effect():
        filter_params = get_filter_params()
        logging.info(f"filter_params: {filter_params}")
        dataset = shiny_app.get_working_dataset()
        settings = dataset.settings
        saved_reports = settings['links']['Reports']

        new_report_name = input.save_report_name()
        if new_report_name is None or new_report_name == "" or new_report_name in saved_reports:
            message = 'Empty name' if (
                    new_report_name is None or new_report_name == "") else f"Report name already exists, all existing names: {', '.join(saved_reports.keys())}"
            ui.notification_show(
                f"Invalid name for the new report '{new_report_name}', {message=}",
                type="error",
            )
            return

        new_report_url = shiny_app.url_for_tag_report(**filter_params)
        saved_reports[
            new_report_name] = new_report_url  # TODO maybe make a class that deals with DatasetSettings for easier use and testing
        settings.save()

        ui.notification_show(
            f"Report '{new_report_name}' has been created successfully.",
            action=ui.tags.a(new_report_url, href=new_report_url),
            duration=10,
            type="message",
        )

    @render.text
    def title_output():
        return f"Tags: {filter_url_params()['filter_in_tags']}"

    @render.ui
    def tag_edit_link():
        return ui.tags.a("Edit tag", href=shiny_app.url_for_tag_edit(filter_in_tags=filter_url_params()['filter_in_tags'][0]))

    @render.ui
    def info_stats():
        logging.info(f"Info stats")
        total_amount_transactions = filtered_transactions().group_and_fill_transactions(
            grouping_key=input.time_unit_select(),
            aggregation_key='sum',
            fill_dates_after_groupagg=True,
        )
        logging.info(f"Info stats: {total_amount_transactions.size()=}")

        transactions_stats_markdown = reports.transactions_stats_markdown(total_amount_transactions,
                                                                          input.time_unit_select().lower())
        return ui.markdown(transactions_stats_markdown)

    @render_widget
    def amount_freq_plot() -> object:
        logging.info('amount_freq_plot')
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.filter_in_tags_select()
        plot = reports.amount_and_frequency_graph_report(filtered_transactions(),
                                                         start_date,
                                                         end_date,
                                                         grouping,
                                                         tags)
        return plot

    @render_widget
    def agg_amount_freq_plot() -> object:
        logging.info('agg_amount_freq_plot')
        trans = filtered_transactions()
        plot = graphs.time_aggregated_amount_and_frequency_fig(
            trans.datetime,
            trans.amount if input.total_amount_or_count_radio() == 'sum' else None,
        )
        return plot

    @render_widget
    def balance_plot() -> object:
        logging.info('balance_plot')
        grouping = input.time_unit_select()
        total_amount_transactions = filtered_transactions().group_and_fill_transactions(
            grouping_key=input.time_unit_select(),
            aggregation_key='sum',
            fill_dates_after_groupagg=True,
        )
        logging.info(f"balance_plot: {total_amount_transactions.size()=}")

        graph = graphs.balance_graph_fig(
            total_amount_transactions.datetime,
            total_amount_transactions.amount,
            fit_line=grouping != 'none'
        )
        return graph

    @render_widget
    def histogram_plot() -> object:
        logging.info('histogram_plot')
        total_amount_transactions = filtered_transactions().group_and_fill_transactions(
            grouping_key=input.time_unit_select(),
            aggregation_key='sum',
            fill_dates_after_groupagg=True,
        )
        logging.info(f"histogram_plot: {total_amount_transactions.size()=}")
        graph = graphs.histogram_and_contributions_fig(
            total_amount_transactions.amount,
            show_bin_edges=input.show_bin_edges_flag()
        )
        return graph

    @render.data_frame
    def transactions_table():
        logging.info('transactions_table')
        total_amount_transactions = filtered_transactions().group_and_fill_transactions(
            grouping_key=input.time_unit_select(),
            aggregation_key='sum',
            fill_dates_after_groupagg=True,
        )
        df = total_amount_transactions.dataframe()
        return render.DataTable(
            df,
            selection_mode="none",
            filters=True,
            styles=shiny_app.datatable_styles
        )


tag_info_app = App(app_ui, server)
