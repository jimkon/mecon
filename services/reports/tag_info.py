# setup_logging()
import logging
import pathlib
from urllib.parse import urlparse, parse_qs

from shiny import App, Inputs, Outputs, Session, render, ui, reactive
from shinywidgets import output_widget, render_widget

from mecon.data import reports
from mecon.app.datasets import WorkingDataManager
from mecon.settings import Settings
from mecon.data import graphs

# from mecon.monitoring.logs import setup_logging

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

datasets_dir = pathlib.Path(__file__).parent.parent.parent / 'datasets'
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

settings = Settings()
settings['DATASETS_DIR'] = str(datasets_dir)

dm = WorkingDataManager()
all_tags = dm.all_tags()

transactions = dm.get_transactions()
working_transactions = transactions.copy()

app_ui = ui.page_fluid(
    ui.navset_pill(
        ui.nav_control(ui.tags.a("Main page", href=f"http://127.0.0.1:8000/")),
        ui.nav_control(ui.tags.a("Reports", href=f"http://127.0.0.1:8001/")),
        ui.nav_control(ui.tags.a("Edit data", href=f"http://127.0.0.1:8002/")),
        ui.nav_control(ui.tags.a("Monitoring", href=f"http://127.0.0.1:8003/")),
        ui.nav_control(ui.input_dark_mode(id="light_mode")),
    ),
    ui.tags.title("MEcon"),
    ui.h5(ui.output_text('title_output')),
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_select(
                id='date_period_input_select',
                label='Select date period',
                choices=['Last 30 days', 'Last 90 days', 'Last year', 'All']
            ),
            ui.input_date_range(  # TODO this must appear when custom period is selected
                id='transactions_date_range',
                label='Select date range',
                start=transactions.date_range()[0],
                end=transactions.date_range()[1]
            ),
            ui.input_radio_buttons(
                id='time_unit_select',
                label='Time unit',
                choices=['none', 'day', 'week', 'month', 'year'],
                selected='day'
            ),
            ui.input_selectize(
                id='input_tags_select',
                label='Select tags',
                choices=sorted([tag.name for tag in all_tags]),
                selected=None,
                multiple=True
            ),
            ui.input_action_button(
                id='reset_filter_values_button',
                label='Reset Values'
            )
        ),
        ui.page_fluid(
            ui.navset_tab(
                ui.nav_panel("Info",
                             ui.output_ui(id="info_stats"),
                             ),
                ui.nav_panel("Timeline",
                             output_widget(id="amount_freq_plot"),
                             ),
                ui.nav_panel("Balance",
                             output_widget(id="balance_plot"),
                             ),
                ui.nav_panel("Histogram",
                             output_widget(id="histogram_plot"),
                             ),
                ui.nav_panel("Table",
                             ui.output_data_frame(id="transactions_table"),
                             ),
            )
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    def reset_filter_inputs():
        urlparse_result = urlparse(input['.clientdata_url_search'].get())
        params = parse_qs(urlparse_result.query)

        default_tags = params.get('tags', [''])[0].split(',')
        ui.update_selectize(id='input_tags_select', selected=default_tags)

        start_date, end_date = working_transactions.date_range()
        min_date, max_date = transactions.date_range()
        ui.update_date_range(id='transactions_date_range',
                             start=start_date,
                             end=end_date,
                             min=min_date,
                             max=max_date, )

        time_unit = params.get('time_unit', 'month')[0]
        ui.update_radio_buttons(id='time_unit_select', selected=time_unit)
        return start_date, end_date, time_unit, default_tags

    @reactive.effect
    @reactive.event(input.make_light)
    def _():
        ui.update_dark_mode("light")

    @reactive.effect
    @reactive.event(input.make_dark)
    def _():
        ui.update_dark_mode("dark")

    @reactive.effect
    @reactive.event(input.reset_filter_values_button)
    def _():
        reset_filter_inputs()

    @render.text
    def title_output():
        start_date, end_date, time_unit, default_tags = reset_filter_inputs()
        return f"Report for tags: {','.join(default_tags)} between {start_date} to {end_date} grouped by {time_unit}"

    @render.ui
    def info_stats():
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.input_tags_select()
        total_amount_transactions = transactions.get_filtered_transactions(start_date,
                                                                           end_date,
                                                                           tags,
                                                                           grouping,
                                                                           aggregation_key='sum',
                                                                           fill_dates_after_groupagg=False)
        transactions_stats_markdown = reports.transactions_stats_markdown(total_amount_transactions,
                                                                          input.time_unit_select().lower())
        return ui.markdown(transactions_stats_markdown)

    @render_widget
    def amount_freq_plot() -> object:
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.input_tags_select()
        total_amount_transactions = transactions.get_filtered_transactions(start_date,
                                                                           end_date,
                                                                           tags,
                                                                           grouping,
                                                                           aggregation_key='sum',
                                                                           fill_dates_after_groupagg=False)
        total_amount_transactions = total_amount_transactions.fill_values(
            grouping if grouping != 'none' else 'day')

        if grouping != 'none':
            freq_transactions = transactions.get_filtered_transactions(start_date,
                                                                       end_date,
                                                                       tags,
                                                                       grouping,
                                                                       aggregation_key='count',
                                                                       fill_dates_after_groupagg=True)
        else:
            freq_transactions = None

        graph = graphs.amount_and_freq_timeline_fig(
            total_amount_transactions.datetime,
            total_amount_transactions.amount,
            freq_transactions.amount if freq_transactions is not None else None,
            grouping=grouping
        )
        return graph

    @render_widget
    def balance_plot() -> object:
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.input_tags_select()
        total_amount_transactions = transactions.get_filtered_transactions(start_date,
                                                                           end_date,
                                                                           tags,
                                                                           grouping,
                                                                           aggregation_key='sum',
                                                                           fill_dates_after_groupagg=True)

        graph = graphs.balance_graph_fig(
            total_amount_transactions.datetime,
            total_amount_transactions.amount,
            fit_line=grouping != 'none'
        )
        return graph

    @render_widget
    def histogram_plot() -> object:
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.input_tags_select()
        total_amount_transactions = transactions.get_filtered_transactions(start_date,
                                                                           end_date,
                                                                           tags,
                                                                           grouping,
                                                                           aggregation_key='sum',
                                                                           fill_dates_after_groupagg=False)

        graph = graphs.histogram_and_contributions_fig(
            total_amount_transactions.amount,
        )
        return graph

    @render.data_frame
    def transactions_table():
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.input_tags_select()
        total_amount_transactions = transactions.get_filtered_transactions(start_date,
                                                                           end_date,
                                                                           tags,
                                                                           grouping,
                                                                           aggregation_key='sum',
                                                                           fill_dates_after_groupagg=False)
        df = total_amount_transactions.dataframe()
        return df


tag_info_app = App(app_ui, server)
