# setup_logging()
import datetime
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

all_transactions = dm.get_transactions()

DEFAULT_PERIOD = 'Last year'
DEFAULT_TIME_UNIT = 'month'

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

    ui.h5(ui.output_text('title_output')),
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_select(
                id='date_period_input_select',
                label='Select date period',
                choices=['Last 30 days', 'Last 90 days', 'Last year', 'All'],
                selected=DEFAULT_PERIOD
            ),
            ui.input_date_range(
                id='transactions_date_range',
                label='Select date range',
                start=datetime.date.today() - datetime.timedelta(days=365),
                end=datetime.date.today(),
                format='dd-mm-yyyy',
                separator=':'
            ),
            ui.input_radio_buttons(
                id='time_unit_select',
                label='Time unit',
                choices=['none', 'day', 'week', 'month', 'year'],
                selected=DEFAULT_TIME_UNIT
            ),
            ui.input_selectize(
                id='input_tags_select',
                label='Select tags (union rows > 0)',
                choices=sorted([tag_name for tag_name, cnt in all_transactions.all_tags().items() if cnt > 0]),
                selected=None,
                multiple=True
            ),
            ui.input_task_button(
                id='reset_filter_values_button',
                label='Reset Values',
                label_busy='Filtering...'
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
            )
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    current_transactions = reactive.Value(None)

    @reactive.calc
    def url_params() -> dict:
        logging.info('Fetching URL params')
        urlparse_result = urlparse(input['.clientdata_url_search'].get())  # TODO move to a reactive.calc func
        _url_params = parse_qs(urlparse_result.query)
        params = {}
        params['tags'] = _url_params.get('tags', [''])[0].split(',')
        params['time_unit'] = _url_params.get('time_unit', DEFAULT_TIME_UNIT)
        logging.info(f"Input params: {params}")

        return params

    @reactive.effect
    def init():
        logging.info('Init')
        if current_transactions.get() is None:
            start_date, end_date, time_unit, tags = reset_filter_inputs()
            logging.info(f"Loading transactions from DB for {tags}...")
            current_transactions.set(
                all_transactions.get_filtered_transactions(
                    start_date=start_date,
                    end_date=end_date,
                    tags=tags
                )
            )
        logging.info(f"Updating UI")
        ui.update_select(id='date_period_input_select', selected=DEFAULT_PERIOD)

    @reactive.calc
    def reset_filter_inputs():
        logging.info('Reset filters')
        default_params = url_params()
        default_tags = default_params['tags']

        if input.date_period_input_select() == 'Last 30 days':
            start_date, end_date = datetime.date.today() - datetime.timedelta(days=30), datetime.date.today()
        elif input.date_period_input_select() == 'Last 90 days':
            start_date, end_date = datetime.date.today() - datetime.timedelta(days=90), datetime.date.today()
        elif input.date_period_input_select() == 'Last year':
            start_date, end_date = datetime.date.today() - datetime.timedelta(days=365), datetime.date.today()
        else:
            start_date, end_date = all_transactions.date_range()

        default_time_unit = default_params['time_unit']
        ui.update_radio_buttons(id='time_unit_select', selected=default_time_unit)

        new_choices = [tag_name for tag_name, cnt in all_transactions.containing_tag(default_tags).all_tags().items() if cnt > 0]
        ui.update_selectize(id='input_tags_select',
                            choices=sorted(new_choices),
                            selected=default_tags)

        return start_date, end_date, default_time_unit, default_tags

    @reactive.calc
    def get_filter_params():
        logging.info('Fetching filter params')
        start_date, end_date = input.date_range()
        time_unit = input.time_unit_select()
        tags = input.tags_select()
        return start_date, end_date, time_unit, tags

    @reactive.effect
    @reactive.event(input.reset_filter_values_button)
    def _():
        logging.info('Reset')
        reset_filter_inputs()
        ui.update_select(id='date_period_input_select', selected=DEFAULT_PERIOD)

    @reactive.effect
    @reactive.event(input.date_period_input_select)
    def _():
        logging.info(f"Changed period to '{input.date_period_input_select()}'")
        if input.date_period_input_select() == 'Last 30 days':
            start_date, end_date = datetime.date.today() - datetime.timedelta(days=30), datetime.date.today()
        elif input.date_period_input_select() == 'Last 90 days':
            start_date, end_date = datetime.date.today() - datetime.timedelta(days=90), datetime.date.today()
        elif input.date_period_input_select() == 'Last year':
            start_date, end_date = datetime.date.today() - datetime.timedelta(days=365), datetime.date.today()
        else:
            start_date, end_date = all_transactions.date_range()

        min_date, max_date = all_transactions.date_range()

        ui.update_date_range(id='transactions_date_range',
                             start=start_date,
                             end=min(max_date, end_date),
                             min=min_date,
                             max=max_date
                             )

    @render.text
    def title_output():
        logging.info('Title')
        start_date, end_date, time_unit, tags = get_filter_params()
        return f"Report for tags: {','.join(tags)} between {start_date} to {end_date} grouped by {time_unit}"

    @render.ui
    def info_stats():
        logging.info(f"Info stats")
        grouping = input.time_unit_select()
        start_date, end_date = input.transactions_date_range()
        tags = input.input_tags_select()
        total_amount_transactions = current_transactions.get().get_filtered_and_grouped_transactions(start_date,
                                                                                                     end_date,
                                                                                                     tags,
                                                                                                     grouping,
                                                                                                     aggregation_key='sum',
                                                                                                     fill_dates_after_groupagg=True)
        transactions_stats_markdown = reports.transactions_stats_markdown(total_amount_transactions,
                                                                          input.time_unit_select().lower())
        return ui.markdown(transactions_stats_markdown)

    @render_widget
    def amount_freq_plot() -> object:
        logging.info('amount_freq_plot')
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.input_tags_select()
        plot = reports.amount_and_frequency_graph_report(current_transactions.get(),
                                                         start_date,
                                                         end_date,
                                                         grouping,
                                                         tags)
        return plot

    @render_widget
    def balance_plot() -> object:
        logging.info('balance_plot')
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.input_tags_select()
        total_amount_transactions = current_transactions.get().get_filtered_and_grouped_transactions(start_date,
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
        logging.info('histogram_plot')
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.input_tags_select()
        total_amount_transactions = current_transactions.get().get_filtered_and_grouped_transactions(start_date,
                                                                                                     end_date,
                                                                                                     tags,
                                                                                                     grouping,
                                                                                                     aggregation_key='sum',
                                                                                                     fill_dates_after_groupagg=False)

        graph = graphs.histogram_and_contributions_fig(
            total_amount_transactions.amount,
            show_bin_edges=input.show_bin_edges_flag()
        )
        return graph

    @render.data_frame
    def transactions_table():
        logging.info('transactions_table')
        start_date, end_date = input.transactions_date_range()
        grouping = input.time_unit_select()
        tags = input.input_tags_select()
        total_amount_transactions = current_transactions.get().get_filtered_and_grouped_transactions(start_date,
                                                                                                     end_date,
                                                                                                     tags,
                                                                                                     grouping,
                                                                                                     aggregation_key='sum',
                                                                                                     fill_dates_after_groupagg=False)
        df = total_amount_transactions.dataframe()
        return df


tag_info_app = App(app_ui, server)
