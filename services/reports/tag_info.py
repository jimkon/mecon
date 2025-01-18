# setup_logging()
import datetime
import logging
from urllib.parse import urlparse, parse_qs

from shiny import App, Inputs, Outputs, Session, render, ui, reactive
from shinywidgets import output_widget, render_widget

from mecon import config
from mecon.app.file_system import WorkingDataManager, WorkingDatasetDir
from mecon.data import graphs
from mecon.data import reports

# from mecon.monitoring.logs import setup_logging

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
                id='filter_in_tags_select',
                label='Select tags to filter IN',
                choices=[],  # sorted([tag_name for tag_name, cnt in all_transactions.all_tags().items() if cnt > 0]),
                selected=None,
                multiple=True
            ),
            ui.input_selectize(
                id='filter_out_tags_select',
                label='Select tags to filter OUT',
                choices=[],  # sorted([tag_name for tag_name, cnt in all_transactions.all_tags().items() if cnt > 0]),
                selected=None,
                multiple=True
            ),
            # ui.input_task_button( # too much trouble for now, just do it manually or refresh the page
            #     id='reset_filter_values_button',
            #     label='Reset Values',
            #     label_busy='Filtering...'
            # )
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
            )
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @reactive.calc
    def url_params():
        urlparse_result = urlparse(input['.clientdata_url_search'].get())  # TODO move to a reactive.calc func
        logging.info(f"Fetched URL params: {urlparse_result=}")
        _url_params = parse_qs(urlparse_result.query)
        params = {}
        params['filter_in_tags'] = _url_params.get('filter_in_tags', [''])[0]
        params['filter_in_tags'] = params['filter_in_tags'].split(',') if len(params['filter_in_tags']) > 0 else []
        params['filter_out_tags'] = _url_params.get('filter_out_tags', [''])[0]
        params['filter_out_tags'] = params['filter_out_tags'].split(',') if len(params['filter_out_tags']) > 0 else []
        params['time_unit'] = _url_params.get('time_unit', DEFAULT_TIME_UNIT)
        params['compare_tags'] = _url_params.get('compare_tags', [''])[0].split(',')
        logging.info(f"Input params: {params}")

        return params

    @reactive.calc
    def default_transactions():
        params = url_params()
        filter_in_tags = params['filter_in_tags']
        filter_out_tags = params['filter_out_tags']
        transactions = dm.get_transactions() \
            .containing_tags(filter_in_tags) \
            .not_containing_tags(filter_out_tags, empty_tags_strategy='all_true')
        logging.info(f"URL param transactions: {transactions.size()=}")
        return transactions

    @reactive.effect
    def init():
        logging.info('Init')
        ui.update_select(id='date_period_input_select', selected=DEFAULT_PERIOD)

        params = url_params()
        transactions = default_transactions()
        all_tags_names = [tag.name for tag in dm.all_tags()]
        new_choices = [tag_name for tag_name, cnt in transactions.all_tags().items() if
                       cnt > 0]

        if len(input.filter_in_tags_select()) == 0:
            logging.info(f"Updating filter In tags: {len(new_choices)} {params['filter_in_tags']}")
            ui.update_selectize(id='filter_in_tags_select',
                                choices=sorted(new_choices),
                                selected=params['filter_in_tags'])

        if len(input.filter_out_tags_select()) == 0:
            logging.info(f"Updating filter OUT tags: {len(all_tags_names)} {params['filter_out_tags']}")
            ui.update_selectize(id='filter_out_tags_select',
                                choices=all_tags_names,
                                selected=params['filter_out_tags'])

        # if len(input.compare_tags_select()) == 0:  TODO
        #     logging.info(f"Updating compare tags: {len(new_choices)} {params['compare_tags']}")
        #     ui.update_selectize(id='compare_tags_select',
        #                         choices=sorted(new_choices),
        #                         selected=params['compare_tags'])

        logging.info(f"init->{input.filter_in_tags_select()=} {input.compare_tags_select()=}")

    # @reactive.calc
    # def reset_filter_inputs():
    #     logging.info('Reset filters')
    #     default_params = url_params()
    #     default_tags = default_params['tags']
    #
    #     if input.date_period_input_select() == 'Last 30 days':
    #         start_date, end_date = datetime.date.today() - datetime.timedelta(days=30), datetime.date.today()
    #     elif input.date_period_input_select() == 'Last 90 days':
    #         start_date, end_date = datetime.date.today() - datetime.timedelta(days=90), datetime.date.today()
    #     elif input.date_period_input_select() == 'Last year':
    #         start_date, end_date = datetime.date.today() - datetime.timedelta(days=365), datetime.date.today()
    #     else:
    #         start_date, end_date = all_transactions.date_range()
    #
    #     default_time_unit = default_params['time_unit']
    #     ui.update_radio_buttons(id='time_unit_select', selected=default_time_unit)
    #
    #     new_choices = [tag_name for tag_name, cnt in all_transactions.containing_tag(default_tags).all_tags().items() if
    #                    cnt > 0]
    #     ui.update_selectize(id='filter_in_tags_select',
    #                         choices=sorted(new_choices),
    #                         selected=default_tags)
    #
    #     return start_date, end_date, default_time_unit, default_tags

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

    @reactive.calc
    def get_filter_params():
        logging.info('Fetching filter params')
        start_date, end_date = input.transactions_date_range()
        time_unit = input.time_unit_select()
        filter_in_tags = input.filter_in_tags_select()
        filter_out_tags = input.filter_out_tags_select()
        return start_date, end_date, time_unit, filter_in_tags, filter_out_tags

    @reactive.calc
    def filtered_transactions():
        start_date, end_date, time_unit, filter_in_tags, filter_out_tags = get_filter_params()
        transactions = dm.get_transactions() \
            .select_date_range(start_date, end_date) \
            .containing_tags(filter_in_tags) \
            .not_containing_tags(filter_out_tags, empty_tags_strategy='all_true')
        logging.info(
            f"Filtered transactions size: {transactions.size()=} for filter params=({start_date, end_date, time_unit, filter_in_tags, filter_out_tags})")

        if transactions.size() == 0:
            raise Exception(
                f"Filtering resulted in zero transactions: params=({start_date=}, {end_date=}, {time_unit=}, {filter_in_tags=}, {filter_out_tags})")

        return transactions

    @render.text
    def title_output():
        return f"Tags: {url_params()['filter_in_tags']}"

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
        return df


tag_info_app = App(app_ui, server)
