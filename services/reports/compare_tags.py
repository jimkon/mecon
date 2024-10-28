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

    # ui.h5(ui.output_text('title_output')),
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
                choices=['day', 'week', 'month', 'year'], # 'none' doesn't make sense for comparisons
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
            ui.input_selectize(
                id='compare_tags_select',
                label='Select tags to show',
                choices=[],  # sorted([tag_name for tag_name, cnt in all_transactions.all_tags().items() if cnt > 0]),
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
                ui.nav_panel("Timelines",
                             output_widget(id="timelines"),
                             ),
                ui.nav_panel("Histograms",
                             output_widget(id="histograms"),
                             ),
                ui.nav_panel("Balance",
                             output_widget(id="balances"),
                             ),
            )
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @reactive.calc
    def url_params() -> dict:
        logging.info('Fetching URL params')
        urlparse_result = urlparse(input['.clientdata_url_search'].get())  # TODO move to a reactive.calc func
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

        logging.info(f"Updating filter In tags: {len(new_choices)} {params['filter_in_tags']}")
        ui.update_selectize(id='filter_in_tags_select',
                            choices=sorted(new_choices),
                            selected=params['filter_in_tags'])
        logging.info(f"Updating filter OUT tags: {len(all_tags_names)} {params['filter_out_tags']}")
        ui.update_selectize(id='filter_out_tags_select',
                            choices=all_tags_names,
                            selected=params['filter_out_tags'])
        logging.info(f"Updating compare tags: {len(new_choices)} {params['compare_tags']}")
        ui.update_selectize(id='compare_tags_select',
                            choices=sorted(new_choices),
                            selected=params['compare_tags'])
        logging.info(f"{input.filter_in_tags_select()=} {input.compare_tags_select()=}")

    @reactive.calc
    def get_filter_params():
        logging.info('Fetching filter params')
        start_date, end_date = input.transactions_date_range()
        time_unit = input.time_unit_select()
        filter_in_tags = input.filter_in_tags_select()
        filter_out_tags = input.filter_out_tags_select()
        compare_tags = input.compare_tags_select() if len(input.compare_tags_select()) > 0 else ['All']
        return start_date, end_date, time_unit, filter_in_tags, filter_out_tags, compare_tags

    @reactive.calc
    def all_ungrouped_transactions():
        start_date, end_date, time_unit, filter_in_tags, filter_out_tags, compare_tags = get_filter_params()
        logging.info(f"Calculating all transactions for {compare_tags}...")
        # TODO default_transactions are only using url_params, not the filters. consider removing the filters fully, or make them work
        transactions = default_transactions().select_date_range(start_date, end_date)
        all_trans = []
        for tag in compare_tags:
            trans = transactions.containing_tags(tag)
            logging.info(f"Transactions for {tag}: {trans.size()}")
            all_trans.append(trans)

        logging.info(f"Calculating all transactions... {len(all_trans)}")
        return all_trans, compare_tags

    def all_synced_and_grouped_transactions():
        all_trans, compare_tags = all_ungrouped_transactions()
        min_date = min([min(trans.datetime) for trans in all_trans])
        max_date = max([max(trans.datetime) for trans in all_trans])

        synced_trans = []
        for trans in all_trans:
            grouped_trans = trans.group_and_fill_transactions(
                grouping_key=input.time_unit_select(),
                aggregation_key='sum',
                fill_dates_after_groupagg=True,
            )
            filled_trans = grouped_trans.fill_values(fill_unit=input.time_unit_select(), start_date=min_date,
                                                     end_date=max_date)
            synced_trans.append(filled_trans)

        return synced_trans, compare_tags

    @render_widget
    def timelines():
        logging.info('timelines')
        synced_trans, tags = all_synced_and_grouped_transactions()

        plot = graphs.stacked_bars_graph_html(
            times=[trans.datetime for trans in synced_trans],
            lines=[trans.amount for trans in synced_trans],
            names=tags
        )
        return plot

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

    # @reactive.effect
    # @reactive.event(input.reset_filter_values_button)
    # def _():
    #     logging.info('Reset')
    #     reset_filter_inputs()
    #     ui.update_select(id='date_period_input_select', selected=DEFAULT_PERIOD)

    # @reactive.effect
    # @reactive.event(input.date_period_input_select)
    # def _():
    #     logging.info(f"Changed period to '{input.date_period_input_select()}'")
    #     if input.date_period_input_select() == 'Last 30 days':
    #         start_date, end_date = datetime.date.today() - datetime.timedelta(days=30), datetime.date.today()
    #     elif input.date_period_input_select() == 'Last 90 days':
    #         start_date, end_date = datetime.date.today() - datetime.timedelta(days=90), datetime.date.today()
    #     elif input.date_period_input_select() == 'Last year':
    #         start_date, end_date = datetime.date.today() - datetime.timedelta(days=365), datetime.date.today()
    #     else:
    #         start_date, end_date = all_transactions.date_range()
    #
    #     min_date, max_date = all_transactions.date_range()
    #
    #     ui.update_date_range(id='transactions_date_range',
    #                          start=start_date,
    #                          end=min(max_date, end_date),
    #                          min=min_date,
    #                          max=max_date
    #                          )
    #
    # @render.text
    # def title_output():
    #     logging.info('Title')
    #     start_date, end_date, time_unit, tags = get_filter_params()
    #     return f"Report for tags: {','.join(tags)} between {start_date} to {end_date} grouped by {time_unit}"


compare_tags_app = App(app_ui, server)
