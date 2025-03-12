import datetime
import logging

from shiny import ui, Inputs, Outputs, Session, reactive, render

from mecon import config
from mecon.app.current_data import WorkingDataManager, WorkingDatasetDir
from mecon.utils.html import build_url

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


# datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
# if not datasets_dir.exists():
#     raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")
#
# datasets_obj = WorkingDatasetDir()
# datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
# dataset = datasets_obj.working_dataset

def get_working_dataset():
    datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
    if not datasets_dir.exists():
        raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

    datasets_obj = WorkingDatasetDir()
    dataset = datasets_obj.working_dataset

    if dataset is None:
        raise ValueError(f"Unable to locate working dataset: {datasets_obj.working_dataset=}")

    return dataset


def create_data_manager():
    logging.info("Creating data manager")
    return WorkingDataManager()


def url_for_tag_report(**kwargs):
    url = build_url("http://127.0.0.1:8001/reports/tags/", kwargs)
    return url

def url_for_comparison_report(**kwargs):
    url = build_url("http://127.0.0.1:8001/reports/compare/", kwargs)
    return url

def url_for_tag_edit(**kwargs):
    url = build_url("http://127.0.0.1:8002/edit_data/tags/edit/", kwargs)
    return url


# dm = WorkingDataManager()
# all_tags = dm.all_tags()
#
# all_transactions = dm.get_transactions()

tab_title = ui.tags.title("μEcon App")
page_title = ui.HTML('<h2>mEcon<sub><small><u><i>v3</i></u></small></sub></h2>')
navbar = ui.navset_pill(
    ui.nav_control(ui.tags.a("Main page", href=f"http://127.0.0.1:8000/")),
    ui.nav_control(ui.tags.a("Reports", href=f"http://127.0.0.1:8001/reports/")),
    ui.nav_control(ui.tags.a("Edit data", href=f"http://127.0.0.1:8002/edit_data/")),
    # ui.nav_control(ui.tags.a("Monitoring", href=f"http://127.0.0.1:8003/")),
    # ui.nav_control(ui.input_dark_mode(id="light_mode")),
)


def app_ui_factory(*args):
    return ui.page_fluid(
        tab_title,
        page_title,
        navbar,
        ui.hr(),
        *args
    )


DEFAULT_FILTER_PERIOD = config.SHINY_DEFAULT_FILTER_PERIOD
DEFAULT_FILTER_TIME_UNIT = config.SHINY_DEFAULT_FILTER_TIME_UNIT


def transactions_intersection_filted_factory():
    # TODO add custom date period option
    # TODO add date period in url params, with higher priority from the date range one
    # TODO move filter to shiny_apps, have to understand how the reactive will be modularized

    return ui.card(
        ui.input_select(
            id='date_period_input_select',
            label='Select date period',
            choices=['Last 30 days', 'Last 90 days', 'Last year', 'All'],
            selected=DEFAULT_FILTER_PERIOD
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
            selected=DEFAULT_FILTER_TIME_UNIT
        ),
        ui.input_selectize(
            id='filter_in_tags_select',
            label='Select tags to filter IN',
            choices=[],  # sorted([tag_name for tag_name, cnt in all_transactions.all_tag_counts().items() if cnt > 0]),
            selected=None,
            multiple=True
        ),
        ui.input_selectize(
            id='filter_out_tags_select',
            label='Select tags to filter OUT',
            choices=[],  # sorted([tag_name for tag_name, cnt in all_transactions.all_tag_counts().items() if cnt > 0]),
            selected=None,
            multiple=True
        ),
        # ui.input_task_button( # too much trouble for now, just do it manually or refresh the page
        #     id='reset_filter_values_button',
        #     label='Reset Values',
        #     label_busy='Filtering...'
        # )
    )


class ShinyTransactionFilterError(ValueError):
    def __init__(self, message):
        message = f"ShinyTransactionFilterError: {message}"
        ui.notification_show(
            message,
            type="error",
            duration=None,
            close_button=True
        )
        super().__init__(message)


def url_params_function_factory(input: Inputs,
                                output: Outputs,
                                session: Session,
                                data_manager: WorkingDataManager, ):
    from urllib.parse import urlparse, parse_qs

    @reactive.calc
    def url_params() -> dict:
        logging.info(f"{input['.clientdata_url_search'].get()=}")
        urlparse_result = urlparse(input['.clientdata_url_search'].get())  # TODO move to a reactive.calc func
        logging.info(f"Fetched URL params: {urlparse_result=}")
        _url_params = parse_qs(urlparse_result.query)
        logging.info(f"Input params: {_url_params=}")
        return _url_params
    return url_params

def filter_url_params_function_factory(input: Inputs,
                                output: Outputs,
                                session: Session,
                                data_manager: WorkingDataManager, ):

    @reactive.calc
    def filter_url_params():
        _url_params = url_params_function_factory(input, output, session, data_manager)()
        params = {}
        params['filter_in_tags'] = _url_params.get('filter_in_tags', [''])[0]
        params['filter_in_tags'] = params['filter_in_tags'].split(',') if len(params['filter_in_tags']) > 0 else []
        params['filter_out_tags'] = _url_params.get('filter_out_tags', [''])[0]
        params['filter_out_tags'] = params['filter_out_tags'].split(',') if len(params['filter_out_tags']) > 0 else []
        params['time_unit'] = _url_params.get('time_unit', DEFAULT_FILTER_TIME_UNIT)
        logging.info(f"Input params: {params=}")
        return params
    return filter_url_params


def filter_funcs_factory(
        input: Inputs,
        output: Outputs,
        session: Session,
        data_manager: WorkingDataManager,
):
    @reactive.calc
    def get_filter_params():
        logging.info('Fetching filter params')
        start_date, end_date = input.transactions_date_range()
        time_unit = input.time_unit_select()
        filter_in_tags = input.filter_in_tags_select()
        filter_out_tags = input.filter_out_tags_select()
        filter_params_dict = {
            'start_date': start_date,
            'end_date': end_date,
            'time_unit': time_unit,
            'filter_in_tags': filter_in_tags,
            'filter_out_tags': filter_out_tags
        }
        return filter_params_dict

    @reactive.calc
    def default_transactions():
        filter_url_params = filter_url_params_function_factory(input, output, session, data_manager)()
        filter_in_tags = filter_url_params['filter_in_tags']
        filter_out_tags = filter_url_params['filter_out_tags']
        transactions = data_manager.get_transactions()
        filtered_in_transactions = transactions.containing_tags(filter_in_tags)
        if filtered_in_transactions.size() == 0:
            error_msg = f"No transactions found for {filter_url_params['time_unit']} time unit containing {params['filter_in_tags']} tags."
            raise ShinyTransactionFilterError(error_msg)

        filtered_in_and_out_transactions = filtered_in_transactions.not_containing_tags(filter_out_tags,
                                                                                        empty_tags_strategy='all_true')
        if filtered_in_and_out_transactions.size() == 0:
            error_msg = f"No transactions found for {filter_url_params['time_unit']} time unit after filtering out {params['filter_in_tags']} tags."
            raise ShinyTransactionFilterError(error_msg)

        logging.info(f"URL param transactions: {filtered_in_and_out_transactions.size()=}")
        return filtered_in_and_out_transactions

    @reactive.effect
    def init():
        logging.info('Init')
        ui.update_select(id='date_period_input_select', selected=DEFAULT_FILTER_PERIOD)
        filter_url_params = filter_url_params_function_factory(input, output, session, data_manager)()
        transactions = default_transactions()
        all_tags_names = [tag.name for tag in data_manager.all_tags()]
        new_choices = [tag_name for tag_name, cnt in transactions.all_tag_counts().items() if
                       cnt > 0]

        if len(input.filter_in_tags_select()) == 0:
            logging.info(f"Updating filter In tags: {len(new_choices)} {filter_url_params['filter_in_tags']}")
            ui.update_selectize(id='filter_in_tags_select',
                                choices=sorted(new_choices),
                                selected=filter_url_params['filter_in_tags'])

        if len(input.filter_out_tags_select()) == 0:
            logging.info(f"Updating filter OUT tags: {len(all_tags_names)} {filter_url_params['filter_out_tags']}")
            ui.update_selectize(id='filter_out_tags_select',
                                choices=all_tags_names,
                                selected=filter_url_params['filter_out_tags'])

        # if len(input.compare_tags_select()) == 0:  TODO
        #     logging.info(f"Updating compare tags: {len(new_choices)} {filter_url_params['compare_tags']}")
        #     ui.update_selectize(id='compare_tags_select',
        #                         choices=sorted(new_choices),
        #                         selected=filter_url_params['compare_tags'])

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
    #     new_choices = [tag_name for tag_name, cnt in all_transactions.containing_tag(default_tags).all_tag_counts().items() if
    #                    cnt > 0]
    #     ui.update_selectize(id='filter_in_tags_select',
    #                         choices=sorted(new_choices),
    #                         selected=default_tags)
    #
    #     return start_date, end_date, default_time_unit, default_tags

    @reactive.effect
    @reactive.event(input.date_period_input_select)
    def period_change_effect():
        _all_transactions = data_manager.get_transactions()
        logging.info(f"Changed period to '{input.date_period_input_select()}'")
        if input.date_period_input_select() == 'Last 30 days':
            start_date, end_date = datetime.date.today() - datetime.timedelta(days=30), datetime.date.today()
        elif input.date_period_input_select() == 'Last 90 days':
            start_date, end_date = datetime.date.today() - datetime.timedelta(days=90), datetime.date.today()
        elif input.date_period_input_select() == 'Last year':
            start_date, end_date = datetime.date.today() - datetime.timedelta(days=365), datetime.date.today()
        else:
            start_date, end_date = _all_transactions.date_range()

        min_date, max_date = _all_transactions.date_range()
        logging.info(f"date_range set to {min_date=} and {max_date=}")
        # if transactions.size()==0:
        #     ui.update_select(id='date_period_input_select', selected="All")

        ui.update_date_range(id='transactions_date_range',
                             start=start_date,
                             end=min(max_date, end_date),
                             min=min_date,
                             max=max_date
                             )

    @reactive.calc
    def filtered_transactions():
        start_date, end_date, time_unit, filter_in_tags, filter_out_tags = get_filter_params().values()
        transactions = data_manager.get_transactions()

        in_date_range_transactions = transactions.select_date_range(start_date, end_date)
        if in_date_range_transactions.size() == 0:
            error_msg = f"No transactions found for '{time_unit}' time unit in given date range {start_date} to {end_date}."
            raise ShinyTransactionFilterError(error_msg)

        filtered_in_transactions = in_date_range_transactions.containing_tags(filter_in_tags)
        if filtered_in_transactions.size() == 0:
            error_msg = f"No transactions found for '{time_unit}' time unit containing {filter_in_tags} tags."
            raise ShinyTransactionFilterError(error_msg)

        filtered_in_and_out_transactions = filtered_in_transactions.not_containing_tags(filter_out_tags,
                                                                                        empty_tags_strategy='all_true')
        if filtered_in_and_out_transactions.size() == 0:
            error_msg = f"No transactions found for '{time_unit}' time unit after filtering out {filter_out_tags} tags."
            raise ShinyTransactionFilterError(error_msg)

        logging.info(
            f"Filtered transactions size: {filtered_in_and_out_transactions.size()=} for filter params=({start_date, end_date, time_unit, filter_in_tags, filter_out_tags})")

        return filtered_in_and_out_transactions

    return get_filter_params, default_transactions, init, filtered_transactions


filter_menu = ui.sidebar(
    ui.input_select(
        id='date_period_input_select',
        label='Select date period',
        choices=['Last 30 days', 'Last 90 days', 'Last year', 'All'],
        selected=DEFAULT_FILTER_PERIOD
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
        selected=DEFAULT_FILTER_TIME_UNIT
    ),
    ui.input_selectize(
        id='filter_in_tags_select',
        label='Select tags to filter IN',
        choices=[],  # sorted([tag_name for tag_name, cnt in all_transactions.all_tag_counts().items() if cnt > 0]),
        selected=None,
        multiple=True
    ),
    ui.input_selectize(
        id='filter_out_tags_select',
        label='Select tags to filter OUT',
        choices=[],  # sorted([tag_name for tag_name, cnt in all_transactions.all_tag_counts().items() if cnt > 0]),
        selected=None,
        multiple=True
    ),
    # ui.input_task_button( # too much trouble for now, just do it manually or refresh the page
    #     id='reset_filter_values_button',
    #     label='Reset Values',
    #     label_busy='Filtering...'
    # )
)

datatable_styles = [
    {
        "location": "body",
        "style": {
            "background-color": "grey",
            "border": "0.5px solid black",
            'font-size': '14px',
            'color': 'black'
        },
    },
    {
        "location": "body",
        "cols": [0],
        "style": {
            'font-size': '8px',
        },
    },
    {
        "location": "body",
        "cols": [1],
        "style": {
            'width': '400px',
        },
    }
]


def render_table_standard(df):
    return render.DataTable(
        df,
        selection_mode="none",
        filters=True,
        styles=datatable_styles
    )