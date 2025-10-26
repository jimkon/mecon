import datetime
import logging
from urllib.parse import urlparse, parse_qs

import dateparser
import pandas as pd
from shiny import ui, Inputs, Outputs, Session, reactive, render, req

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

def get_all_datasets():
    datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
    if not datasets_dir.exists():
        raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")
    datasets_obj = WorkingDatasetDir()
    return datasets_obj.datasets()


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
page_title = ui.HTML(f"<big><big><big>mEcon</big></big></big><sub><small><u><i>v{config.MECON_VERSION}</i></u></small></sub><br>")
dataset_label = ui.tooltip(ui.HTML(f"<sub><small>Selected dataset: {get_working_dataset().name}</small></sub>"), f"Dataset directory: {config.DEFAULT_DATASETS_DIR_PATH}")
navbar = ui.navset_pill(
    ui.nav_control(ui.tags.a("Main page", href=f"http://127.0.0.1:8000/")),
    ui.nav_control(ui.tags.a("Datasets", href=f"http://127.0.0.1:8000/datasets")),
    ui.nav_control(ui.tags.a("Data Flow", href=f"http://127.0.0.1:8000/data")),
    ui.nav_control(ui.tags.a("Edit data", href=f"http://127.0.0.1:8002/edit_data/")),
    # ui.nav_control(ui.tags.a("Monitoring", href=f"http://127.0.0.1:8003/")),
    # ui.nav_control(ui.input_dark_mode(id="light_mode")),
)


def app_ui_factory(*args):
    return ui.page_fluid(
        # TODO add icon
        # ui.tags.link(rel="icon", type="image/x-icon", href="favicon.ico"),
        # https://forum.posit.co/t/how-to-add-an-logo-in-the-header-in-pythonshiny/189333
        tab_title,
        page_title,
        dataset_label,
        navbar,
        ui.hr(),
        *args
    )


DEFAULT_FILTER_PERIOD = config.SHINY_DEFAULT_FILTER_PERIOD
DEFAULT_FILTER_TIME_UNIT = config.SHINY_DEFAULT_FILTER_TIME_UNIT


def transactions_intersection_filtered_factory(
        default_period=None,
        fixed_time_unit=False,
        default_time_unit=None,
):
    # TODO add custom date period option
    # TODO add date period in url params, with higher priority from the date range one
    # TODO move filter to shiny_apps, have to understand how the reactive will be modularized

    selected_period = DEFAULT_FILTER_PERIOD if default_period is None else default_period
    selected_time_unit = DEFAULT_FILTER_TIME_UNIT if default_time_unit is None else default_time_unit
    time_unit_choices = ['none', 'day', 'week', 'month', 'year'] if not fixed_time_unit else [selected_time_unit]
    return ui.card(
        ui.input_select(
            id='date_period_input_select',
            label='Select date period',
            choices=['Last 7 days', 'Last 30 days', 'Last 90 days', 'Last year', 'All'], # TODO last week, q1-4 (if exist), <2020, 2020, 2021, 2022, etc...
            selected=selected_period
        ),
        ui.input_date_range(
            id='transactions_date_range',
            label='Select date range',
            start=dateparser.parse('today'), #datetime.date.today() - datetime.timedelta(days=365),
            end=dateparser.parse('today'),#datetime.date.today(),
            format='dd-mm-yyyy',
            separator=':'
        ),
        ui.input_radio_buttons(
            id='time_unit_select',
            label='Time unit',
            choices=time_unit_choices,
            selected=selected_time_unit,
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


def _parse_params(input_url:str,
                  ensure_exists:str|list[str]|None=None):
    urlparse_result = urlparse(input_url)
    _url_params = parse_qs(urlparse_result.query)

    if ensure_exists is not None:
        ensure_exists = [ensure_exists] if isinstance(ensure_exists, str) else ensure_exists
        missing_params = [param for param in ensure_exists if param not in _url_params]
        if missing_params:
            raise ValueError(f"Missing '{missing_params}' required query parameters")

    return _url_params


def url_params_function_factory(input: Inputs,
                                output: Outputs,
                                session: Session,
                                data_manager: WorkingDataManager,
                                ensure_exists=None):

    @reactive.calc
    def get_url_params() -> dict:
        logging.info(f"{input['.clientdata_url_search'].get()=}")
        _url_params = _parse_params(input['.clientdata_url_search'].get(), ensure_exists=ensure_exists)  # TODO move to a reactive.calc func
        logging.info(f"Input params: {_url_params=}")
        return _url_params
    return get_url_params

def filter_url_params_function_factory(input: Inputs,
                                output: Outputs,
                                session: Session,
                                data_manager: WorkingDataManager, ):

    url_params = url_params_function_factory(input, output, session, data_manager)

    def _get_single_param(params: dict, key: str, default=None):
        values = params.get(key)
        if not values:
            return default
        return values[0]

    def _parse_date(value: str | None):
        if value is None or len(value) == 0:
            return None
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            logging.warning(f"Invalid date provided for '{value}', ignoring")
            return None

    @reactive.calc
    def filter_url_params():
        _raw_url_params = url_params()
        params = {}
        filter_in_tags_raw = _get_single_param(_raw_url_params, 'filter_in_tags', '')
        params['filter_in_tags'] = filter_in_tags_raw.split(',') if len(filter_in_tags_raw) > 0 else []
        filter_out_tags_raw = _get_single_param(_raw_url_params, 'filter_out_tags', '')
        params['filter_out_tags'] = filter_out_tags_raw.split(',') if len(filter_out_tags_raw) > 0 else []
        params['time_unit'] = _get_single_param(_raw_url_params, 'time_unit', DEFAULT_FILTER_TIME_UNIT)
        params['period'] = _get_single_param(_raw_url_params, 'period')
        params['start_date'] = _parse_date(_get_single_param(_raw_url_params, 'start_date'))
        params['end_date'] = _parse_date(_get_single_param(_raw_url_params, 'end_date'))
        logging.info(f"Input params: {params=}")
        return params
    return filter_url_params


def filter_funcs_factory(
        input: Inputs,
        output: Outputs,
        session: Session,
        data_manager: WorkingDataManager,
):
    filter_url_params_calc = filter_url_params_function_factory(input, output, session, data_manager)
    initialized = reactive.Value(False)
    skip_period_sync = reactive.Value(False)
    last_period = reactive.Value(None)

    def _clamp_date_range(transactions, requested_start_date: datetime.date, requested_end_date: datetime.date):
        """
        make sure that the start_date and end_date are valid for the transactions.date_range
        """
        tx_min_date, tx_max_date = transactions.date_range()
        if tx_max_date < requested_start_date:
            return None

        start = max(requested_start_date if requested_start_date is not None else tx_min_date, tx_min_date)
        end = min(requested_end_date if requested_end_date is not None else tx_max_date, tx_max_date)
        return start, end, tx_min_date, tx_max_date

    def _dates_for_period(period: str, transactions):
        today = datetime.date.today()
        if period == 'Last 7 days':
            start_date, end_date = today - datetime.timedelta(days=7), today
        elif period == 'Last 30 days':
            start_date, end_date = today - datetime.timedelta(days=30), today
        elif period == 'Last 90 days':
            start_date, end_date = today - datetime.timedelta(days=90), today
        elif period == 'Last year':
            start_date, end_date = today - datetime.timedelta(days=365), today
        else:
            start_date, end_date = transactions.date_range()
        return start_date, end_date

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
        filter_url_params = filter_url_params_calc()
        filter_in_tags = filter_url_params['filter_in_tags']
        filter_out_tags = filter_url_params['filter_out_tags']
        transactions = data_manager.get_transactions()
        filtered_in_transactions = transactions
        if len(filter_in_tags) > 0:
            filtered_in_transactions = transactions.containing_tags(filter_in_tags)
            if filtered_in_transactions.size() == 0:
                error_msg = f"No transactions found for {filter_url_params['time_unit']} time unit containing {filter_url_params['filter_in_tags']} tags."
                raise ShinyTransactionFilterError(error_msg)

        filtered_in_and_out_transactions = filtered_in_transactions
        if len(filter_out_tags) > 0:
            filtered_in_and_out_transactions = filtered_in_transactions.not_containing_tags(
                filter_out_tags,
                empty_tags_strategy='all_true')
            if filtered_in_and_out_transactions.size() == 0:
                error_msg = f"No transactions found for {filter_url_params['time_unit']} time unit after filtering out {filter_url_params['filter_in_tags']} tags."
                raise ShinyTransactionFilterError(error_msg)

        logging.info(f"URL param transactions: {filtered_in_and_out_transactions.size()=}")
        return filtered_in_and_out_transactions

    @reactive.effect
    @reactive.event(filter_url_params_calc)
    def init():
        if initialized.get():
            return
        logging.info('Init')
        filter_url_params = filter_url_params_calc()
        transactions = default_transactions()
        all_transactions = data_manager.get_transactions()
        all_tags_names = [tag.name for tag in data_manager.all_tags()]
        new_choices = [tag_name for tag_name, cnt in transactions.all_tag_counts().items() if
                       cnt > 0]

        current_filter_in = input.filter_in_tags_select() or []
        if len(current_filter_in) == 0:
            logging.info(f"Updating filter In tags: {len(new_choices)} {filter_url_params['filter_in_tags']}")
            ui.update_selectize(id='filter_in_tags_select',
                                choices=sorted(new_choices),
                                selected=filter_url_params['filter_in_tags'])

        current_filter_out = input.filter_out_tags_select() or []
        if len(current_filter_out) == 0:
            logging.info(f"Updating filter OUT tags: {len(all_tags_names)} {filter_url_params['filter_out_tags']}")
            ui.update_selectize(id='filter_out_tags_select',
                                choices=all_tags_names,
                                selected=filter_url_params['filter_out_tags'])

        ui.update_radio_buttons(id='time_unit_select', selected=filter_url_params['time_unit'])

        with reactive.isolate():
            current_period = input.date_period_input_select()

        requested_period = filter_url_params.get('period')
        if requested_period:
            ui.update_select(id='date_period_input_select', selected=requested_period)
            current_period = requested_period

        start_date_override = filter_url_params['start_date']
        end_date_override = filter_url_params['end_date']
        has_custom_dates = start_date_override is not None or end_date_override is not None

        if has_custom_dates:
            start_date = start_date_override
            end_date = end_date_override
            skip_period_sync.set(True)
        else:
            start_date, end_date = _dates_for_period(current_period, all_transactions)

        # start_date, end_date, min_date, max_date = _clamp_date_range(all_transactions, start_date, end_date)
        # ui.update_date_range(id='transactions_date_range',
        #                      start=start_date,
        #                      end=end_date,
        #                      min=min_date,
        #                      max=max_date)

        last_period.set(current_period)
        initialized.set(True)

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
        if not initialized.get():
            return
        current_period = input.date_period_input_select()
        if skip_period_sync.get() and current_period == last_period.get():
            skip_period_sync.set(False)
            return
        skip_period_sync.set(False)
        _all_transactions = data_manager.get_transactions()
        logging.info(f"Changed period to '{current_period}'")
        start_date, end_date = _dates_for_period(current_period, _all_transactions)
        date_range_values = _clamp_date_range(_all_transactions, start_date, end_date)
        if date_range_values:
            start_date, end_date, min_date, max_date = date_range_values
        else:
            start_date, end_date, min_date, max_date = [end_date]*4
        logging.info(f"date_range set to {min_date=} and {max_date=}")

        ui.update_date_range(id='transactions_date_range',
                             start=start_date,
                             end=end_date,
                             min=min_date,
                             max=max_date
                             )
        last_period.set(current_period)

    @reactive.calc
    def filtered_transactions():
        req(initialized.get())
        params = get_filter_params()
        start_date = params['start_date']
        end_date = params['end_date']
        time_unit = params['time_unit']
        filter_in_tags = params['filter_in_tags']
        filter_out_tags = params['filter_out_tags']
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

        agg_filtered_transactions = filtered_in_and_out_transactions.group_and_fill_transactions(
            grouping_key = time_unit,
            aggregation_key = 'sum'
        )

        logging.info(
            f"Filtered transactions size: {agg_filtered_transactions.size()=} for filter params=({start_date, end_date, time_unit, filter_in_tags, filter_out_tags})")


        return agg_filtered_transactions

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


def column_name_formating(col_name: str) -> str:
    return col_name.replace('_', ' ').title()


def render_table_standard(df,
                          format_columns=False,
                          format_boolean_values=False,
                          empty_message=None):
    if len(df)==0 and empty_message is not None:
        return pd.DataFrame({empty_message: ['0 rows']})

    if format_columns:
        df = df.copy()
        df.columns = [column_name_formating(col) for col in df.columns]

    if format_boolean_values:
        df = df.copy()
        df.replace({True: 'True', False: 'False'}, inplace=True)

    return render.DataTable(
        df,
        selection_mode="none",
        filters=True,
        styles=datatable_styles
    )