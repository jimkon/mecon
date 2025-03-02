import datetime
import logging

from shiny import ui

from mecon import config
from mecon.app.current_data import WorkingDataManager, WorkingDatasetDir
from mecon.utils.html import build_url

DEFAULT_PERIOD = 'Last year'
DEFAULT_TIME_UNIT = 'month'

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
    ui.nav_control(ui.tags.a("Monitoring", href=f"http://127.0.0.1:8003/")),
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


filter_menu = ui.sidebar(
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
