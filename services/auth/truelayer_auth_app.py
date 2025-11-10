import json
import logging
import threading
from datetime import datetime
from typing import Literal

from json2html import json2html

from shiny import App, ui, render, reactive

from mecon.app import shiny_app
from mecon.etl.account_statements import TrueLayerStatements, TrueLayerHSBCStatements, TrueLayerHSBCSSaverStatements
from mecon.etl.true_layer_client_by_o3 import TrueLayerClient, AuthFlowError

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

dataset = shiny_app.get_working_dataset()
creds = dataset.creds
tl = TrueLayerClient(creds)


# --- helpers -------------------------------------------------

def sanitize(source: str) -> str:
    return source.replace("-", "_")


def get_accounts_info_from_creds(source):
    if 'truelayer' not in creds:
        logging.warning(f"No TrueLayer credentials found in {creds}")
        return None

    if 'sources' not in creds['truelayer']:
        logging.warning(f"No 'sources' found in TrueLayer credentials")
        return None

    if source not in creds['truelayer']['sources']:
        logging.warning(f"No {source} found in TrueLayer sources credentials")
        return None

    import copy
    accounts = copy.deepcopy(creds['truelayer']['sources'][source]['accounts'])

    return accounts


def get_account_ids_for_source(source):
    accounts = get_accounts_info_from_creds(source)
    if accounts is None:
        return []

    return [account['account_id'] for account in accounts]


def format_accounts_info(accounts_info):
    if accounts_info is None or len(accounts_info) == 0:
        return '## <span style="color:red"> No accounts info found</span>.'
    return json2html.convert(json.dumps(accounts_info))


_source_current_data_info_cache = {}


def source_current_data_info(source):
    account_ids = get_account_ids_for_source(source)
    res = {}
    for account_id in account_ids:
        account = TrueLayerStatements.from_account_id(
            dataset=dataset,
            account_id=account_id
        )
        if account is None:
            raise ValueError(f"Account '{account_id}' not found in {source}.")
        tx = account.to_transactions()
        start_date, end_date = tx.date_range()
        days_missing_from_today = datetime.today().date() - end_date
        number_of_days_included = end_date - start_date
        res[account_id] = {
            'account_dir': account.dir_name,
            'start_date': str(start_date),
            'end_date': str(end_date),
            'days_included': number_of_days_included.days,
            'days_missing_from_today': days_missing_from_today.days,
        }
    return res


def source_current_data_info_cached(source):
    if source not in _source_current_data_info_cache:
        _source_current_data_info_cache[source] = source_current_data_info(source)
    return _source_current_data_info_cache[source]


def format_data_info(source):
    formated_data = {}
    account_data_info = source_current_data_info_cached(source)
    for acc_id, acc_data in account_data_info.items():
        formated_data[acc_id] = {
            'generals': {'account_dir': acc_data['account_dir']},
            'dates': {
                'start_date': acc_data['start_date'],
                'end_date': acc_data['end_date'],
                'days_missing_from_today': acc_data['days_missing_from_today'],
                'days_included': acc_data['days_included'],
            }
        }
    return json2html.convert(json.dumps(formated_data))


def fetch_data(source, account_id, which_data: Literal['max', 'last'] = 'last'):
    if which_data == 'max':
        from_date, to_date = None, None
    elif which_data == 'last':
        acc_data_info = source_current_data_info_cached(source)[account_id]
        last_date = acc_data_info['end_date']
        from_date, to_date = last_date, datetime.today()
    # tl.get_transactions(source, account_id, from_date, to_date)
    raise ValueError(f"ERROR for inputs: {source=} {account_id=}, {which_data=}, {from_date=}, {to_date=}")


def source_ui(source: str):
    sid = sanitize(source)
    return ui.layout_columns(
        ui.navset_pill(
            ui.nav_panel(
                "Status",
                ui.card(
                    ui.card_body(ui.output_ui(id=f"{sid}_api_status")),
                    ui.card_footer(
                        ui.input_task_button(id=f"refresh_{sid}_accounts_button", label="Refresh accounts"),
                    )
                )
            ),
            ui.nav_panel(
                "Authentication",
                ui.card(
                    ui.card_body(
                        ui.markdown(f"Visit this [link]({tl.build_auth_link(provider_id=source)})"),
                        ui.input_text(id=f"{sid}_auth_link_input",
                                      label="Enter the url from the authentication page: "),
                    ),
                    ui.card_footer(
                        ui.input_task_button(id=f"auth_{sid}_button", label="Authenticate"),
                    )
                )
            ),
            ui.nav_panel(
                "Data",
                ui.card(
                    ui.card_header("Data"),
                    ui.card_body(ui.input_selectize(
                        id=f"{sid}_fetch_account_select",
                        label="Select account",
                        choices=['All'] + [account['account_id'] for account in get_accounts_info_from_creds(source)]
                    ),
                        ui.input_radio_buttons(
                            id=f"{sid}_fetch_period_radio",
                            label="Period",
                            choices={'last': 'Since last fetch', 'max': 'All available (90 days)'},
                            selected='last',
                        ),
                        ui.input_task_button(id=f"fetch_{sid}_button", label="Fetch data...")
                    ),
                    ui.card_footer(
                        ui.output_ui(id=f"{sid}_data_info")
                    )
                )
            ),
        )

    )


def mount_source_server(source: str, input, output, session):
    sid = sanitize(source)

    @output(id=f"{sid}_api_status")
    @render.ui
    def _api_status():
        accounts = get_accounts_info_from_creds(source)
        status_md = format_accounts_info(accounts)
        return ui.markdown(status_md)

    @reactive.effect
    @reactive.event(input[f"refresh_{sid}_accounts_button"])
    def _on_test_click():
        try:
            tl.get_accounts(source)  # for example
            ui.notification_show(
                f"Successfully pinged {source}. Refresh page to load the new results", type="message", duration=2
            )
        except Exception as e:
            ui.notification_show(
                f"Ping failed for {source}: {e}", type="error", duration=None
            )

    @reactive.effect
    @reactive.event(input[f"auth_{sid}_button"])
    def _auth_source_button():
        try:
            auth_link_resp = getattr(input, f"{sid}_auth_link_input")()
            tl.exchange_code_from_code_url(auth_link_resp, bank=source)
            accounts = tl.get_accounts(source)
            ui.notification_show(
                f"Successfully pinged the '{source}' account. Received payload: {accounts}",
                type='message',
                duration=2
            )
        except Exception as e:
            ui.notification_show(
                f"Failed to exchange token for '{source}': {e}",
                type='error',
                duration=None
            )
            logging.exception(e)

    @output(id=f"{sid}_data_info")
    @render.ui
    def _data_info():
        data_md = format_data_info(source)
        return ui.markdown(data_md)

    @reactive.effect
    @reactive.event(input[f"fetch_{sid}_button"])
    def fetch_source_data_button():
        account_selection = getattr(input, f"{sid}_fetch_account_select")()
        if account_selection == 'All':
            account_ids = get_account_ids_for_source(source)
        else:
            account_ids = [account_selection]

        period_selection = getattr(input, f"{sid}_fetch_period_radio")()

        for account_id in account_ids:
            try:
                fetch_data(input, account_id, period_selection)
                ui.notification_show(
                    f"Fetching data from '{source}', account: '{account_id}', period: '{period_selection}'...DISABLED.",
                    type='warning',
                    duration=2
                )
            except Exception as e:
                ui.notification_show(
                    f"Failed to exchange token for 'ob-hsbc': {e}",
                    type='error',
                    duration=None
                )
                logging.exception(e)


# --- app -----------------------------------------------------

sources = ["ob-hsbc", "ob-revolut", "ob-monzo"]

app_ui = shiny_app.app_ui_factory(
    ui.navset_tab(
        *(ui.nav_panel(src.upper(), source_ui(src)) for src in sources)
    )
)


def server(input, output, session):
    for src in sources:
        mount_source_server(src, input, output, session)


auth_app = App(app_ui, server)
