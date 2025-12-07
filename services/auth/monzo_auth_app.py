import copy
import json
import logging
from datetime import datetime
from typing import Literal

from json2html import json2html
from shiny import App, ui, render, reactive

from mecon.app import shiny_app
from mecon.etl.account_statements import MonzoAPIStatements
from mecon.etl.monzo_api_client import MonzoClient

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

dataset = shiny_app.get_working_dataset()
creds = dataset.creds
monzo_client = MonzoClient(creds)
sid = 'monzo_api'


# --- helpers -------------------------------------------------


def get_accounts_info_from_creds():
    if 'monzo-api' not in creds:
        logging.warning(f"No 'monzo-api' credentials found in creds")
        return None

    if 'accounts' not in creds['monzo-api']:
        logging.warning(f"No 'accounts' found in 'monzo-api' credentials")
        return None

    accounts = copy.deepcopy(creds['monzo-api']['accounts'])

    return accounts


def get_account_ids_for_source():
    accounts = get_accounts_info_from_creds()
    if accounts is None:
        return []

    return [account['account_id'] for account in accounts]


def format_accounts_info(accounts_info):
    if accounts_info is None or len(accounts_info) == 0:
        return '## <span style="color:red"> No accounts info found</span>.'
    return json2html.convert(json.dumps(accounts_info))


def source_current_data_info():
    account_ids = get_account_ids_for_source()
    res = {}
    for account_id in account_ids:
        account = MonzoAPIStatements.from_dataset(
            dataset=dataset,
        )
        if account is None:
            raise ValueError(f"Account '{account_id}' not found in 'monzo-api' credentials.")
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


_source_current_data_info_cache = None


def source_current_data_info_cached():
    global _source_current_data_info_cache
    if _source_current_data_info_cache is None:
        _source_current_data_info_cache = source_current_data_info()
    return _source_current_data_info_cache


def format_data_info():
    formated_data = {}
    account_data_info = source_current_data_info()
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


def fetch_data(account_id, which_data: Literal['max', 'last'] = 'last'):
    if which_data == 'max':
        from_date, to_date = None, None
    elif which_data == 'last':
        acc_data_info = source_current_data_info_cached()[account_id]
        last_date = acc_data_info['end_date']
        from_date, to_date = datetime.strptime(last_date, "%Y-%m-%d"), datetime.today()

    logging.info(
        f"Fetching data from 'monzo-api', account: '{account_id}', period: '{which_data}', {from_date=}, {to_date=} ")
    account = MonzoAPIStatements.from_dataset(
        dataset=dataset,
    )
    df = account.fetch(since=from_date)
    return df


def source_ui():
    accounts = get_accounts_info_from_creds() or []
    account_choices = ['All'] + [account['account_id'] for account in accounts]
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
                        ui.markdown(
                            f"Visit this [link]({monzo_client.get_authentication_url()})"
                        ),
                        ui.markdown(
                            "Enter your email address that is linked with your Monzo account"
                        ),
                        ui.markdown(
                            "Find the 'Log in to Monzo' email just sent to you and copy the link address "
                            "from the 'Log in to Monzo' button found in the email."
                        ),
                        ui.input_text(id=f"{sid}_auth_link_input",
                                      label="Paste the link address here: "),
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
                    ui.card_body(
                        ui.row(
                            ui.input_selectize(
                                id=f"{sid}_fetch_account_select",
                                label="Select account",
                                choices=account_choices
                            ),
                            ui.input_radio_buttons(
                                id=f"{sid}_fetch_period_radio",
                                label="Period",
                                choices={'last': 'Since last fetch', 'max': 'All available (90 days)'},
                                selected='last',
                            ),
                            ui.input_task_button(id=f"fetch_{sid}_button", label="Fetch data...", width='10%',
                                                 height='10%'),
                        )
                    ),
                    ui.card_footer(
                        ui.output_ui(id=f"{sid}_data_info")
                    )
                )
            ),
        )

    )


def mount_source_server(input, output, session):
    @output(id=f"{sid}_api_status")
    @render.ui
    def _api_status():
        accounts = get_accounts_info_from_creds()
        status_md = format_accounts_info(accounts)
        return ui.markdown(status_md)

    @reactive.effect
    @reactive.event(input[f"refresh_{sid}_accounts_button"])
    def _on_test_click():
        try:
            monzo_client.get_accounts()  # for example
            ui.notification_show(
                f"Successfully pinged 'monzo-api'. Refresh page to load the new results", type="message", duration=2
            )
        except Exception as e:
            ui.notification_show(
                f"Ping failed for 'monzo-api': {e}", type="error", duration=None
            )

    @reactive.effect
    @reactive.event(input[f"auth_{sid}_button"])
    def _auth_source_button():
        try:
            auth_link_resp = getattr(input, f"{sid}_auth_link_input")()
            monzo_client.set_authentication_code_from_url(
                auth_link_resp,
            )
            accounts = monzo_client.get_accounts()
            ui.notification_show(
                f"Successfully pinged the 'monzo-api' account. Received payload: {accounts}",
                type='message',
                duration=2
            )
        except Exception as e:
            ui.notification_show(
                f"Failed to exchange token for 'monzo-api': {e}",
                type='error',
                duration=None
            )
            logging.exception(e)

    @output(id=f"{sid}_data_info")
    @render.ui
    def _data_info():
        data_md = format_data_info()
        return ui.markdown(data_md)

    @reactive.effect
    @reactive.event(input[f"fetch_{sid}_button"])
    def fetch_source_data_button():
        account_selection = getattr(input, f"{sid}_fetch_account_select")()
        if account_selection == 'All':
            account_ids = get_account_ids_for_source()
        else:
            account_ids = [account_selection]

        period_selection = getattr(input, f"{sid}_fetch_period_radio")()

        for account_id in account_ids:
            try:
                df = fetch_data(account_id=account_id, which_data=period_selection)
                shape = df.shape if df is not None else None

                global _source_current_data_info_cache
                _source_current_data_info_cache = None
                ui.notification_show(
                    f"Fetching data from 'monzo-api', account: '{account_id}', period: '{period_selection}' returned results with shape {shape}",
                    type='message',
                    duration=5
                )
            except Exception as e:
                ui.notification_show(
                    f"Failed to exchange token for 'monzo-api': {e}",
                    type='error',
                    duration=None
                )
                logging.exception(e)


# --- app -----------------------------------------------------


app_ui = shiny_app.app_ui_factory(
    ui.navset_tab(
        ui.nav_panel('MonzoApi', source_ui())
    )
)


def server(input, output, session):
    mount_source_server(input, output, session)


auth_app = App(app_ui, server)
