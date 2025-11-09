import logging

from shiny import App, Inputs, Outputs, Session, ui, reactive

from mecon.app import shiny_app
from mecon.etl.account_statements import TrueLayerStatements, TrueLayerHSBCStatements, TrueLayerHSBCSSaverStatements
from mecon.etl.true_layer_client_by_o3 import TrueLayerClient, AuthFlowError

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

dataset = shiny_app.get_working_dataset()
creds_file = dataset.creds
tl = TrueLayerClient(creds_file)

account_statements = []


def get_account_info_from_creds(creds, source, account_id):
    if 'truelayer' not in creds:
        logging.warning(f"No TrueLayer credentials found in {creds}")
        return None

    if 'sources' not in creds['truelayer']:
        logging.warning(f"No 'sources' found in TrueLayer credentials")
        return None

    if  source not in creds['truelayer']['sources']:
        logging.warning(f"No {source} found in TrueLayer sources credentials")
        return None

    for account in creds['truelayer']['sources']:
        if account['account_id'] == account_id:
            return account

    logging.warning(f"No account with {account_id=} found in TrueLayer sources credentials")
    return None


def ping_tl_ob_hsbc():
    logging.info("test_ob_hsbc_button")
    try:
        accounts = tl.get_accounts('ob-hsbc')

        message = "## Healthy"
        for account in accounts:
            message += f"\n * {account['account_type']} [{account['account_id']}]: {account['display_name']} ({account['currency']})"
            account_stat = TrueLayerStatements.from_account_id(
                dataset=dataset,
                account_id=account['account_id']
            )
            if account_stat is None:
                continue
            account_stat.account_id = account['account_id']
            account_stat.bank = 'ob-hsbc'
            account_statements.append(account_stat)
        return message
    except AuthFlowError as e:
        return f"Pinging the 'ob-hsbc' account failed: {e}"


def ob_hsbc_existing_dates():
    res = {}
    for account_type in [TrueLayerHSBCStatements, TrueLayerHSBCSSaverStatements]:
        account = account_type.from_path_and_creds(
            working_dir=dataset.statements / account_type.dir_name,
            creds=dataset.creds
        )
        tx = account.to_transactions()
        start_date, end_date = tx.date_range()
        res[account_type.dir_name] = {'start_date': start_date, 'end_date': end_date}
        # res[account.account_id] = {'start_date': account_type.dir_name, 'end_date': dataset.statements / account_type.dir_name}
    return res


def ob_hsbc_data_info():
    logging.info("ob_hsbc_info")
    message = '### Data stats'
    for acc_id, account in ob_hsbc_existing_dates().items():
        message += f"\n * {acc_id}: {account['start_date']} - {account['end_date']}"
    return message


def source_app_page(source):
    return ui.layout_columns(
        ui.card(
            ui.card_header("Status"),
            ui.card_body(ui.markdown(ping_tl_ob_hsbc())),
            ui.card_footer(
                ui.input_task_button(id="test_ob_hsbc_button", label="Test credentials"),
            )
        ),
        # ui.card(
        #     ui.card_header("Authentication"),
        #     ui.card_body(
        #         ui.markdown(f"Visit this [link]({tl.build_auth_link(provider_id='ob-hsbc')})"),
        #         ui.input_text(id="ob_hsbc_auth_link_input",
        #                       label="Enter the url from the authentication page: "),
        #     ),
        #     ui.card_footer(
        #         ui.input_task_button(id="auth_ob_hsbc_button", label="Authenticate"),
        #     )
        # ),
        # ui.card(
        #     ui.card_header("Data"),
        #     ui.card_body(ui.markdown(ob_hsbc_data_info())),
        #     ui.card_footer(
        #         ui.input_task_button(id="fetch_ob_hsbc_button", label="Fetch"),
        #     )
        # )
    )


app_ui = shiny_app.app_ui_factory(
    ui.page_fluid(
        ui.input_select(
            id='tl_provider_select',
            label="True Layer :",
            choices={
                'ob-hsbc': 'HSBC',
                # 'ob-revolut': 'Revolut',
                # 'ob-monzo': 'Monzo',
            }
        ),
        ui.card(
            ui.card_header("Status"),
            ui.card_body(ui.markdown(ping_tl_ob_hsbc())),
            ui.card_footer(
                ui.input_task_button(id="test_ob_hsbc_button", label="Test credentials"),
            )
        ),
        ui.card(
            ui.card_header("Authentication"),
            ui.card_body(
                ui.markdown(f"Visit this [link]({tl.build_auth_link(provider_id='ob-hsbc')})"),
                ui.input_text(id="ob_hsbc_auth_link_input",
                              label="Enter the url from the authentication page: "),
            ),
            ui.card_footer(
                ui.input_task_button(id="auth_ob_hsbc_button", label="Authenticate"),
            )
        ),
        ui.card(
            ui.card_header("Data"),
            ui.card_body(ui.markdown(ob_hsbc_data_info())),
            ui.card_footer(
                ui.input_task_button(id="fetch_ob_hsbc_button", label="Fetch"),
            )
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @reactive.effect
    @reactive.event(input.test_ob_hsbc_button)
    def test_ob_hsbc_button():
        logging.info("test_ob_hsbc_button")
        try:
            accounts = tl.get_accounts('ob-hsbc')
            ui.notification_show(
                f"Successfully pinged the ob-hsbc account. Received payload: {accounts}",
                type='message',
                duration=2
            )
        except AuthFlowError as e:
            ui.notification_show(
                f"Pinging the 'ob-hsbc' account failed: {e}",
                type='error',
                duration=None
            )

        logging.info("test_ob_hsbc_button Success")

    @reactive.effect
    @reactive.event(input.auth_ob_hsbc_button)
    def auth_ob_hsbc_button():
        logging.info("auth_ob_hsbc_button")
        try:
            auth_link_resp = input.ob_hsbc_auth_link_input()
            tl.exchange_code_from_code_url(auth_link_resp, bank='ob-hsbc')
            accounts = tl.get_accounts('ob-hsbc')
            ui.notification_show(
                f"Successfully pinged the 'ob-hsbc' account. Received payload: {accounts}",
                type='message',
                duration=2
            )
        except Exception as e:
            ui.notification_show(
                f"Failed to exchange token for 'ob-hsbc': {e}",
                type='error',
                duration=None
            )
            logging.exception(e)

        logging.info("auth_ob_hsbc_button Success")

    @reactive.effect
    @reactive.event(input.fetch_ob_hsbc_button)
    def fetch_ob_hsbc_button():
        logging.info("fetch_ob_hsbc_button")
        # try:
        #     tl.
        #     ui.notification_show(
        #         f"Successfully pinged the 'ob-hsbc' account. Received payload: {accounts}",
        #         type='message',
        #         duration=2
        #     )
        # except Exception as e:
        #     ui.notification_show(
        #         f"Failed to exchange token for 'ob-hsbc': {e}",
        #         type='error',
        #         duration=None
        #     )
        #     logging.exception(e)

        logging.info("auth_ob_hsbc_button Success")


auth_app = App(app_ui, server)
