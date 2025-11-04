import logging

import pandas as pd
from htmltools import HTML
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app import shiny_app
from mecon.app.current_data import WorkingDataManager
from mecon.etl.true_layer_client_by_o3 import TrueLayerClient, AuthFlowError
from mecon.tags import tagging

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

dataset = shiny_app.get_working_dataset()
creds_file = dataset.creds
tl = TrueLayerClient(creds_file)

app_ui = shiny_app.app_ui_factory(
    ui.page_fluid(
        ui.navset_tab(
            ui.nav_panel(
                "True Layers HSBC",
                ui.input_task_button(id="test_ob_hsbc_button", label="Test credentials"),
                ui.card(
                    ui.card_header("Authentication"),
                    ui.card_body(
                        ui.markdown(f"Visit this [link]({tl.build_auth_link(provider_id='ob-hsbc')})"),
                        ui.input_text(id="ob_hsbc_auth_link_input", label="Enter the url from the authentication page: "),
                    ),
                    ui.card_footer(
                        ui.input_task_button(id="auth_ob_hsbc_button", label="Authenticate"),
                    )
                )
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


auth_app = App(app_ui, server)
