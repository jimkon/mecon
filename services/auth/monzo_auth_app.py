import shiny
from shiny import App, ui, render, reactive, Inputs, Outputs, Session
import pandas as pd

from mecon.etl.monzo_api_client import MonzoClient, MonzoCredentialsError, MonzoTokenRefreshError, MonzoNetworkError
from mecon.settings import DictFile

"""
Monzo Authentication Service: UI/UX Structure
Sections
* Credentials Overview Card

    - Show non-sensitive credential info:
        client_id
        redirect_url
        Token status (valid/expired, expiry time, minutes since creation)
        Last token refresh time
        Account IDs (if available)
    - Hide sensitive fields (client_secret, refresh_token, access_token).
* Actions Card

    - Refresh Token Button

        Triggers MonzoClient.refresh_token()
        Shows success/error message.
    - Test Download Button

        Tries to download today’s transactions.
        Shows result or error.
* OAuth Flow Card

    - Step-by-step Instructions

        Visit the authentication URL: [auth_url] (clickable link)
        Enter your email and click 'Continue'
        Check your email for the Monzo login message
        Copy the link address from the 'Log in to Monzo' button and paste it below:
        >[input box for response URL]
        >[Submit button] (calls set_authentication_code_from_url)
        Approve the authentication request in your Monzo app
    - Show status of OAuth flow (pending, success, error).
"""

creds_file = DictFile("path/to/creds.yaml")  # <-- update path as needed
monzo_client = MonzoClient(creds_file)

def get_safe_creds():
    creds = monzo_client.monzo_creds
    return {
        "client_id": creds.get("client_id"),
        "redirect_url": creds.get("redirect_url"),
        "token_expiry": monzo_client.expires_at(),
        "minutes_since_creation": monzo_client.minutes_passed_from_token_creation(),
        "accounts": [a.get("account_id") for a in creds.get("accounts", [])]
    }

app_ui = ui.page_fluid(
    ui.h2("Monzo Authentication"),
    ui.layout_columns(
        # Credentials Overview
        ui.card(
            ui.h4("Credentials Overview"),
            ui.output_table("creds_table"),
            ui.output_text("token_status"),
        ),
        # Actions
        ui.card(
            ui.h4("Actions"),
            ui.input_action_button("refresh_token", "Refresh Token"),
            ui.output_text("refresh_status"),
            ui.input_action_button("test_download", "Test Download Today's Data"),
            ui.output_text("download_status"),
        ),
        # OAuth Flow
        ui.card(
            ui.h4("OAuth Flow"),
            ui.markdown("""
1. Visit the authentication URL: [Monzo Auth Link](auth_url)
2. Enter your email and click 'Continue'
3. Check your email for the Monzo login message
4. Copy the link address from the 'Log in to Monzo' button and paste it below:
            """),
            ui.input_text("response_url", "Paste response URL here"),
            ui.input_action_button("submit_oauth", "Submit"),
            ui.output_text("oauth_status"),
            ui.markdown("""
5. Approve the authentication request in your Monzo app
            """),
        ),
    )
)

def server(input: Inputs, output: Outputs, session: Session):
    @output
    @render.table
    def creds_table():
        creds = get_safe_creds()
        return pd.DataFrame(list(creds.items()), columns=["Field", "Value"])

    @output
    @render.text
    def token_status():
        if monzo_client.has_token():
            return f"Token is valid. Expires at: {monzo_client.expires_at()}"
        else:
            return "No valid token found."

    @reactive.event(input.refresh_token)
    def _():
        try:
            monzo_client.refresh_token()
            output.refresh_status.set("Token refreshed successfully.")
        except Exception as e:
            output.refresh_status.set(f"Error refreshing token: {e}")

    @reactive.event(input.test_download)
    def _():
        try:
            today = pd.Timestamp.now().strftime("%Y-%m-%dT00:00:00Z")
            df = monzo_client.download_full_history(since=today)
            output.download_status.set(f"Downloaded {len(df)} transactions for today.")
        except Exception as e:
            output.download_status.set(f"Error downloading data: {e}")

    @reactive.event(input.submit_oauth)
    def _():
        try:
            monzo_client.set_authentication_code_from_url(input.response_url())
            output.oauth_status.set("OAuth authentication successful. Token refreshed.")
        except Exception as e:
            output.oauth_status.set(f"OAuth authentication failed: {e}")

    @output
    @render.text
    def auth_url():
        return monzo_client.get_authentication_url()

monzo_app = App(app_ui, server)