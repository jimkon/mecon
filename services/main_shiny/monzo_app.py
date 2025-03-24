import logging
import shutil

from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app import shiny_app
from mecon.etl.transformers import MonzoFileStatementTransformer

import monzo_api_lib as monzo

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

app_ui = shiny_app.app_ui_factory(
    ui.card(
        ui.card(ui.h3("Monzo API"),
            ui.output_ui('monzo_info_text'),
            ui.input_task_button(id='monzo_refresh_token_button', label='Refresh Token'),
            ui.input_action_button(id='monzo_authenticate_button', label='Authenticate'),
            ui.input_task_button(id='monzo_fetch_transactions_button',
                                 label='Fetch Transactions'),
        ),
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = shiny_app.create_data_manager()

    dataset = shiny_app.get_working_dataset()
    token_filepath = dataset.db.parent.parent.parent / 'monzo-api.json'

    monzo_client = monzo.MonzoClient(token_file=token_filepath)

    def save_monzo_statement(df, name):
        monzo_dir = dataset.statements / 'Monzo'

        # back up old files
        backup_dir = monzo_dir / 'backup'
        if backup_dir.exists():
            shutil.rmtree(backup_dir)  # Remove existing backup
        backup_dir.mkdir()
        # Move all original files (excluding the backup folder itself)
        for file in monzo_dir.iterdir():
            if file != backup_dir:
                shutil.move(str(file), backup_dir)

        df.to_csv(monzo_dir / name, index=False)

    @render.text
    def monzo_info_text():
        if monzo_client.has_token():
            return ui.HTML(f"Authenticated until {monzo_client.expires_at()}.")
        else:
            return ui.HTML(f"No token found.")

    @reactive.effect
    @reactive.event(input.monzo_refresh_token_button)
    def monzo_refresh_token_button_clicked():
        try:
            monzo_client.refresh_token()
            ui.update_text(id='monzo_info_text',
                           label='Status',
                           value=f"NEW: Authenticated until {monzo_client.expires_at()}.")
            ui.notification_show(
                f"Proceed to refresh token from the Monzo App",
                type="message",
                duration=10,
                close_button=True
            )
        except Exception as e:
            logging.exception("Error while refreshing token")
            ui.notification_show(
                f"Error while refreshing token: {e}",
                type="error",
                duration=10,
                close_button=True
            )

    @reactive.effect
    @reactive.event(input.monzo_authenticate_button)
    def monzo_authenticate_button_clicked():
        try:
            auth_url = monzo_client.get_authentication_url()
            m = ui.modal(
                ui.HTML(f"<ol><li>{ui.tags.a('Redirect to...', href=auth_url)}</li>"
                        "<li>Put your email and click 'Continue'</li>"
                        "<li>Go to your emails and open the last one from Monzo</li>"
                        "<li>Copy the link address from the 'Log in to Monzo' button inside the email</li>"
                        f"<li>{ui.input_text(id='monzo_authentication_url_input_text', label='Paste the link address here and click Submit:', value='https://localhost/?code=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx&state=yyyyyyyyyyyyy')}</li>"
                        "<li>" + str(ui.input_task_button(id="monzo_authentication_url_input_action_button",
                                                          label="Submit")) + "</li>"
                                                                             "<li>Go to your Monzo app and approve the authentication request</li>"

                                                                             "</ol>"),
                title=f"Monzo API authentication",
                easy_close=True,
                size='xl'
            )
            ui.modal_show(m)

            @reactive.effect
            @reactive.event(input.monzo_authentication_url_input_action_button)
            def monzo_authentication_url_input_action_button_clicked():
                try:
                    auth_url = input.monzo_authentication_url_input_text()
                    monzo_client.set_authentication_code_from_url(auth_url)
                    ui.modal_remove()
                    ui.update_text(id='monzo_info_text',
                                   value=f"NEW: Authenticated until {monzo_client.expires_at()}.")

                    ui.notification_show(
                        f"Successfully authenticated unitl {monzo_client.expires_at()}. You might need to refresh the main page",
                        type="message",
                        duration=20,
                        close_button=True
                    )
                except Exception as e:
                    logging.exception("Error while authenticating")
                    ui.notification_show(
                        f"Error while authenticating: {e}",
                        type="error",
                        duration=20,
                        close_button=True
                    )

        except Exception as e:
            logging.exception("Error while authenticating")
            ui.notification_show(
                f"Error while authenticating: {e}",
                type="error",
                duration=20,
                close_button=True
            )

    @reactive.effect
    @reactive.event(input.monzo_fetch_transactions_button)
    def monzo_fetch_transactions_button_clicked():
        try:
            df_raw = monzo_client.download_full_history()
            df_monzo = MonzoFileStatementTransformer().transform(df_raw)
            min_date, max_date = df_monzo['datetime'].dt.date.min(), df_monzo['datetime'].dt.date.max()
            days_in_between, n_unique_days = (max_date - min_date).days, len(set(df_monzo['datetime'].dt.date))
            minutes_since_token_creation = int(
                monzo_client.minutes_passed_from_token_creation()) if monzo_client.minutes_passed_from_token_creation() else '[None]'
            m = ui.modal(
                ui.HTML(
                    f"Downloaded <b>{len(df_raw)}</b> transactions with {len(df_raw.shape)} columns."
                    f"The date range of the transactions is from <b>{min_date}</b> to <b>{max_date}</b> (<b>{days_in_between} days period, {n_unique_days} unique</b>). "
                    f"Task run <b>{minutes_since_token_creation}</b> minutes after token creation, it needs to run less that <b>5</b> minutes "
                    f"before token creations to parse all transactions, consider refreshing the token.<br>"),
                ui.input_task_button(id="monzo_save_transactions_button", label="Save Monzo statement"),
                ui.output_data_frame('monzo_fetch_transactions_dataframe'),
                title=f"Transactions fetched from Monzo",
                easy_close=True,
                size='xl'
            )
            ui.modal_show(m)

            @render.data_frame
            def monzo_fetch_transactions_dataframe():

                return shiny_app.render_table_standard(df_monzo)

            @reactive.effect
            @reactive.event(input.monzo_save_transactions_button)
            def monzo_save_transactions_button_clicked():
                save_monzo_statement(df_monzo, f"monzo_api_transactions_{min_date}_to_{max_date}.csv")


        except Exception as e:
            logging.exception("Error while authenticating")
            ui.notification_show(
                f"Error while downloading transactions history: {e}",
                type="error",
                duration=20,
                close_button=True
            )


monzo_app = App(app_ui, server)
