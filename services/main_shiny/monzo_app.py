import logging
import shutil

import pandas as pd
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app import shiny_app
from mecon.app.current_data import WorkingDatasetDirInfo
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
        monzo_dir = dataset.statements / 'MonzoAPI'

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
        logging.info(f"Successfully saved {len(df)} Monzo transactions as {name} in {monzo_dir}, and backed up previous statements in {backup_dir}.")
        ui.notification_show(
            f"Successfully saved {len(df)} Monzo transactions as {name} in {monzo_dir}, and backed up previous statements in {backup_dir}.",
            type="message",
            duration=10,
            close_button=True
        )

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
                        f"<li>{ui.input_text(id='monzo_authentication_url_input_text', label='Paste the link address here and click Submit:', value='https://localhost/?code=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx&state=yyyyyyyyyyyyy')}"
                        "" + str(ui.input_task_button(id="monzo_authentication_url_input_action_button", label="Submit")) + "</li>"
                        "<li>Go to your Monzo app and approve the authentication request</li></ol>"),
                title=f"Monzo API authentication",
                easy_close=True,
                size='xl'
            )
            ui.modal_show(m)

            @reactive.effect
            @reactive.event(input.monzo_authentication_url_input_action_button)
            def monzo_authentication_url_input_action_button_clicked():
                try:
                    response_url = input.monzo_authentication_url_input_text()
                    monzo_client.set_authentication_code_from_url(response_url)
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
            df_raw['created_date'] = pd.to_datetime(df_raw['created'].apply(lambda datetime_str: datetime_str[:10]))

            min_date, max_date = df_raw['created_date'].dt.date.min(), df_raw['created_date'].dt.date.max()
            # df_monzo = MonzoFileStatementTransformer().transform(df_raw)
            days_in_between, n_unique_days = (max_date - min_date).days, len(set(df_raw['created_date'].dt.date))
            minutes_since_token_creation = int(
                monzo_client.minutes_passed_from_token_creation()) if monzo_client.minutes_passed_from_token_creation() else '[None]'

            WorkingDatasetDirInfo().statement_files_info()
            dataset_dir_info = WorkingDatasetDirInfo()
            prev_statements = [path for path, *_ in dataset_dir_info.statement_files_info()['MonzoAPI']]# if str(path).startswith('monzo_api_transactions')]
            logging.info(f"{prev_statements=}")
            if prev_statements:
                df_prev = pd.concat([pd.read_csv(file, index_col=None) for file in prev_statements])
                df_prev['created_date'] = pd.to_datetime(df_prev['created'].apply(lambda datetime_str: datetime_str[:10]))
                prev_stats_message = f"Previous statements >>{prev_statements}<< contains {len(df_prev)} transactions with {df_prev.shape[1]} columns."
                f"The date range of the transactions is from {df_prev['created'].min()} to {df_prev['created'].max()}, "
                f"{df_prev['id'].duplicated(keep=False).sum()} duplicated ids"
                df_prev.drop_duplicates(subset='id', inplace=True)
            else:
                df_prev = None
                prev_stats_message = 'No previous statements found'

            m = ui.modal(
                ui.HTML(
                    f"Downloaded <b>{len(df_raw)}</b> transactions with {len(df_raw.shape)} columns."
                    f"The date range of the transactions is from <b>{min_date}</b> to <b>{max_date}</b> (<b>{days_in_between} days period, {n_unique_days} unique</b>). "
                    f"Task run <b>{minutes_since_token_creation}</b> minutes after token creation, it needs to run less that <b>5</b> minutes "
                    f"before token creations to parse all transactions, consider refreshing the token.<br>"),
                ui.tooltip(
                    ui.input_task_button(id="merge_previous_transactions_button", label="Merge with previous transactions?", disabled=df_prev is None),
                    prev_stats_message
                ),
                ui.input_task_button(id="monzo_save_transactions_button", label="Save Monzo statement"),
                ui.navset_tab(
                    ui.nav_panel('New data', ui.output_data_frame('fetched_transactions_dataframe')),
                    ui.nav_panel('Old data', ui.output_data_frame('old_transactions_dataframe')),
                    id='fetched_data_navset_tab',
                ),
                title=f"Transactions fetched from Monzo",
                easy_close=True,
                size='xl'
            )
            ui.modal_show(m)

            @render.data_frame
            def fetched_transactions_dataframe():
                return shiny_app.render_table_standard(df_raw)

            @render.data_frame
            def old_transactions_dataframe():
                if df_prev is not None:
                    return shiny_app.render_table_standard(df_prev)
                return pd.DataFrame({'Data': ['No previous transactions found']})

            @reactive.effect
            @reactive.event(input.monzo_save_transactions_button)
            def monzo_save_transactions_button_clicked():
                save_monzo_statement(df_raw, f"monzo_api_transactions_{min_date}_to_{max_date}.csv")
                ui.modal_remove()

            @reactive.effect
            @reactive.event(input.merge_previous_transactions_button)
            def merge_previous_transactions_button_clicked():
                df_merged = pd.concat([df_prev, df_raw]).reset_index(drop=True)
                size_before_merge = df_prev.shape[0]
                merged_min_date, merged_max_date = df_merged['created_date'].dt.date.min(), df_merged['created_date'].dt.date.max()
                logging.info(f"Merging {merged_min_date=}, {merged_max_date=}, {df_merged['id'].duplicated(keep=False).sum()} duplicated transactions")
                df_merged.drop_duplicates(subset='id', inplace=True)
                size_after_merge = df_merged.shape[0]
                logging.info(f"Merging added {size_after_merge-size_before_merge} transactions")
                save_monzo_statement(df_merged, f"monzo_api_transactions_{merged_min_date}_to_{merged_max_date}_merged.csv")
                ui.modal_remove()

        except Exception as e:
            logging.exception("Error while authenticating")
            ui.notification_show(
                f"Error while downloading transactions history: {e}",
                type="error",
                duration=20,
                close_button=True
            )


monzo_app = App(app_ui, server)
