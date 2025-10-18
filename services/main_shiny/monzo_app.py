import logging
import shutil

import pandas as pd
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app import shiny_app
# from mecon.app.current_data import WorkingDatasetDirInfo
from mecon.etl.transformers import MonzoFileStatementTransformer

import monzo_api_lib as monzo

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def create_monzo_client_for_dataset(dataset):
    token_filepath = dataset.db.parent.parent.parent / 'monzo-api.json'
    return monzo.MonzoClient(token_file=token_filepath)


def monzo_status_html(monzo_client):
    if monzo_client.has_token():
        return ui.HTML(f"Authenticated until {monzo_client.expires_at()}.")
    return ui.HTML("No token found.")


def refresh_token_and_messages(monzo_client):
    monzo_client.refresh_token()
    status_text = f"NEW: Authenticated until {monzo_client.expires_at()}."
    notification_message = "Proceed to refresh token from the Monzo App"
    return status_text, notification_message


def build_authentication_modal(auth_url):
    instructions = ui.HTML(
        f"<ol><li>{ui.tags.a('Redirect to...', href=auth_url)}</li>"
        "<li>Put your email and click 'Continue'</li>"
        "<li>Go to your emails and open the last one from Monzo</li>"
        "<li>Copy the link address from the 'Log in to Monzo' button inside the email</li>"
        f"<li>{ui.input_text(id='monzo_authentication_url_input_text', label='Paste the link address here and click Submit:', value='https://localhost/?code=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx&state=yyyyyyyyyyyyy')}"
        "" + str(ui.input_task_button(id="monzo_authentication_url_input_action_button", label="Submit")) + "</li>"
        "<li>Go to your Monzo app and approve the authentication request</li></ol>"
    )
    return ui.modal(
        instructions,
        title="Monzo API authentication",
        easy_close=True,
        size='xl'
    )


def process_authentication_response(monzo_client, response_url: str):
    monzo_client.set_authentication_code_from_url(response_url)
    status_text = f"NEW: Authenticated until {monzo_client.expires_at()}."
    notification_message = (
        f"Successfully authenticated until {monzo_client.expires_at()}. You might need to refresh the main page"
    )
    return status_text, notification_message


def fetch_monzo_transactions_dataframe(monzo_client):
    df_raw = monzo_client.download_full_history()
    df_raw['created_date'] = pd.to_datetime(df_raw['created'].apply(lambda datetime_str: datetime_str[:10]))
    return df_raw


def summarise_new_transactions(df_raw, monzo_client):
    min_date = df_raw['created_date'].dt.date.min()
    max_date = df_raw['created_date'].dt.date.max()
    days_in_between = (max_date - min_date).days
    n_unique_days = len(set(df_raw['created_date'].dt.date))
    minutes_since_token_creation = monzo_client.minutes_passed_from_token_creation()
    minutes_since_token_creation = (
        int(minutes_since_token_creation) if minutes_since_token_creation is not None else '[None]'
    )
    return {
        'row_count': len(df_raw),
        'column_count': len(df_raw.columns),
        'min_date': min_date,
        'max_date': max_date,
        'days_in_between': days_in_between,
        'n_unique_days': n_unique_days,
        'minutes_since_token_creation': minutes_since_token_creation,
    }


def load_previous_transactions(dataset):
    prev_statements = [path for path, *_ in dataset.statement_files_info()['MonzoAPI']]
    logging.info(f"{prev_statements=}")
    if not prev_statements:
        return None, 'No previous statements found'

    df_prev = pd.concat([pd.read_csv(file, index_col=None) for file in prev_statements])
    df_prev['created_date'] = pd.to_datetime(df_prev['created'].apply(lambda datetime_str: datetime_str[:10]))
    message = (
        f"Previous statements >>{prev_statements}<< contain {len(df_prev)} transactions with {df_prev.shape[1]} columns. "
        f"The date range of the transactions is from {df_prev['created'].min()} to {df_prev['created'].max()}, "
        f"{df_prev['id'].duplicated(keep=False).sum()} duplicated ids"
    )
    df_prev.drop_duplicates(subset='id', inplace=True)
    return df_prev, message


def build_transactions_summary_html(summary):
    return ui.HTML(
        f"Downloaded <b>{summary['row_count']}</b> transactions with {summary['column_count']} columns."
        f"The date range of the transactions is from <b>{summary['min_date']}</b> to <b>{summary['max_date']}</b> "
        f"(<b>{summary['days_in_between']} days period, {summary['n_unique_days']} unique</b>). "
        f"Task run <b>{summary['minutes_since_token_creation']}</b> minutes after token creation, it needs to run less that <b>5</b> minutes "
        f"before token creations to parse all transactions, consider refreshing the token.<br>"
    )


def build_transactions_modal(df_raw, df_prev, summary, prev_stats_message):
    modal_body = build_transactions_summary_html(summary)
    merge_button = ui.input_task_button(id="merge_previous_transactions_button", label="Merge with previous transactions?", disabled=df_prev is None)
    tooltip = ui.tooltip(merge_button, prev_stats_message)
    return ui.modal(
        modal_body,
        tooltip,
        ui.input_task_button(id="monzo_save_transactions_button", label="Save Monzo statement"),
        ui.navset_tab(
            ui.nav_panel('New data', ui.output_data_frame('fetched_transactions_dataframe')),
            ui.nav_panel('Old data', ui.output_data_frame('old_transactions_dataframe')),
            id='fetched_data_navset_tab',
        ),
        title="Transactions fetched from Monzo",
        easy_close=True,
        size='xl'
    )


def save_monzo_statement(dataset, df, name):
    monzo_dir = dataset.statements / 'MonzoAPI'
    backup_dir = monzo_dir / 'backup'
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
    backup_dir.mkdir()
    for file in monzo_dir.iterdir():
        if file != backup_dir:
            shutil.move(str(file), backup_dir)
    df.to_csv(monzo_dir / name, index=False)
    return f"Successfully saved {len(df)} Monzo transactions as {name} in {monzo_dir}, and backed up previous statements in {backup_dir}."


def merge_transactions(df_prev, df_raw):
    df_merged = pd.concat([df_prev, df_raw]).reset_index(drop=True)
    df_merged['created_date'] = pd.to_datetime(df_merged['created'].apply(lambda datetime_str: datetime_str[:10]))
    size_before_merge = df_prev.shape[0]
    merged_min_date = df_merged['created_date'].dt.date.min()
    merged_max_date = df_merged['created_date'].dt.date.max()
    duplicates = df_merged['id'].duplicated(keep=False).sum()
    df_merged.drop_duplicates(subset='id', inplace=True)
    size_after_merge = df_merged.shape[0]
    added = size_after_merge - size_before_merge
    logging.info(
        f"Merging {merged_min_date=}, {merged_max_date=}, {duplicates} duplicated transactions, added {added} transactions"
    )
    return df_merged, merged_min_date, merged_max_date


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
    monzo_client = create_monzo_client_for_dataset(dataset)

    @render.text
    def monzo_info_text():
        return monzo_status_html(monzo_client)

    @reactive.effect
    @reactive.event(input.monzo_refresh_token_button)
    def monzo_refresh_token_button_clicked():
        try:
            status_text, notification_message = refresh_token_and_messages(monzo_client)
            ui.update_text(id='monzo_info_text', label='Status', value=status_text)
            ui.notification_show(
                notification_message,
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
            ui.modal_show(build_authentication_modal(auth_url))

            @reactive.effect
            @reactive.event(input.monzo_authentication_url_input_action_button)
            def monzo_authentication_url_input_action_button_clicked():
                try:
                    status_text, notification_message = process_authentication_response(
                        monzo_client,
                        input.monzo_authentication_url_input_text()
                    )
                    ui.modal_remove()
                    ui.update_text(id='monzo_info_text', value=status_text)

                    ui.notification_show(
                        notification_message,
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
            df_raw = fetch_monzo_transactions_dataframe(monzo_client)
            summary = summarise_new_transactions(df_raw, monzo_client)
            df_prev, prev_stats_message = load_previous_transactions(dataset)
            ui.modal_show(build_transactions_modal(df_raw, df_prev, summary, prev_stats_message))

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
                message = save_monzo_statement(
                    dataset,
                    df_raw,
                    f"monzo_api_transactions_{summary['min_date']}_to_{summary['max_date']}.csv"
                )
                logging.info(message)
                ui.notification_show(
                    message,
                    type="message",
                    duration=10,
                    close_button=True
                )
                ui.modal_remove()

            @reactive.effect
            @reactive.event(input.merge_previous_transactions_button)
            def merge_previous_transactions_button_clicked():
                df_merged, merged_min_date, merged_max_date = merge_transactions(df_prev, df_raw)
                message = save_monzo_statement(
                    dataset,
                    df_merged,
                    f"monzo_api_transactions_{merged_min_date}_to_{merged_max_date}_merged.csv"
                )
                logging.info(message)
                ui.notification_show(
                    message,
                    type="message",
                    duration=10,
                    close_button=True
                )
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
