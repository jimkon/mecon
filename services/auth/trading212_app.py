import logging
import time

import dateparser
import pandas as pd
from json2html import json2html
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app import shiny_app
from mecon.app.current_data import WorkingDataManager
from mecon.etl.trading212_client import Trading212Client, Trading212CredentialsError, Trading212APICaller



def generate_export_reports_table(trd212_client, input_comps):
    ui.notification_show(
        f"Requesting data from the API, it might take a while...",
        type="default",
        duration=2,
    )
    reports_list = trd212_client.list_generated_reports()
    _export_reports = trd212_client.existing_data_stats()
    df = pd.DataFrame([dict(rep) for rep in reports_list])
    df['timeFrom'] = pd.to_datetime(df['timeFrom']).dt.strftime("%Y-%m-%d")
    df['timeTo'] = pd.to_datetime(df['timeTo']).dt.strftime("%Y-%m-%d")
    df.sort_values(by=["timeTo", "timeFrom"], inplace=True, ascending=False)

    def generate_download_button(report_id):
        btn = ui.input_task_button(
            id=f"{report_id}_download_button",
            label="Download",
        )

        @reactive.effect
        @reactive.event(input_comps[f"{report_id}_download_button"])
        def _():
            ui.notification_show(
                f"Downloading report {report_id}...",
                type="warning",
                duration=2,
            )
            trd212_client.download_report_id(report_id)
            ui.notification_show(
                f"Downloading report {report_id}... Done!",
                type="default",
                duration=2,
            )

        return btn

    # df['downloadLink'] = df['downloadLink'].apply(lambda url: ui.HTML(f"<a href='{url}' target='_blank' rel='noopener'>Download</a>")
    #     if url else "")
    df['downloadLink'] = df['reportId'].apply(lambda rid: generate_download_button(rid))
    df['downloaded'] = df['reportId'].apply(lambda rid: rid in _export_reports['report_ids'].keys())
    return reports_list, df



data_manager = WorkingDataManager()
dataset = data_manager.dataset

client = Trading212Client.from_dataset(dataset)
client.load_existing_data(dataset.statements / 'Trading212API')
last_fetched_date = client.last_fetched_date()
existing_data_stats_dict = client.existing_data_stats()



# from mecon.monitoring.logs import setup_logging

logging.basicConfig()

logging.getLogger().setLevel(logging.INFO)

app_ui = shiny_app.app_ui_factory(
    ui.row(
        ui.tags.a("Monzo auth", href=f"http://127.0.0.1:8003/auth/monzo"),
        ui.tags.a("True Layer auth", href=f"http://127.0.0.1:8003/auth/truelayer"),
        ui.tags.a("Trading 212 API", href=f"http://127.0.0.1:8003/auth/trd212"),
    ),
    ui.card(
        ui.card_header("API"),
        ui.card(
            ui.card_header("API key"),
            ui.output_text(id="api_key_status_text"),
            ui.card_footer(ui.input_action_button(id="api_key_configure_button", label="Configure...", disabled=True)),
        ),
        ui.card(
            ui.card_header("Request new export report"),
            ui.card_body(
                ui.input_date(id="time_from_input_date", label=f"From (last fetched: {str(last_fetched_date)})",
                              value=last_fetched_date),
                ui.input_date(id="time_to_input_date", label="To", value=dateparser.parse('today')),
            ),
            ui.card_footer(
                ui.input_action_button(id="export_reports_request_button", label="Request", disabled=False),
                ui.input_action_button(id="export_reports_request_and_download_button", label="Request and download", disabled=False),
            ),
        ),
        ui.card(
            ui.card_header("Requested export reports"),
            ui.output_data_frame(id="export_reports_table"),
            ui.card_footer(ui.input_action_button(id="export_reports_refresh_button", label="Refresh", disabled=False)),
        ),
        ui.card(
            ui.card_header("Existing data"),
            ui.output_ui(id="existing_data_stats_table"),
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    try:
        client.load_api_key()
        client.init_api_caller()
        reports_list, export_reports_df = generate_export_reports_table(client, input_comps=input)
    except Trading212CredentialsError as e:
        logging.warning(f"Trading212 credentials error: {e}")
        export_reports_df = pd.DataFrame()

    export_reports_df_value = reactive.Value(export_reports_df)

    @render.text
    def api_key_status_text():
        return f"**** ({len(str(client.api_key))})" if client.api_key else "No API key"

    @render.data_frame
    def export_reports_table():
        return shiny_app.render_grid_standard(export_reports_df_value.get(), format_boolean_values=True)

    @render.ui
    def existing_data_stats_table():
        return ui.markdown(json2html.convert(existing_data_stats_dict))

    @reactive.effect
    @reactive.event(input.export_reports_request_button)
    def _():
        try:
            date_from, date_to = input.time_from_input_date(), input.time_to_input_date()
            if date_from >= date_to:
                ui.notification_show(
                    f"'From' date cannot be greater or equal to the 'To' date -> ERROR: {date_from} >= {date_to}",
                    type="error",
                    duration=5
                )
                return
            report_id = client.api_caller.request_a_csv_report(time_from=date_from, time_to=date_to)
            ui.notification_show(
                f"Requested a CSV report from {date_from} to {date_to}. Report ID will be '{report_id}'",
                type="default",
            )
        except Exception as e:
            logging.exception(e)
            ui.notification_show(
                f"Failed to request a CSV report because of {e}",
                type="error",
            )

    @reactive.effect
    @reactive.event(input.export_reports_request_and_download_button)
    def _():
        try:
            wait_duration = 60
            max_wait_duration = 180
            date_from, date_to = input.time_from_input_date(), input.time_to_input_date()
            report_id = client.api_caller.request_a_csv_report(time_from=date_from, time_to=date_to)
            ui.notification_show(
                f"Requested a CSV report from {date_from} to {date_to}. Report ID will be '{report_id}'. Waiting for the request to finish (max {wait_duration} seconds).",
                type="default",
                duration=None,
                id="request_wait_notification"
            )
            is_ready = client.api_caller.check_requested_report_status(report_id.reportId)
            if is_ready is None:
                raise Exception('Not ready')
            time_start = time.time()
            while time.time() < time_start + max_wait_duration and not is_ready:
                logging.info(f"Waiting for {wait_duration} seconds...")
                time.sleep(wait_duration)
                is_ready = client.api_caller.check_requested_report_status(report_id.reportId)

            if is_ready:
                ui.notification_show(
                    f"Downloading report {report_id}...",
                    type="warning",
                    duration=2,
                )
                client.download_report_id(report_id)
                ui.notification_show(
                    f"Downloading report {report_id}... Done!",
                    type="default",
                    duration=2,
                )
            else:
                ui.notification_show(
                    f"Downloading report {report_id} went wrong!",
                    type="error",
                    duration=5,
                )

        except Exception as e:
            logging.exception(e)
            ui.notification_show(
                f"Failed to request a CSV report because of {e}",
                type="error",
            )

    @reactive.effect
    @reactive.event(input.export_reports_refresh_button)
    def _():
        global reports_list, export_reports_df
        try:
            client.load_api_key()
            client.init_api_caller()
            reports_list, export_reports_df = generate_export_reports_table(client, input_comps=input)
        except Trading212CredentialsError as e:
            logging.warning(f"Trading212 credentials error: {e}")
            export_reports_df = pd.DataFrame()

        export_reports_df_value.set(export_reports_df)




app = App(app_ui, server)
