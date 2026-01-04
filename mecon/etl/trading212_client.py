import logging
import pathlib
import time
import uuid
from typing import Literal, Optional

from datetime import date, datetime, timezone
from datetime import time as dt_time
import requests

import pandas as pd
from pydantic import BaseModel, Field, RootModel
from tqdm import tqdm

from mecon.etl.dataset import Dataset


def _sleep_for(seconds, message):
    logging.info(message)
    for _ in tqdm(range(seconds), desc='Waiting...'):
        time.sleep(1)


def to_rfc3339_z(time_from: datetime | date) -> str:
    """
    Convert a datetime.datetime or datetime.date to an RFC3339 UTC "Z" string like:
        "2025-08-24T14:15:22Z"

    Edge cases handled:
      - date -> treated as start of day (00:00:00) in UTC
      - naive datetime -> assumed UTC
      - aware datetime -> converted to UTC
      - microseconds -> dropped (API examples use second precision)
    """
    if isinstance(time_from, date) and not isinstance(time_from, datetime):
        dt_utc = datetime.combine(time_from, dt_time(0, 0, 0), tzinfo=timezone.utc)
    elif isinstance(time_from, datetime):
        dt_utc = time_from
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt_utc.astimezone(timezone.utc)
    else:
        raise TypeError(f"time_from must be datetime or date, got {type(time_from)}")

    dt_utc = dt_utc.replace(microsecond=0)
    return dt_utc.isoformat().replace("+00:00", "Z")


class Trading212CredentialsError(Exception):
    pass


class Trading212WrongAPIParametersError(Exception):
    pass


class ReportDataIncluded(BaseModel):
    includeTransactions: bool = False
    includeOrders: bool = False
    includeDividends: bool = False
    includeInterest: bool = False


ReportStatus = Literal[
    "Queued",
    "Processing",
    "Running",
    "Canceled",
    "Failed",
    "Finished",
]


class ExportReport(BaseModel):
    dataIncluded: ReportDataIncluded = Field(default_factory=ReportDataIncluded)
    downloadLink: Optional[str] = None
    reportId: int
    status: ReportStatus
    timeFrom: datetime
    timeTo: datetime


class ExportReportsResponse(RootModel[list[ExportReport]]):
    """
    Response model for GET /equity/history/exports.

    Allows empty responses: [] is valid.
    """
    root: list[ExportReport]


class ReportId(BaseModel):
    reportId: int


class Trading212APICaller:
    def __init__(self, api_key):
        self.base_url = "https://live.trading212.com/api/v0"
        self.api_key = api_key

    def list_generated_reports(
            self,
            retry_after=60
    ) -> ExportReportsResponse:

        url = f"{self.base_url}/equity/history/exports"
        headers = {"Authorization": self.api_key}
        logging.info(f"Trading212 API: Requesting exports history...")

        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 401:
            raise Trading212CredentialsError("Trading212 API: Authentication error while requesting exports history.")
        if r.status_code == 429:
            if retry_after > 0:
                _sleep_for(retry_after,
                           f"Trading212 API: API[list_generated_reports] responded with a 429 'Limited: 1 / 1m0s' status. Waiting for {retry_after} seconds.")
                r = requests.get(url, headers=headers, timeout=30)
            else:
                r.raise_for_status()

        r.raise_for_status()
        if r.status_code == 200:
            exports = r.json()
            logging.info(f"Trading212 API: Requesting exports history returned {len(exports)} exports.")
            exports_obj = ExportReportsResponse.model_validate(exports)
            return exports_obj
        else:
            raise Exception(f"Trading212 API: API returned {r.status_code}.")

    def request_a_csv_report(
            self,
            time_from: datetime | date,
            time_to: datetime | date,
            retry_after=30,
    ) -> ReportId:
        url = f"{self.base_url}/equity/history/exports"

        time_from_str, time_to_str = to_rfc3339_z(time_from), to_rfc3339_z(time_to)

        payload = {
            "dataIncluded": {
                "includeDividends": True,
                "includeInterest": True,
                "includeOrders": True,
                "includeTransactions": True
            },
            "timeFrom": time_from_str,
            "timeTo": time_to_str
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": self.api_key
        }

        r = requests.post(url, json=payload, headers=headers)
        if r.status_code == 400:
            raise Trading212WrongAPIParametersError(
                f"Trading212 API: Bad Request. time_from and time_to parameters should not "
                f"be more than a year apart, {time_from_str} and {time_to_str} was given")
        if r.status_code == 401:
            raise Trading212CredentialsError("Trading212 API: Authentication error while requesting exports history.")
        if r.status_code == 429:
            if retry_after > 0:
                _sleep_for(retry_after,
                           f"Trading212 API: API[request_a_csv_report] responded with a 429 'Limited: 1 / 30s status. Waiting for {retry_after} seconds.")
                r = requests.get(url, headers=headers, timeout=30)
            else:
                r.raise_for_status()

        r.raise_for_status()
        if r.status_code == 200:
            data = r.json()
            report_id = ReportId.model_validate(data)
            return report_id
        else:
            logging.info(f"Trading212 API: API returned {r.status_code} for {r.json()}.")
            raise Exception(f"Trading212 API: API returned {r.status_code}.")

    @staticmethod
    def download_export_report_from_link(download_link) -> pd.DataFrame | None:
        try:
            with requests.get(download_link, stream=True) as r:
                r.raise_for_status()
                logging.info('Trading212 API: Downloading export report...')
                df = pd.read_csv(download_link)
                logging.info(f"Trading212 API: Downloading export report...Done. {len(df)} rows downloaded.")
                return df
        except Exception as e:
            raise

    @staticmethod
    def download_export_report(report: ExportReport) -> pd.DataFrame | None:
        download_link = report.downloadLink
        if download_link is None:
            logging.warning(f"Trading212 API: No download link for {report.reportId}")
            return None
        return Trading212APICaller.download_export_report_from_link(download_link)


    def check_requested_report_status(
            self,
            report_id: int,
    ):
        try:
            reports_list = self.list_generated_reports()
            filtered_reports = [rep for rep in reports_list.root if rep.reportId == report_id]
            if len(filtered_reports) == 0:
                return None
            else:
                report = filtered_reports[0]
                status = report.status
                if status == "Finished":
                    return True
                elif status in ["Canceled"
                                "Failed"]:
                    return None
                else:
                    return False

        except Exception as e:
            raise


class Trading212Client:
    def __init__(self, creds: "DictFile"):
        self.creds = creds
        self.api_key = None
        self.api_caller = None
        self.existing_data = None
        self.reports_list : list[ExportReport] | None = None

    @classmethod
    def from_dataset(cls, dataset: Dataset):
        return cls(dataset.creds)

    def load_api_key(self):
        if not self.creds \
                or 'trading212' not in self.creds \
                or 'api_key' not in self.creds['trading212'] \
                or self.creds['trading212']['api_key'] is None:
            raise Trading212CredentialsError('Trading212 credentials not configured')

        self.api_key = self.creds['trading212']['api_key']
        logging.info('Trading212 credentials loaded.')

    def init_api_caller(self):
        if self.api_key is None:
            raise Trading212CredentialsError('Trading212 API caller cannot be initialised, API_KEY not configured.')
        self.api_caller = Trading212APICaller(self.api_key)
        logging.info('Trading212 API caller initialized.')

    def load_existing_data(self, from_dirpath: pathlib.Path, remove_na=True):
        files = from_dirpath.glob('*.csv')
        df = pd.concat([pd.read_csv(file) for file in files])

        if remove_na:
            self.existing_data = df[~df['_reportId'].isna()].copy()
        else:
            self.existing_data = df

        logging.info(f"Existing data loaded, {len(self.existing_data)} rows (filtered from {len(df)} rows).")

    def existing_data_stats(self):
        df = self.existing_data.copy()
        df['_reportId'] = df['_reportId'].astype(int)
        df['transaction_date'] = df['Time'].str.split().apply(lambda sp: sp[0])
        reports_data = self.existing_data.groupby('_reportId').agg(
            {'_chunk_from': min, '_chunk_to': max}).reset_index().set_index('_reportId').to_dict('index')
        stats = {
            'report_ids': reports_data,
            'actions': df['Action'].value_counts().to_dict(),
            'n_transaction_dates': df['Time'].nunique(),
            'tickers': df['Ticker'].unique().tolist(),
            'currencies': df['Currency (Result)'].value_counts().to_dict(),
            'currency_conversion_total_fees': df['Currency conversion fee'].sum(),
            'transaction_fees': df['Transaction fee'].sum(),
            'finra': df['Finra fee'].sum(),
        }
        return stats

    def last_fetched_date(self) -> str | None:
        if self.existing_data is not None \
                and len(self.existing_data) > 0 \
                and not self.existing_data['_chunk_to'].isna().all():

            datetime_str = self.existing_data[~self.existing_data['_chunk_to'].isna()]['_chunk_to'].max()
            date_str = datetime_str.split()[0]
            return date_str
        else:
            return None

    def list_generated_reports(self) -> list[ExportReport] | None:
        if self.reports_list is None and self.api_key is not None:
            self.reports_list = self.api_caller.list_generated_reports().root
        return self.reports_list

    def download_report(self, report: ExportReport):
        df = self.api_caller.download_export_report(report)
        if len(df) == 0:
            logging.error(f"No data found for {report}")
            return

        _id = report.reportId
        from_str, to_str = str(report.timeFrom)[:10], str(report.timeTo)[:10]
        # filename = self.dataset.statements / 'Trading212API' / f"from_{from_str}_to_{to_str}_rid{_id}.csv"
        # df["_file_name"] = filename
        df["_reportId"] = _id
        df["_chunk_from"] = from_str
        df["_chunk_to"] = to_str
        # df.to_csv(filename, index=False)
        logging.info(f"Trading212Client.download_report_id: Downloaded report {_id}.")
        return df

    def download_report_id(self, report_id):
        if self.reports_list is None or report_id not in [rep.reportId for rep in self.reports_list]:
            self.list_generated_reports()

        filtered_report = [rep for rep in self.reports_list if rep.reportId == report_id]
        if len(filtered_report) == 0:
            raise Exception(f"Trading212Client.download_report_id: {report_id} is not a valid report id.")

        return self.download_report(filtered_report[0])


if __name__ == '__main__':
    from mecon.app.current_data import WorkingDataManager

    data_manager = WorkingDataManager()
    dataset = data_manager.dataset

    client = Trading212Client.from_dataset(dataset)
    client.load_api_key()
    client.init_api_caller()
    client.load_existing_data(dataset.statements / 'Trading212API')
    client.existing_data_stats()
    # t = client.api_caller.list_generated_reports()
    # t = client.api_caller.request_a_csv_report()
    # client.api_caller.check_requested_report_status(report_id=5082567)

    exports = [
        {'dataIncluded': {'includeDividends': True, 'includeInterest': True, 'includeOrders': True,
                          'includeTransactions': True},
         'downloadLink': 'https://tzswiy3zk5dms05cfeo.s3.eu-central-1.amazonaws.com/from_2025-08-24_to_2025-12-24_MTc2NzA5OTYwNjA5MA.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251230T130010Z&X-Amz-SignedHeaders=host&X-Amz-Expires=604799&X-Amz-Credential=AKIARJCCZCDEKCUWYOXG%2F20251230%2Feu-central-1%2Fs3%2Faws4_request&X-Amz-Signature=c2b0bfd4d355f1679dcb6b24a72cae0c9fca34a62b695fd38988c7892de0d8e2',
         'reportId': 5009667, 'status': 'Finished', 'timeFrom': '2025-08-24T14:15:22.000Z',
         'timeTo': '2025-12-24T14:15:22.000Z'},
        {'dataIncluded': {'includeDividends': True, 'includeInterest': True, 'includeOrders': True,
                          'includeTransactions': True},
         'downloadLink': 'https://tzswiy3zk5dms05cfeo.s3.eu-central-1.amazonaws.com/from_2025-08-24_to_2025-12-24_MTc2NzEwNDc4MTU3Nw.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251230T142628Z&X-Amz-SignedHeaders=host&X-Amz-Expires=604799&X-Amz-Credential=AKIARJCCZCDEKCUWYOXG%2F20251230%2Feu-central-1%2Fs3%2Faws4_request&X-Amz-Signature=cbb293d673d456aa8f0e7dffca3f3cc7f02f6d111d9b7a2d4bba895f55c5cba9',
         'reportId': 5010507, 'status': 'Finished', 'timeFrom': '2025-08-24T14:15:22.000Z',
         'timeTo': '2025-12-24T14:15:22.000Z'}
    ]

    # client.api_caller.download_export_report(ExportReport(**exports[1]))

    # wait_duration = 60
    # date_from, date_to = datetime(2025, 10, 23), datetime(2025, 11, 25)
    # report_id = client.api_caller.request_a_csv_report(time_from=date_from, time_to=date_to)
    # is_ready = client.api_caller.check_requested_report_status(report_id.reportId)
    # if is_ready is None:
    #     raise Exception('Not ready')
    # time_start = time.time()
    # while time.time() < time_start + wait_duration and not is_ready:
    #     logging.info(f"Waiting for {wait_duration} seconds...")
    #     time.sleep(10)
    #     is_ready = client.api_caller.check_requested_report_status(report_id.reportId)

    print(client.download_report_id(5009667).shape)
    print(client.download_report_id(5010507).shape)

    t = 0
