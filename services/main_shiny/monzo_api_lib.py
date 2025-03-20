import logging
import pathlib
import json
import datetime
import os
import pandas as pd

from monzo.authentication import Authentication
from monzo.endpoints.account import Account
from monzo.exceptions import MonzoError

logging.basicConfig(level=logging.INFO)

class MonzoClient:
    def __init__(self, token_file):
        self.token_file = pathlib.Path(token_file)
        self._load_token_file()

        self.monzo = Authentication(
            client_id=os.getenv('MONZO_CLIENT_ID'),
            client_secret=os.getenv('MONZO_CLIENT_SECRET'),
            redirect_url=os.getenv('MONZO_REDIRECT_URI'),
            access_token=self.access_token,
            access_token_expiry=self.expires_at,
            refresh_token=self.refresh_token
        )

    def _load_token_file(self):
        if self.token_file.exists():
            logging.info(f"MonzoClient init with token file {self.token_file}")
            token = json.loads(self.token_file.read_text())
            self.access_token = token.get('access_token')
            self.refresh_token = token.get('refresh_token')
            self.expires_at = token.get('expires_at')
        else:
            self.access_token = self.refresh_token = self.expires_at = None

    def has_token(self):
        return self.access_token is not None

    def expires_at(self):
        return datetime.datetime.fromtimestamp(float(self.expires_at)).strftime('%Y-%m-%d %H:%M:%S') if self.expires_at else None

    def created_at_secs(self):
        return float(self.expires_at) if self.expires_at else None

    def minutes_passed_from_token_creation(self):
        created_at_secs = self.created_at_secs()
        if created_at_secs is None:
            return None
        now_secs = datetime.datetime.now().timestamp()
        return (now_secs - created_at_secs) / 60.0

    def refresh_token(self):
        logging.info("Refreshing token...")
        self.monzo.refresh_access_token()
        self._save_token_to_file_callback(self.monzo.access_token)
        logging.info("Refreshing token... Done")

    def get_authentication_url(self):
        return self.monzo.get_authorization_url()

    def set_authentication_code(self, auth_code):
        logging.info("Fetching access token...")
        self.monzo.fetch_access_token(auth_code)
        self._save_token_to_file_callback(self.monzo.access_token)
        logging.info("Fetching access token... Done")

    def set_authentication_code_from_url(self, auth_code_url):
        auth_code = auth_code_url.split('?code=')[1].split('&state')[0]
        self.set_authentication_code(auth_code)

    def _save_token_to_file_callback(self, token):
        token_data = {
            'access_token': self.monzo.access_token,
            'refresh_token': self.monzo.refresh_token,
            'expires_at': datetime.datetime.now().timestamp() + self.monzo.access_token_expiry
        }
        with open(self.token_file, 'w') as fp:
            json.dump(token_data, fp, sort_keys=True, indent=4)
        logging.info(f"Callback: Saved token to {self.token_file}")

    def download_full_history(self, batch_size=100):
        accounts = Account.fetch(self.monzo)
        account_id = accounts[0].id

        dfs = []
        before = None

        while True:
            logging.info(f"Downloading history from {account_id} before {before}...")
            transactions = accounts[0].transactions(before=before, limit=batch_size)
            if not transactions:
                break

            tr_table = pd.json_normalize([t.__dict__ for t in transactions])
            dfs.append(tr_table)
            logging.info(
                f"{len(dfs)} batch downloaded with dims {tr_table.shape}, and date range from {tr_table['created'].min()} to {tr_table['created'].max()}"
            )
            before = tr_table['created'].min(skipna=True)

        all_transactions = pd.concat(dfs)
        logging.info(
            f"Downloaded {len(all_transactions)} batches with dims {all_transactions.shape}, and date range from {all_transactions['created'].min()} to {all_transactions['created'].max()}"
        )

        return all_transactions

if __name__ == '__main__':
    mc = MonzoClient("/Users/wimpole/Library/CloudStorage/GoogleDrive-jimitsos41@gmail.com/Other computers/My Laptop/datasets/shared/monzo.json")


    pass

