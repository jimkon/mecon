import logging
import pathlib

import monzo
import pandas as pd
import json
import datetime
import os

from monzo.errors import UnauthorizedError

logging.basicConfig(level=logging.INFO)

# Configuration
CLIENT_ID = os.getenv('MONZO_CLIENT_ID')
CLIENT_SECRET = os.getenv('MONZO_CLIENT_SECRET')
REDIRECT_URI = os.getenv('MONZO_REDIRECT_URI')


class MonzoClient:
    def __init__(self, token_file):
        self.token_file = token_file
        client_id = os.getenv('MONZO_CLIENT_ID')
        client_secret = os.getenv('MONZO_CLIENT_SECRET')
        # redirect_uri = os.getenv('MONZO_REDIRECT_URI')
        access_token = None
        refresh_token = None
        expires_at = None
        token_file = pathlib.Path(token_file)
        if token_file.exists():
            logging.info(f"MonzoClient init with token file {token_file}")
            token = json.loads(self.token_file.read_text())
            access_token = token.get('access_token')
            refresh_token = token.get('refresh_token')
            expires_at = token.get('expires_at')

        self.oauth_client = monzo.MonzoOAuth2Client(client_id, client_secret,
                                                    access_token=access_token,
                                                    refresh_token=refresh_token,
                                                    expires_at=expires_at,
                                                    refresh_callback=self._save_token_to_file_callback)

    def has_token(self):
        return self.oauth_client.session.token['access_token'] is not None

    def expires_at(self):
        if self.has_token() and 'expires_at' in self.oauth_client.session.token:
            secs = float(self.oauth_client.session.token['expires_at'])
            return datetime.datetime.fromtimestamp(secs).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return None

    def created_at(self):
        if self.has_token() and 'created_at' in self.oauth_client.session.token:
            secs = float(self.oauth_client.session.token['created_at'])
            return datetime.datetime.fromtimestamp(secs).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return None

    def created_at_secs(self):
        if self.has_token() and 'created_at' in self.oauth_client.session.token:
            secs = float(self.oauth_client.session.token['created_at'])
            return secs
        else:
            return None

    def minutes_passed_from_token_creation(self):
        created_at_secs = self.created_at_secs()
        if created_at_secs is None:
            return None
        now_secs = datetime.datetime.now().timestamp()
        secs_passed = now_secs - created_at_secs
        minutes_passed = secs_passed / 60.0
        return minutes_passed

    def refresh_token(self):
        logging.info(f"Refreshing token...")
        self.oauth_client.refresh_token()
        logging.info(f"Refreshing token... Done")

    def get_authentication_url(self):
        self.oauth_client = monzo.MonzoOAuth2Client(CLIENT_ID,
                                                    CLIENT_SECRET,
                                                    redirect_uri=REDIRECT_URI,
                                                    refresh_callback=self._save_token_to_file_callback
                                                    )
        auth_start_url, state = self.oauth_client.authorize_token_url()
        return auth_start_url

    def set_authentication_code(self, auth_code):
        logging.info(f"Fetching access token...")
        self.oauth_client.fetch_access_token(auth_code)
        logging.info(f"Fetching access token... Done")

    def set_authentication_code_from_url(self, auth_code_url):
        auth_code = auth_code_url.split('?code=')[1].split('&state')[0]
        self.set_authentication_code(auth_code)
        self.save_token()

    def _save_token_to_file_callback(self, token):
        """Saves a token dictionary to a json file"""
        token['client_secret'] = CLIENT_SECRET
        token['create_at'] = datetime.datetime.now().timestamp()
        with open(self.token_file, 'w') as fp:
            json.dump(token, fp, sort_keys=True, indent=4)
        logging.info(f"Callback: Saved token to {self.token_file}")

    def save_token(self):
        logging.info(f"Saving token to {self.token_file}...")
        token = self.oauth_client.session.token
        self._save_token_to_file_callback(token)
        logging.info(f"Saving token to {self.token_file}... Done")

    def download_full_history(self, batch_size=100):
        client = monzo.Monzo.from_oauth_session(self.oauth_client)
        account_id = client.get_first_account()['id']

        before = None
        dfs = []
        while True:
            logging.info(f"Downloading history from {account_id} before {before}...")
            try:
                transactions = client.get_transactions(account_id,
                                                       before=before,
                                                       limit=batch_size)
            except monzo.errors.ForbiddenError as e:
                logging.info(f"Reached the limit: {e}")
                break

            tr_table = pd.json_normalize(transactions['transactions'])
            if len(tr_table) == 0:
                break
            dfs.append(tr_table)
            logging.info(
                f"{len(dfs)} batch downloaded with dims {tr_table.shape}, and date range from {tr_table['created'].min()} to {tr_table['created'].max()}")
            before = tr_table['created'].min(skipna=True)

        all_transactions = pd.concat(dfs)
        logging.info(
            f"Downloaded {len(all_transactions)} batches with dims {all_transactions.shape}, and date range from {all_transactions['created'].min()} to {all_transactions['created'].max()}")

        return all_transactions

