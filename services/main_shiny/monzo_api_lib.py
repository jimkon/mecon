import logging
import pathlib
import json
import datetime
import os
import pandas as pd

from monzo.authentication import Authentication
from monzo.endpoints.account import Account
from monzo.handlers.filesystem import FileSystem
from monzo.monzo import Monzo
from monzo.errors import ForbiddenError, BadRequestError


logging.basicConfig(level=logging.INFO)

class EnchancedFileSystem(FileSystem):
    def store(
            self,
            access_token: str,
            client_id: str,
            client_secret: str,
            expiry: int,
            refresh_token: str = ''
    ) -> None:
        """
        Store the Monzo credentials.

        Args:
            access_token: New access token
            client_id: Monzo client ID
            client_secret: Monzo client secret
            expiry: Access token expiry as a unix timestamp
            refresh_token: Refresh token that can be used to renew an access token
        """
        content = {
            'access_token': access_token,
            'client_id': client_id,
            'client_secret': client_secret,
            'expiry': expiry,
            'refresh_token': refresh_token,
            'created_at': datetime.datetime.now().timestamp(),
        }
        with open(self._file, 'w') as handler:
            handler.write(json.dumps(content, indent=4))
        logging.info("Successfully stored access token")


class MonzoClient:
    def __init__(self, token_file):
        self.token_file = pathlib.Path(token_file)
        self.filesystem_handler = EnchancedFileSystem(token_file)
        self.monzo = None

        if not self.token_file.exists():
            self.monzo_auth = Authentication(
                client_id=os.getenv('MONZO_CLIENT_ID'),
                client_secret=os.getenv('MONZO_CLIENT_SECRET'),
                redirect_url=os.getenv('MONZO_REDIRECT_URI'),
            )
        else:
            content = self.filesystem_handler.fetch()
            self.created_at = content['created_at'] if 'created_at' in content else 0

            self.monzo_auth = Authentication(
                client_id=content['client_id'],
                client_secret=content['client_secret'],
                redirect_url=os.getenv('MONZO_REDIRECT_URI'),
                access_token=content['access_token'],
                access_token_expiry=content['expiry'],
                refresh_token=content['refresh_token']
            )

        self.monzo_auth.register_callback_handler(self.filesystem_handler)


    def has_token(self):
        return self.monzo_auth.access_token != ''

    def expires_at(self):
        return datetime.datetime.fromtimestamp(float(self.monzo_auth.access_token_expiry)).strftime('%Y-%m-%d %H:%M:%S') if self.expires_at else None

    def created_at_secs(self):
        return float(self.created_at) if self.created_at else None

    def minutes_passed_from_token_creation(self):
        created_at_secs = self.created_at_secs()
        if created_at_secs is None:
            return None
        now_secs = datetime.datetime.now().timestamp()
        return (now_secs - created_at_secs) / 60.0

    def authenticated(self):
        return self.has_token()

    def refresh_token(self):
        logging.info("Refreshing token...")
        self.monzo_auth.refresh_access()
        logging.info("Refreshing token... Done")

    def get_authentication_url(self):
        return self.monzo_auth.authentication_url

    def set_authentication_code_from_url(self, response_url):
        code_and_state = response_url.split('code=')[1]
        code, state = code_and_state.split('&state=')
        self.monzo_auth.authenticate(authorization_token=code, state_token=state)


    def download_full_history(self, batch_size=100, start_date="2019-01-01T00:00:00Z"):
        account_id = os.getenv('MONZO_ACCOUNT_ID')
        if account_id is None:
            accounts = Account.fetch(self.monzo_auth)
            account = accounts[0]
            account_id = account.account_id

        monzo = Monzo(self.monzo_auth.access_token)

        since = start_date
        before = str(int(since[:4])+1)+since[4:]
        dfs = []
        recently_authenticated = True
        while True:
            logging.info(f"Downloading history from {account_id} before {before=} and {since=}...")
            try:
                transactions = monzo.get_transactions(account_id,
                                                       before=before,
                                                       since=since,
                                                       limit=batch_size
                                                      )
                df = pd.json_normalize(transactions['transactions'])
                if len(df) == 0:
                    logging.info(f"Reached the limit, no more transactions to download: {len(df) == 0=} {df.shape=}")
                    break
                dfs.append(df)
                new_since = df['created'].to_list()[-1]#.isoformat()[:19] + "Z"
                if new_since == since:
                    logging.info(f"Reached the limit, no more transactions to download: {new_since == since=} {df.shape=}")
                    break
                since = new_since
                df['created_date'] = pd.to_datetime(df['created'].apply(lambda s: s[:10]))
                before = (df['created_date'].to_list()[-1]+datetime.timedelta(days=365)).isoformat()[:19] + "Z"
                logging.info(
                    f"{len(dfs)} batch downloaded with dims {df.shape}, and date range from {df['created_date'].max()} to {df['created_date'].min()}")

            except (BadRequestError, ForbiddenError) as e:
                logging.info(f"Reached the limit, authenticate again for the full history: {e}")
                since = (datetime.datetime.utcnow() - datetime.timedelta(days=89, minutes=59, seconds=59)).isoformat() + "Z"
                before = None
                recently_authenticated = False
            except IndexError as e:
                logging.info(f"Reached the limit, no more transactions to download: {e}")
                break
            except Exception as e:
                raise

        all_transactions = pd.concat(dfs).reset_index(drop=True)
        all_transactions.drop_duplicates(subset='id', inplace=True)
        all_transactions.sort_values(by=['created'], inplace=True)

        logging.info(
            f"Downloaded {len(all_transactions)} transactions with dims {all_transactions.shape}\n"
            f"date range from {all_transactions['created_date'].max()} to {all_transactions['created_date'].min()}\n"
            f"{all_transactions['created_date'].dt.date.nunique()} unique days\n"
            f"{all_transactions['id'].duplicated(keep=False).sum()} duplicated transactions\n")

        return all_transactions

