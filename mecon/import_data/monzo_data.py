import json
import pathlib
from datetime import datetime
import logging
from io import StringIO
from typing import Mapping

import pandas as pd
from monzo.authentication import Authentication
from monzo.endpoints.account import Account


from mecon import config


class MonzoClient:
    def __init__(self):
        monzo_creds = (config.CREDS_DIRECTORY_PATH / 'monzo').read_text().split(',')
        self._client_id = monzo_creds[0]  # Client ID obtained when creating Monzo client
        self._client_secret = monzo_creds[1]  # Client secret obtained when creating Monzo client
        self._redirect_uri = monzo_creds[
            2]  # 'http://127.0.0.1/monzo'  # URL requests via Monzo will be redirected in a browser

        self._monzo = None

    def is_authenticated(self):
        if self._monzo is not None:
            Account.fetch(self._monzo)
            return True
        return

    def token_expiry(self):
        if self.is_authenticated():
            return datetime.utcfromtimestamp(self._monzo.access_token_expiry)
        return None

    def start_authentication(self):
        self._monzo = Authentication(client_id=self._client_id,
                                     client_secret=self._client_secret,
                                     redirect_url=self._redirect_uri)
        return self._monzo.authentication_url

    def finish_authentication(self, token, state):
        self._monzo.authenticate(authorization_token=token, state_token=state)
        logging.info(f"Monzo client authenticated.")

    def download_transactions(self, filepath: str | pathlib.Path):
        accounts = Account.fetch(self._monzo)
        if len(accounts) > 1:
            logging.warning('WARNING: More than one accounts found in Monzo Bank. The 1st will be used.')

        account_id = accounts[0].account_id

        transactions_json = self._monzo.make_request('/transactions', data={
            'account_id': account_id,
            'expand': 'merchant'
        })['data']

        with open(filepath, 'w') as fp:
            json.dump(transactions_json['data']['transactions'], fp, indent=4)


class MonzoDataTransformer:
    def __init__(self, json_data):
        self._data = json_data

    def to_dataframe(self):
        return self.convert_transactions_to_df(self._data)

    @classmethod
    def from_json_file(cls, filename):
        with open(filename, 'r') as fp:
            json_data = json.load(fp)
        return MonzoDataTransformer(json_data)

    @staticmethod
    def convert_transactions_to_df(transactions):
        def look_for(_dict, keys, default=None):
            if _dict is None:
                return default

            subdict = _dict
            for key in keys:
                if isinstance(subdict, Mapping) and key in subdict and subdict[key] is not None:
                    subdict = subdict[key]
                else:
                    return default
            return subdict

        transactions_json = []
        for transaction in transactions:
            date = transaction['created'][:10]  # datetime.strptime(transaction['created'][:10], "%Y-%m-%d").strftime("%d/%m/%Y")
            time = transaction['created'][11:19]
            _type = "Faster Payment" if look_for(transaction, ['metadata', 'faster_payment']) \
                        else "Monzo-to-Monzo" if look_for(transaction, ['counterparty', 'account_id']) \
                        else "Card Payment"
            name = look_for(transaction, ['counterparty', 'name']) or look_for(transaction, ['merchant', 'name'])
            emoji = look_for(transaction, ['merchant', 'emoji'])
            notes_and_tags = look_for(transaction, ['notes'], default='') +' tags=('+ look_for(transaction, ['merchant', 'suggested_tags'], default='').replace('#', '')+')'
            address = look_for(transaction, ['merchant', 'address', 'formatted']) \
                      or look_for(transaction, ['merchant', 'address', 'short_formatted']) \
                      or look_for(transaction, ['merchant', 'address', 'address']) \
                      or '[Online]'

            transactions_dict = {
                'Transaction ID': transaction['id'],
                'Date': date,
                'Time': time,
                'Type': _type,
                'Name': name,
                'Emoji': emoji,
                'Category': transaction['category'],
                'Amount': float(transaction['amount']) / 100,
                'Currency': transaction['currency'],
                'Local amount': float(transaction['local_amount']) / 100,
                'Local currency': transaction['local_currency'],
                'Notes and #tags': notes_and_tags,
                'Address': address,
                'Receipt': '',
                'Category split': transaction['categories'],
                'Description': transaction['description'],
                'Money In': float(transaction['amount']) / 100 if float(transaction['amount']) / 100 <= 0 else '',
                'Money Out': float(transaction['amount']) / 100 if float(transaction['amount']) / 100 > 0 else '',
            }
            transactions_json.append(transactions_dict)

        df = pd.read_json(StringIO(json.dumps(transactions_json)))
        df['Date'] = df['Date'].dt.strftime("%Y-%m-%d")
        return df

