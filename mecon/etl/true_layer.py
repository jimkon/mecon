import json
import pathlib
from datetime import datetime

import pandas as pd
import requests

from mecon.etl.dataset import Dataset
from mecon.etl.transformers import TrueLayerStatementTransformer


def _now():
    return datetime.utcnow().isoformat() + "Z"


class TrueLayerAccount:
    def __init__(self, bank, account, name=None):
        self._bank = bank
        self._account = account
        self._name = name

    def account_creds(self, all_creds):
        return {
            'client_id': all_creds['truelayer']['client_id'],
            'client_secret': all_creds['truelayer']['client_secret'],
            'redirect_uri': all_creds['truelayer']['redirect_uri'],
            'authentication_code': all_creds['truelayer'][self._bank]['authentication_code'],
            'token': all_creds['truelayer'][self._bank]['token'],
        }

    @classmethod
    def from_tl_creds(cls, json_creds):
        tl_accs = []
        sources = json_creds['truelayer']['sources']
        banks = sources.keys()

        for bank in banks:
            bank_accs = sources[bank]['accounts']
            for acc in bank_accs:
                acc_id = acc['account_id']
                dname = acc['display_name'] if 'display_name' in acc else 'no_disp_name'
                provider = acc['provider']['display_name'] if 'provider' in acc and 'display_name' in acc['provider'] else 'no_provider'
                name = f"{provider}_{dname}"

                tl_accs.append(cls(bank, acc_id, name))
        return tl_accs

    def __repr__(self):
        if self._name is not None:
            return f"TrueLayerAccount({self._name})"
        else:
            return f"TrueLayerAccount({self._bank}, {self._account})"




class TrueLayerAPIHandler:
    def __init__(self, dataset: Dataset):
        self._creds = dataset.creds['truelayer']

    @property
    def auth_link(self):
        return f"""
        https://auth.truelayer.com/?response_type=code
        &client_id={self._creds['client_id']}
        &scope=info%20accounts%20transactions%20offline_access
        &redirect_uri={self._creds['redirect_uri']}
        &providers=revolut
        &state=abc123
        &nonce=xyz456
        """.replace('\n', '')

    def fetch_access_token(self, bank):
        data = {
            'grant_type': 'authorization_code',
            'client_id': self._creds['client_id'],
            'client_secret': self._creds['client_secret'],
            'redirect_uri': self._creds['redirect_uri'],
            'code': self._creds["accounts"][bank]['authentication_code'],
        }

        token_res = requests.post('https://auth.truelayer.com/connect/token', data=data)

        if token_res.status_code != 200:
            print(f"Error fetching access token: {token_res.status_code}")
        else:
            token_json = token_res.json()
            token_json['fetched_at'] = _now()
            token_json['refreshed_at'] = None
            self._creds["accounts"][bank]['token'] = token_json
            print(f"Error fetching access token: {token_res.status_code}")


    def refresh_token(self, bank):
        url = "https://auth.truelayer.com/connect/token"

        data = {
            "grant_type": "refresh_token",
            "client_id": self._creds['client_id'],
            "client_secret": self._creds['client_secret'],
            "refresh_token": self._creds["accounts"][bank]['token']['refresh_token'],
        }

        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url, headers=headers, data=data)

        if response.status_code != 200:
            print("❌", response.status_code, response.text)
        else:
            new_token = response.json()
            new_token['fetched_at'] = self._creds["accounts"][bank]['token']['fetched_at'] if 'fetched_at' in self._creds["accounts"][bank] else _now()
            new_token['refreshed_at'] = _now()

            # Save it back into your self._creds structure
            self._creds["accounts"][bank]['token'] = new_token
            self._creds.save()
            print(f"✅Token for '{bank.upper()}' successfully refreshed")

    def authenticated(self, source):
        try:
            self.refresh_token(source)
            return True
        except Exception as e:
            raise e
            return False

    def reauthenticate(self, bank):
        url = "https://auth.truelayer.com/v1/reauthuri"

        payload = {
            "response_type": "code",
            "refresh_token": self._creds["accounts"][bank]['token']['refresh_token'],
            'redirect_uri': self._creds['redirect_uri'],

        }
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)

        if response.status_code == 200:
            print(f"✅ Successfully refreshed!")
        else:
            print(f"❌ Error {response.status_code}: {response.text}")

    def fetch_accounts(self, bank):
        headers = {
            'Authorization': f"Bearer {self._creds['accounts'][bank]['token']['access_token']}"
        }
        acc_res = requests.get('https://api.truelayer.com/data/v1/accounts', headers=headers)
        if acc_res.status_code != 200:
            print(acc_res.text)
            return []
        else:
            self._creds["accounts"][bank]['accounts'] = acc_res.json()['results']
            self._creds.save()
            print(f"Accounts for '{bank.upper()}' successfully refreshed")
            for acc in self._creds["accounts"][bank]['accounts']:
                print(f"{acc['account_id']} | {acc['account_type']} | {acc['display_name']}")
            return [acc['account_id'] for acc in self._creds["accounts"][bank]['accounts']]

    def fetch_transactions(self, bank, account_id, file_pointer: pathlib.Path = None):
        headers = {
            'Authorization': f"Bearer {self._creds['accounts'][bank]['token']['access_token']}",
            "accept": "application/json; charset=UTF-8"
        }

        # from_date = (datetime.date.today() - datetime.timedelta(days=180)).isoformat()
        # to_date = datetime.date.today().isoformat()
        url = f"https://api.truelayer.com/data/v1/accounts/{account_id}/transactions?from=2023-06-01&to=2024-06-2"
        # url = f"https://api.truelayer.com/data/v1/accounts/{account_id}/transactions"

        txn_res = requests.get(url, headers=headers)

        if txn_res.status_code == 200:
            txns = txn_res.json()['results']
            if file_pointer is not None:
                file_pointer.parent.mkdir(parents=True, exist_ok=True)
                file_pointer.write_text(json.dumps(txns, indent=4))
                print(f"✅ Got {len(txns)} transactions and saved to {file_pointer}")
            else:
                print(f"✅ Got {len(txns)} transactions")
            return txns
        else:
            print(f"❌ Error {txn_res.status_code}: {txn_res.text}")
            return None


class TrueLayer:
    _valid_sources = ['HSBC', 'HSBCSVR', "RVLTEUR", "RVLTGBP", "RVLTHUF", "RVLTRON"]
    _source_dict = {
        'hsbc' : {
            'abr': 'HSBC',
            'account_name': 'HSBC ADVANCE',
        }
    }

    def __init__(self, dataset: Dataset):
        self._dataset = dataset
        self._creds = dataset.creds

        self._statements_dir = self._dataset.statements / TrueLayerStatementTransformer.source_name
        self._statements_dir.mkdir(parents=True, exist_ok=True)

    def fetch_hsbc_current(self, api_handler: TrueLayerAPIHandler):
        source_info = self._source_dict['hsbc']
        if not api_handler.authenticated('hsbc'):
            raise ValueError(f"Not authenticated API handler")

        api_handler.refresh_token('hsbc')
        selected_accounts = [crd for crd in self._creds['hsbc']['accounts'] if crd['display_name'] == source_info['account_name']]
        assert len(selected_accounts) == 1
        current_account_id = selected_accounts[0]['account_id']

        json_transactions = api_handler.fetch_transactions(bank='hsbc', account_id=current_account_id)

        transformer = TrueLayerStatementTransformer('HSBC')
        filename = f"HSBC_transactions.csv"
        df = transformer.json_to_csv(json_transactions)
        df.to_csv(self._statements_dir / filename)

    def transactions(self, source):
        transformer = TrueLayerStatementTransformer(source)
        filename = self._statements_dir / f"HSBC_transactions.csv"
        df = pd.read_csv(filename, index_col=0)
        return transformer.transform(df)

if __name__ == '__main__':
    d = Dataset(r"/Users/wimpole/Library/CloudStorage/GoogleDrive-jimitsos41@gmail.com/Other computers/My Laptop/datasets/v2_dataset_true_layer")
    tl_api = TrueLayerAPIHandler(d)
    tl = TrueLayer(d)
    tl.fetch_hsbc_current(tl_api)
    trans = tl.transactions('HSBC')


