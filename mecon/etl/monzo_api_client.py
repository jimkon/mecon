# mecon/etl/monzo_api_client.py

import logging
import datetime
import pandas as pd

from monzo.authentication import Authentication
from monzo.endpoints.account import Account
from monzo.monzo import Monzo
from monzo.errors import ForbiddenError, BadRequestError

from mecon.settings import DictFile

logging.basicConfig(level=logging.INFO)


class MonzoCredentialsError(Exception):
    pass


def _fmt_rfc3339_seconds(value) -> str:
    """
    Return UTC RFC3339 with **seconds precision** (no microseconds), e.g. 2025-08-14T10:11:12Z.
    Accepts str/datetime; assumes naive datetimes are UTC.
    """
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        # fall back to raw string if we really can't parse
        return str(value)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _plus_seconds(rfc3339: str, secs: int) -> str:
    ts = pd.to_datetime(rfc3339, utc=True, errors="coerce")
    if pd.isna(ts):
        return rfc3339
    return (ts + pd.Timedelta(seconds=secs)).strftime("%Y-%m-%dT%H:%M:%SZ")


class MonzoClient:
    def __init__(self, creds_file: "DictFile", force_new_token=False):
        self.creds_file = creds_file

        if 'monzo-api' not in creds_file:
            raise MonzoCredentialsError("No credentials for 'monzo-api' found in the creds file")

        self.monzo_creds = self.creds_file['monzo-api']

        token = {} if force_new_token else self.monzo_creds.get('token', {})
        self.monzo_auth = Authentication(
            client_id=self.monzo_creds['client_id'],
            client_secret=self.monzo_creds['client_secret'],
            redirect_url=self.monzo_creds['redirect_url'],
            access_token=token.get('access_token', ''),
            access_token_expiry=token.get('expiry', 0),
            refresh_token=token.get('refresh_token', '')
        )

    # -------- token helpers --------
    def has_token(self):
        return bool(self.monzo_auth.access_token)

    def expires_at(self):
        try:
            exp = float(self.monzo_auth.access_token_expiry)
        except (TypeError, ValueError):
            return None
        if not exp:
            return None
        return datetime.datetime.fromtimestamp(exp).strftime('%Y-%m-%d %H:%M:%S')

    def created_at_secs(self):
        try:
            return float(self.monzo_creds.get('created_at', 0))
        except (TypeError, ValueError):
            return None

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
        self._refresh_token_in_creds()
        logging.info("Refreshing token... Done")

    def _refresh_token_in_creds(self):
        self.monzo_creds['created_at'] = datetime.datetime.now().timestamp()
        self.monzo_creds['token'] = {
            'access_token': self.monzo_auth.access_token,
            'expiry': self.monzo_auth.access_token_expiry,
            'expires_at': self.expires_at(),
            'refresh_token': self.monzo_auth.refresh_token,
        }
        logging.info(f"Saving new token...")
        self.creds_file.save()

    # -------- OAuth helpers --------
    def get_authentication_url(self):
        return self.monzo_auth.authentication_url

    def get_authentication_url_and_state(self):
        url_and_state = self.get_authentication_url()
        url, state = url_and_state.split('&state=')
        return url, state

    def set_authentication_code_from_url(self, response_url):
        code_and_state = response_url.split('code=')[1]
        code, state = code_and_state.split('&state=')
        logging.info("Authenticating with Monzo...")
        self.monzo_auth.authenticate(authorization_token=code, state_token=state)
        self._refresh_token_in_creds()
        logging.info("Authentication complete. Token expires at: %s", self.expires_at())

    # -------- accounts --------
    def get_accounts(self):
        if 'accounts' not in self.monzo_creds or len(self.monzo_creds['accounts']) == 0:
            logging.info("Fetching account info for Monzo-API")
            accounts = Account.fetch(self.monzo_auth)
            accounts_dict = []
            for account in accounts:
                created = getattr(account, "created", None)
                created_str = created.strftime('%Y-%m-%d %H:%M:%S') if created else None
                accounts_dict.append({
                    "account_id": getattr(account, "account_id", getattr(account, "id", None)),
                    "created_at": created_str,
                })
            self.monzo_creds['accounts'] = accounts_dict
            self.creds_file.save()

        return self.monzo_creds['accounts']

    # -------- transactions --------
    def _new_monzo_client(self) -> Monzo:
        """Always construct the legacy client from the CURRENT access token."""
        return Monzo(self.monzo_auth.access_token)

    def download_accounts_transaction_history(self, account_id, batch_size=100, since="2019-01-01T00:00:00Z"):
        """
        Legacy client + capped windows:
        - Build Monzo with the current access token (no Authentication object).
        - Send RFC3339 **second-precision** timestamps.
        - Keep each window < ~1 year (safe cap = 364 days + 23:59:59).
        - If 'time range too large' (400), shrink window (→ 180d, then 90d) and retry.
        - If auth fails (Forbidden/verification), refresh once; if still blocked, fall back to last ~90 days.
        - Catch IndexError from legacy client when page is empty.
        """

        def _safe_before_from_since(since_rfc3339: str, days: int) -> str:
            """Compute a safe 'before' as since + days (<= 364) with second precision, clamped to now."""
            since_dt = pd.to_datetime(since_rfc3339, utc=True, errors="coerce")
            now_dt = pd.Timestamp.now(tz="UTC")  # <-- instead of .utcnow().tz_localize("UTC")
            window_end = since_dt + pd.Timedelta(days=days, hours=23, minutes=59, seconds=59)
            end_dt = min(window_end, now_dt)
            return end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        monzo = self._new_monzo_client()

        # Normalize the incoming 'since' to second precision Z
        since = _fmt_rfc3339_seconds(since)

        # Start with a conservative window (<= ~1y)
        window_days = 364  # safe cap; Monzo returns 400 for '> ~1 year'
        before = _safe_before_from_since(since, window_days)

        dfs = []
        while True:
            logging.info(f"Downloading history from {account_id} before before='{before}' and since='{since}'...")
            try:
                transactions = monzo.get_transactions(
                    account_id,
                    before=before,
                    since=since,
                    limit=min(int(batch_size), 100)  # default 30; max 100
                )
            except IndexError:
                logging.info(
                    "Reached the limit, no more transactions to download: empty page (IndexError from client).")
                break
            except BadRequestError as e:
                msg = (str(e) or "").lower()
                # If the API says the time range is too large, shrink and retry this same page.
                if "time range" in msg or "invalid_time_range" in msg:
                    if window_days > 180:
                        window_days = 180
                    elif window_days > 90:
                        window_days = 90
                    else:
                        # Already at 90d and still too large? Give up this page.
                        logging.info("Time-range still too large at 90 days; skipping this window.")
                        break
                    before = _safe_before_from_since(since, window_days)
                    logging.info(f"Shrank window due to time-range error. Retrying with ~{window_days} days.")
                    continue
                # Other 400s: re-raise so you can see them
                raise
            except ForbiddenError as e:
                # Verification required / SCA window etc.
                logging.info(f"Monzo error ({type(e).__name__}): {e}. Trying a token refresh and retry...")
                try:
                    self.refresh_token()
                    monzo = self._new_monzo_client()
                    transactions = monzo.get_transactions(
                        account_id,
                        before=before,
                        since=since,
                        limit=min(int(batch_size), 100)
                    )
                except IndexError:
                    logging.info("Reached the limit after refresh: empty page (IndexError).")
                    break
                except (BadRequestError, ForbiddenError):
                    # Fall back to ~90 days window
                    logging.info("Retry after refresh failed. Falling back to last ~90 days window.")
                    since_dt = datetime.datetime.utcnow() - datetime.timedelta(days=89, minutes=59, seconds=59)
                    since = _fmt_rfc3339_seconds(since_dt)
                    window_days = 90
                    before = _safe_before_from_since(since, window_days)
                    continue

            # ---- success path ----
            items = (transactions or {}).get('transactions', [])
            if not items:
                logging.info("Reached the limit, no more transactions to download: empty page []")
                break

            df = pd.json_normalize(items)
            if df.empty:
                logging.info("Reached the limit, no more transactions to download: empty DataFrame.")
                break

            # Ensure date helpers
            if 'created' in df.columns:
                df['created'] = df['created'].map(_fmt_rfc3339_seconds)
                df['created_date'] = pd.to_datetime(df['created'].str[:10], errors='coerce')
            else:
                df['created_date'] = pd.NaT

            dfs.append(df)

            # Advance the window forward:
            # Move 'since' to the oldest row in this page (+1s to avoid overlap),
            # then set next 'before' using the current window_days.
            if 'created' not in df.columns or df['created'].isna().all():
                logging.info("No 'created' in page; cannot page further.")
                break

            oldest_created = str(df['created'].iloc[-1])  # API returns newest first; last row is oldest
            new_since = _plus_seconds(oldest_created, 1)
            if new_since == since:
                logging.info(f"Reached the limit, no more transactions to download: {new_since == since=} {df.shape=}")
                break

            since = new_since
            before = _safe_before_from_since(since, window_days)

            logging.info(
                f"{len(dfs)} batch(es) downloaded with dims {df.shape}, "
                f"date range {df['created_date'].min()} -> {df['created_date'].max()}"
            )

        if not dfs:
            return pd.DataFrame()

        all_transactions = pd.concat(dfs, ignore_index=True)
        if 'id' in all_transactions.columns:
            all_transactions.drop_duplicates(subset='id', inplace=True)
        if 'created' in all_transactions.columns:
            all_transactions.sort_values(by=['created'], inplace=True, ignore_index=True)

        logging.info(
            f"Downloaded {len(all_transactions)} transactions with dims {all_transactions.shape}\n"
            f"date range {all_transactions.get('created_date').min()} -> {all_transactions.get('created_date').max()}\n"
            f"{all_transactions.get('created_date').dt.date.nunique() if 'created_date' in all_transactions else 'n/a'} unique days\n"
            f"{all_transactions.duplicated(subset='id', keep=False).sum() if 'id' in all_transactions else 0} duplicated transactions\n"
        )
        return all_transactions

    def download_full_history(self, batch_size=100, since="2019-01-01T00:00:00Z"):
        dfs = []
        for account in self.get_accounts():
            account_id = account['account_id']
            dfs.append(self.download_accounts_transaction_history(
                account_id, batch_size=batch_size, since=since
            ))
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


if __name__ == "__main__":
    from mecon.etl.dataset import Dataset

    d = Dataset(r"C:\Users\dimitris\PycharmProjects\datasets\20250812")
    mc = MonzoClient(d.creds, force_new_token=True)
    auth_code_url = input(f"{mc.get_authentication_url()} -> ")
    mc.set_authentication_code_from_url(auth_code_url)
