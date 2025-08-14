# mecon/etl/monzo_api_client.py

import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from monzo.authentication import Authentication
from monzo.endpoints.account import Account
from monzo.endpoints.transaction import Transaction
from monzo.exceptions import MonzoError  # Monzo-API's catch-all

from mecon.settings import DictFile


class MonzoCredentialsError(Exception):
    pass


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _to_iso_z(value: Any) -> Optional[str]:
    """Return an RFC3339 string with 'Z' for UTC or None."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, dt.date):
        dttm = dt.datetime.combine(value, dt.time.min, tzinfo=dt.timezone.utc)
        return dttm.isoformat().replace("+00:00", "Z")
    # assume already string-like
    s = str(value)
    if s.endswith("+00:00"):
        s = s[:-6] + "Z"
    return s


def _ensure_utc_datetime(value: Any) -> Optional[dt.datetime]:
    """
    Convert string/date/datetime to a tz-aware UTC datetime for Monzo-API.
    Returns None iff value is None.
    """
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time.min, tzinfo=dt.timezone.utc)
    # string-like: be robust (supports 'Z', offsets, date-only, etc.)
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Cannot parse datetime from: {value!r}")
    return ts.to_pydatetime()


def _objects_to_dicts(items: Iterable[Any]) -> List[Dict[str, Any]]:
    """
    Convert Monzo-API model objects (or dicts) into plain dicts safe for json_normalize.
    Normalizes common datetime-ish fields to ISO Z strings.
    """
    out: List[Dict[str, Any]] = []
    for it in items:
        if isinstance(it, dict):
            d = dict(it)
        else:
            d = {k: v for k, v in getattr(it, "__dict__", {}).items() if not k.startswith("_")}
        for k in ("created", "updated", "settled", "last_updated"):
            if k in d:
                d[k] = _to_iso_z(d[k])
        out.append(d)
    return out


# ---------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------
class MonzoClient:
    """
    Monzo client using ONLY the Monzo-API package.

    Expects a DictFile with a "monzo-api" section:
      {
        "monzo-api": {
          "client_id": "...",
          "client_secret": "...",
          "redirect_url": "...",
          "token": {
            "access_token": "...",
            "expiry": 1710000000,
            "refresh_token": "..."
          },
          "created_at": 1710000000.0,
          "accounts": [
            {"account_id": "acc_...", "created_at": "2020-01-01T00:00:00Z"}
          ]
        }
      }
    """

    def __init__(self, creds_file: "DictFile"):
        self.creds_file = creds_file
        if "monzo-api" not in creds_file:
            raise MonzoCredentialsError("No credentials for 'monzo-api' found in the creds file")

        self.monzo_creds: Dict[str, Any] = self.creds_file["monzo-api"]
        token = self.monzo_creds.get("token") or {}

        self.monzo_auth = Authentication(
            client_id=self.monzo_creds["client_id"],
            client_secret=self.monzo_creds["client_secret"],
            redirect_url=self.monzo_creds["redirect_url"],
            access_token=token.get("access_token", ""),
            access_token_expiry=token.get("expiry", 0),
            refresh_token=token.get("refresh_token", ""),
        )

        # record creation time if missing
        self.monzo_creds.setdefault("created_at", dt.datetime.now().timestamp())

    # ------------------------ token helpers -------------------------
    def has_token(self) -> bool:
        return bool(getattr(self.monzo_auth, "access_token", ""))

    def expires_at(self) -> Optional[str]:
        exp = getattr(self.monzo_auth, "access_token_expiry", None)
        try:
            exp = float(exp)
        except (TypeError, ValueError):
            return None
        if not exp:
            return None
        return dt.datetime.fromtimestamp(exp, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

    def created_at_secs(self) -> Optional[float]:
        try:
            return float(self.monzo_creds.get("created_at"))
        except (TypeError, ValueError):
            return None

    def minutes_passed_from_token_creation(self) -> Optional[float]:
        created = self.created_at_secs()
        if created is None:
            return None
        return (dt.datetime.now().timestamp() - created) / 60.0

    def authenticated(self) -> bool:
        return self.has_token()

    def refresh_token(self) -> None:
        logging.info("Refreshing Monzo access token...")
        self.monzo_auth.refresh_access()
        self._persist_token()
        logging.info("Token refresh complete. New expiry: %s", self.expires_at())

    def _persist_token(self) -> None:
        self.monzo_creds["token"] = {
            "access_token": self.monzo_auth.access_token,
            "expiry": self.monzo_auth.access_token_expiry,
            "expires_at": self.expires_at(),
            "refresh_token": self.monzo_auth.refresh_token,
        }
        self.monzo_creds["created_at"] = dt.datetime.now().timestamp()
        self.creds_file.save()

    # -------------------------- OAuth ------------------------------
    def get_authentication_url(self) -> str:
        return self.monzo_auth.authentication_url

    def get_authentication_url_and_state(self):
        url_and_state = self.get_authentication_url()
        url, state = url_and_state.split("&state=")
        return url, state

    def set_authentication_code_from_url(self, response_url: str) -> None:
        code_and_state = response_url.split("code=")[1]
        code, state = code_and_state.split("&state=")
        logging.info("Authenticating with Monzo...")
        self.monzo_auth.authenticate(authorization_token=code, state_token=state)
        self._persist_token()
        logging.info("Authentication complete. Token expires at: %s", self.expires_at())

    # ------------------------- accounts ----------------------------
    def get_accounts(self) -> List[Dict[str, Any]]:
        if "accounts" not in self.monzo_creds or not self.monzo_creds["accounts"]:
            logging.info("Fetching Monzo accounts (caching results)...")
            accounts = Account.fetch(self.monzo_auth)
            accounts_out = []
            for a in accounts:
                created = _to_iso_z(getattr(a, "created", None))
                accounts_out.append({
                    "account_id": getattr(a, "account_id", getattr(a, "id", None)),
                    "created_at": created,
                })
            self.monzo_creds["accounts"] = accounts_out
            self.creds_file.save()

        return list(self.monzo_creds["accounts"])

    # ----------------------- transactions --------------------------
    def download_accounts_transaction_history(
        self,
        account_id: str,
        *,
        batch_size: int = 100,                      # kept for interface parity (unused by Monzo-API)
        since: str = "2019-01-01T00:00:00Z",
    ) -> pd.DataFrame:
        """
        Download full transaction history for a single account via Monzo-API.

        Notes:
        - After ~5 minutes post-auth, Monzo restricts sync to ~90 days unless you re-auth.
        - Monzo-API wrapper does not expose a `limit`; we page by advancing `since`.
        """
        dfs: List[pd.DataFrame] = []
        current_since_dt = _ensure_utc_datetime(since)
        before_dt: Optional[dt.datetime] = None  # not used; advancing since is enough

        while True:
            logging.info(
                "Downloading transactions for account_id=%s (since=%s, before=%s)...",
                account_id,
                _to_iso_z(current_since_dt),
                _to_iso_z(before_dt),
            )
            try:
                txs = Transaction.fetch(
                    self.monzo_auth,
                    account_id=account_id,
                    since=current_since_dt,
                    before=before_dt,
                )
                rows = _objects_to_dicts(txs)
                if not rows:
                    logging.info("No more transactions returned (page empty).")
                    break

                df = pd.json_normalize(rows)

                # Normalize temporal fields
                if "created" in df.columns:
                    df["created"] = df["created"].map(_to_iso_z)
                    df["created_date"] = pd.to_datetime(df["created"].str[:10])
                else:
                    df["created_date"] = pd.NaT

                # Sort and append
                if "created" in df.columns:
                    df.sort_values(by=["created"], inplace=True, ignore_index=True)
                dfs.append(df)

                # Advance the cursor — add +1s to avoid inclusive 'since' returning the same last row
                if "created" in df.columns:
                    last_created_str = df["created"].iloc[-1]
                    last_created_dt = _ensure_utc_datetime(last_created_str)
                    new_since_dt = last_created_dt + dt.timedelta(seconds=1)
                else:
                    logging.info("No 'created' field to page with; stopping after this page.")
                    break

                # Safety valve: if no progress, stop
                if new_since_dt <= current_since_dt:
                    logging.info("Pagination stalled (no progress). Stopping.")
                    break

                current_since_dt = new_since_dt

                logging.info(
                    "Fetched page: %s rows, date range %s -> %s",
                    df.shape[0],
                    df["created_date"].min(),
                    df["created_date"].max(),
                )

            except MonzoError as e:
                # Commonly triggered by the 90-day restriction.
                logging.warning(
                    "Monzo API error while fetching transactions: %s. "
                    "Falling back to the last ~90 days window (once).",
                    e,
                )
                current_since_dt = (dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
                                    - dt.timedelta(days=89, hours=23, minutes=59)).replace(microsecond=0)
                before_dt = None

                try:
                    txs = Transaction.fetch(
                        self.monzo_auth,
                        account_id=account_id,
                        since=current_since_dt,
                        before=before_dt,
                    )
                    rows = _objects_to_dicts(txs)
                    if not rows:
                        logging.info("No transactions in the ~90 day fallback window.")
                        break

                    df = pd.json_normalize(rows)
                    if "created" in df.columns:
                        df["created"] = df["created"].map(_to_iso_z)
                        df["created_date"] = pd.to_datetime(df["created"].str[:10])
                        df.sort_values(by=["created"], inplace=True, ignore_index=True)
                    else:
                        df["created_date"] = pd.NaT
                    dfs.append(df)

                    logging.info(
                        "Fetched fallback page: %s rows, date range %s -> %s",
                        df.shape[0], df["created_date"].min(), df["created_date"].max()
                    )
                    break
                except MonzoError:
                    logging.exception("Fallback fetch also failed; giving up.")
                    raise

        if not dfs:
            return pd.DataFrame()

        all_transactions = pd.concat(dfs, ignore_index=True)

        if "id" in all_transactions.columns:
            before_drop = len(all_transactions)
            all_transactions.drop_duplicates(subset="id", inplace=True)
            logging.info("Dropped %s duplicate transactions by 'id'.", before_drop - len(all_transactions))

        if "created" in all_transactions.columns:
            all_transactions.sort_values(by=["created"], inplace=True, ignore_index=True)

        logging.info(
            "Downloaded %s transactions (shape=%s). "
            "Dates: %s -> %s. Unique days: %s. Duplicates (post-drop): %s.",
            len(all_transactions),
            all_transactions.shape,
            (all_transactions.get("created_date").max() if "created_date" in all_transactions else None),
            (all_transactions.get("created_date").min() if "created_date" in all_transactions else None),
            (all_transactions["created_date"].dt.date.nunique() if "created_date" in all_transactions else None),
            (all_transactions.duplicated(subset="id", keep=False).sum() if "id" in all_transactions else 0),
        )

        return all_transactions

    def download_full_history(
        self,
        *,
        batch_size: int = 100,                      # kept for interface parity (unused here)
        since: str = "2019-01-01T00:00:00Z",
    ) -> pd.DataFrame:
        """Download and merge transactions across all known accounts."""
        dfs: List[pd.DataFrame] = []
        for account in self.get_accounts():
            account_id = account["account_id"]
            df = self.download_accounts_transaction_history(
                account_id,
                batch_size=batch_size,
                since=since,
            )
            if not df.empty:
                df["account_id"] = account_id
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)
