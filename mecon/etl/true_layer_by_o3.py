from __future__ import annotations

import datetime as dt
import logging
import secrets
import threading
import urllib.parse as up
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import httpx
import base64, hashlib


def _create_s256_code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


# ---------- exceptions --------------------------------------------------------

class TrueLayerError(Exception):
    """Base‑class for any TrueLayer integration error."""


class AuthFlowError(TrueLayerError):
    """Something went wrong while exchanging or refreshing tokens."""


class ApiError(TrueLayerError):
    """Non‑2xx from the Data API."""


# ---------- token model -------------------------------------------------------

@dataclass
class Token:
    access_token: str
    refresh_token: str
    expires_at: dt.datetime  #  UTC
    fetched_at: dt.datetime
    refreshed_at: Optional[dt.datetime] = None

    @classmethod
    def from_json(cls, raw: Dict[str, Any]) -> "Token":
        return cls(
            access_token=raw["access_token"],
            refresh_token=raw["refresh_token"],
            expires_at=_parse_ts(raw["expires_at"]),
            fetched_at=_parse_ts(raw["fetched_at"]),
            refreshed_at=_parse_ts(raw.get("refreshed_at")),
        )

    def to_json(self) -> Dict[str, Any]:
        out = asdict(self)
        # dataclass ↔ json: iso‑format the datetimes
        for k in ("expires_at", "fetched_at", "refreshed_at"):
            if out[k]:
                out[k] = out[k].isoformat()
        return out

    @property
    def is_expired(self) -> bool:
        # refresh proactively 60s before expiry
        return (
                dt.datetime.now(dt.timezone.utc)  # ← aware
                >= self.expires_at - dt.timedelta(seconds=60)
        )


# ---------- helper functions --------------------------------------------------

def _parse_ts(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    ts = ts.replace("Z", "+00:00")  # handle trailing 'Z'
    return dt.datetime.fromisoformat(ts).astimezone(dt.timezone.utc)


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


# ---------- main client -------------------------------------------------------

class TrueLayerClient:
    AUTH_BASE = "https://auth.truelayer.com"
    DATA_BASE = "https://api.truelayer.com/data/v1"

    _LOCK = threading.Lock()

    def __init__(self, creds_file: "DictFile", sandbox: bool = False) -> None:
        self._creds_full = creds_file
        self._creds = self._creds_full["truelayer"]
        self._client = httpx.Client(
            timeout=10,
            follow_redirects=True,
            base_url=self.DATA_BASE if not sandbox else self.DATA_BASE.replace(
                "api.", "api.truelayer-sandbox."
            ),
        )

    # --------------------------------------------------------------------- auth
    def build_auth_link(
            self,
            provider_id: Optional[str] = None,
            scopes: str = "info accounts balance transactions offline_access",  # keep minimal
    ) -> str:
        """
        Return an auth URL *and* stash verifier/state so we can validate later.
        """
        code_verifier = secrets.token_urlsafe(64)
        # code_challenge = (
        #     httpx._client._utils.hashes
        #     .create_s256_code_challenge(code_verifier)  # tiny helper from httpx
        # )
        code_challenge = _create_s256_code_challenge(code_verifier)

        state = secrets.token_urlsafe(16)
        nonce = secrets.token_urlsafe(16)

        # cache PKCE + anti‑csrf bits
        self._creds["_transient"] = {
            "code_verifier": code_verifier,
            "state": state,
            "nonce": nonce,
        }
        self._save()

        params = {
            "response_type": "code",
            "client_id": self._creds["client_id"],
            "redirect_uri": self._creds["redirect_uri"],
            "scope": scopes,
            "state": state,
            "nonce": nonce,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        if provider_id:
            # provider pre‑selection (newer param name) :contentReference[oaicite:0]{index=0}
            params["provider_id"] = provider_id

        return f"{self.AUTH_BASE}/?{up.urlencode(params)}"

    def exchange_code(self, bank: str, code: str, returned_state: str) -> None:
        """
        Swap `code` for an access+refresh token and persist to creds.
        """
        t = self._creds["_transient"]
        if returned_state != t["state"]:
            raise AuthFlowError("state mismatch in OAuth redirect")

        data = {
            "grant_type": "authorization_code",
            "client_id": self._creds["client_id"],
            "client_secret": self._creds["client_secret"],
            "code_verifier": t["code_verifier"],
            "redirect_uri": self._creds["redirect_uri"],
            "code": code,
        }

        resp = httpx.post(f"{self.AUTH_BASE}/connect/token", data=data)
        if resp.status_code != 200:
            raise AuthFlowError(
                f"token exchange failed: {resp.status_code} → {resp.text}"
            )

        now = _utc_now()
        raw = resp.json()
        token = Token(
            access_token=raw["access_token"],
            refresh_token=raw["refresh_token"],
            fetched_at=now,
            refreshed_at=None,
            expires_at=now + dt.timedelta(seconds=raw["expires_in"]),
        )
        self._creds.setdefault("sources", {}).setdefault(bank, {})["token"] = (
            token.to_json()
        )
        # clean transient fields
        self._creds.pop("_transient", None)
        self._save()

    def exchange_code_from_code_url(self, url):
        auth_link_resp_split = url.split('?')[1].split('&')
        code = auth_link_resp_split[0].split('=')[1]
        state = auth_link_resp_split[2].split('=')[1]
        # … user completes flow …
        self.exchange_code(bank, code=code, returned_state=state)

    # ----------------------------------------------------------------- refresh
    def _ensure_token(self, bank: str) -> Token:
        """Return a fresh Token, refreshing if required."""
        src = self._creds["sources"][bank]
        if "token" not in src:
            raise AuthFlowError(f"no token stored for bank '{bank}'")

        token = Token.from_json(src["token"])

        if not token.is_expired:
            return token

        # refresh
        data = {
            "grant_type": "refresh_token",
            "client_id": self._creds["client_id"],
            "client_secret": self._creds["client_secret"],
            "refresh_token": token.refresh_token,
        }
        resp = httpx.post(f"{self.AUTH_BASE}/connect/token", data=data)
        if resp.status_code != 200:
            raise AuthFlowError(
                f"refresh failed: {resp.status_code} → {resp.text}"
            )

        raw = resp.json()
        now = _utc_now()
        token = Token(
            access_token=raw["access_token"],
            refresh_token=raw.get("refresh_token", token.refresh_token),
            fetched_at=token.fetched_at,
            refreshed_at=now,
            expires_at=now + dt.timedelta(seconds=raw["expires_in"]),
        )
        # persist
        src["token"] = token.to_json()
        self._save()
        return token

    # -------------------------------------------------------------- API calls
    def _request(
            self,
            bank: str,
            method: str,
            url: str,
            **kwargs,
    ) -> httpx.Response:
        token = self._ensure_token(bank)
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token.access_token}"
        resp = self._client.request(method, url, headers=headers, **kwargs)
        if resp.is_error:
            raise ApiError(f"{method} {url} → {resp.status_code}: {resp.text}")
        return resp

    def get_accounts(self, bank: str) -> List[Dict[str, Any]]:
        resp = self._request(bank, "GET", "/accounts")
        data = resp.json()["results"]
        # with self._LOCK:
        self._creds["sources"][bank]["accounts"] = data
        self._save()
        return data

    def get_transactions(
            self,
            bank: str,
            account_id: str,
            from_date: Optional[dt.date] = None,
            to_date: Optional[dt.date] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, str] = {}
        if from_date:
            params["from"] = from_date.isoformat()
        if to_date:
            params["to"] = to_date.isoformat()
        resp = self._request(
            bank,
            "GET",
            f"/accounts/{account_id}/transactions",
            params=params,
            headers={"accept": "application/json; charset=UTF-8"},
        )
        txns = resp.json()["results"]
        return txns

    # ----------------------------------- 90‑day reconfirmation (connections)
    def extend_connection(self, connection_id: str) -> None:
        """Trigger a 90‑day reconfirmation (replacement for deprecated /reauthuri)."""
        url = f"{self.AUTH_BASE}/v3/connections/{connection_id}/extend"
        resp = httpx.post(url, headers={"Client-Id": self._creds["client_id"]})
        if resp.status_code not in (200, 201):
            raise ApiError(f"extend_connection failed: {resp.status_code} {resp.text}")

    # -------------------------------------------------------------- helpers
    def _save(self) -> None:
        with self._LOCK:
            self._creds_full.save()
