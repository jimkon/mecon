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
class TrueLayerCredentialsError(Exception):
    pass


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

        if "truelayer" not in creds_file:
            raise TrueLayerCredentialsError("No TrueLayer credentials found")

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
            bank: str,
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

        # cache PKCE + anti‑csrf bits per source/provider
        sources = self._creds.setdefault("sources", {})
        source_creds = sources.setdefault(bank, {})
        transient_store = source_creds.setdefault("_transient_store", {})
        key = provider_id or "__default__"
        transient_store[key] = {
            "code_verifier": code_verifier,
            "state": state,
            "nonce": nonce,
            "created_at": _utc_now().isoformat(),
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

    def _get_transient_store(self, bank: str) -> Dict[str, Dict[str, Any]]:
        sources = self._creds.setdefault("sources", {})
        if bank not in sources:
            raise AuthFlowError(f"No source credentials found for '{bank}'")
        return sources[bank].setdefault("_transient_store", {})

    def exchange_code(
            self,
            bank: str,
            code: str,
            returned_state: str,
            provider_id: Optional[str] = None,
    ) -> None:
        """
        Swap `code` for an access+refresh token and persist to creds.
        """
        transient_store = self._get_transient_store(bank)
        key = provider_id or "__default__"
        t = transient_store.get(key)
        if not t:
            raise AuthFlowError("No PKCE verifier/state found for this session")
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

        resp = httpx.post(
            f"{self.AUTH_BASE}/connect/token",
            data=data,
            headers={"Accept": "application/json"},
        )
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
        source_creds = self._creds.setdefault("sources", {}).setdefault(bank, {})
        source_creds["token"] = token.to_json()
        source_creds["last_authenticated_at"] = now.isoformat()
        source_creds["last_token_refresh_at"] = now.isoformat()
        # clean transient fields
        transient_store.pop(key, None)
        if not transient_store:
            source_creds.pop("_transient_store", None)
        self._save()

    def exchange_code_from_code_url(
            self,
            url: str,
            bank: str,
            provider_id: Optional[str] = None,
    ):
        """Extract the OAuth code+state parameters from a redirect URL."""

        parsed = up.urlparse(url)
        expected_redirect = up.urlparse(self._creds["redirect_uri"])
        if (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
        ) != (
                expected_redirect.scheme,
                expected_redirect.netloc,
                expected_redirect.path,
        ):
            raise AuthFlowError(
                "Redirect URI does not match the configured redirect_uri. "
                "Did you use the TrueLayer Console redirect page?"
            )
        query_params = up.parse_qs(parsed.query)

        code_values = query_params.get("code")
        state_values = query_params.get("state")

        if not code_values or not state_values:
            raise AuthFlowError("missing code or state in OAuth redirect")

        code = code_values[0]
        state = state_values[0]
        # … user completes flow …
        self.exchange_code(
            bank,
            code=code,
            returned_state=state,
            provider_id=provider_id,
        )

    # ----------------------------------------------------------------- refresh
    def _ensure_token(self, bank: str) -> Token:
        """Return a fresh Token, refreshing if required."""
        src = self._creds["sources"][bank]
        if "token" not in src:
            raise AuthFlowError(f"no token stored for bank '{bank}'")

        token = Token.from_json(src["token"])

        if not token.is_expired:
            return token

        return self.refresh_token(bank)

    def refresh_token(self, bank: str) -> Token:
        """Force-refresh a bank token and persist metadata."""

        src = self._creds["sources"].get(bank, {})
        if "token" not in src:
            raise AuthFlowError(f"no token stored for bank '{bank}'")

        token = Token.from_json(src["token"])

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
        src["last_token_refresh_at"] = now.isoformat()
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
