from __future__ import annotations
import datetime as dt
import logging
import threading
import time, random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from email.utils import parsedate_to_datetime
from datetime import timezone
from io import BytesIO

import httpx
import pandas as pd
import zipfile

from mecon.settings import DictFile


# ───────── exceptions ─────────
class Trading212Error(Exception): ...
class ApiError(Trading212Error): ...


def _to_rfc3339_z(d: dt.datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sleep(n: float) -> None:
    logging.info(f"Sleeping {n} seconds...")
    time.sleep(n)


@dataclass
class T212Creds:
    api_key: str
    mode: str = "live"  # "live" | "demo"

    @property
    def base_url(self) -> str:
        host = "live.trading212.com" if self.mode == "live" else "demo.trading212.com"
        return f"https://{host}/api/v0"


class Trading212Client:
    """
    Ultra-stable Trading212 client (read-only). Designed to be "set & forget".
    """

    # Fixed, polite spacing between calls to each /history/* endpoint family
    _HISTORY_GAP_SEC = 12     # T212 limit ≈ 6/min → 12s keeps us safe
    _RETRY_MAX = 3

    _LOCK = threading.RLock()

    def __init__(self, creds_file: "DictFile") -> None:
        self._creds_file = creds_file
        self._creds = creds_file["trading212"]
        self.creds = T212Creds(**self._creds)

        self._client = httpx.Client(
            base_url=self.creds.base_url,
            timeout=20,
            headers={"Authorization": self.creds.api_key},  # raw key for T212 API
        )
        self._dl = httpx.Client(timeout=60, headers={}, follow_redirects=True)  # for S3 links
        # last time a /history/* request was made (for polite throttling)
        self._last_history_call = 0.0

    # ───────── low-level request with boring-but-safe behavior ─────────
    def _throttle_history(self):
        gap = self._HISTORY_GAP_SEC
        now = time.monotonic()
        wait = self._last_history_call + gap - now
        if wait > 0:
            logging.info(f"Sleeping for {wait} seconds before it call the API again")
            _sleep(wait)

    def _mark_history_call(self):
        self._last_history_call = time.monotonic()

    def _retry_after_seconds(self, resp, fallback: float) -> float:
        ra = resp.headers.get("Retry-After")
        if ra:
            try:
                return float(int(ra))
            except ValueError:
                try:
                    return max(0.0, (parsedate_to_datetime(ra) - dt.datetime.now(timezone.utc)).total_seconds())
                except Exception:
                    pass
        return fallback + random.uniform(0, 0.5)

    def _request(self, method: str, path_or_url: str, *, history=False, **kw) -> httpx.Response:
        # If nextPagePath is absolute, pass it straight through
        url = path_or_url

        for attempt in range(self._RETRY_MAX + 1):
            if history:
                self._throttle_history()

            resp = self._client.request(method, url, **kw)

            if not resp.is_error:
                if history:
                    self._mark_history_call()
                return resp

            # Retry only on throttling / transient server issues
            if resp.status_code in (429, 408) or 500 <= resp.status_code < 600:
                if attempt == self._RETRY_MAX:
                    break
                backoff = self._retry_after_seconds(resp, self._HISTORY_GAP_SEC if history else 5.0)
                logging.info(f"{resp.status_code} on {url} – retrying in {backoff:.1f}s (attempt {attempt+1}/{self._RETRY_MAX})")
                _sleep(backoff)
                continue

            # Non-retryable
            raise ApiError(f"{method} {url} → {resp.status_code}: {resp.text}")

        raise ApiError(f"{method} {url} → {resp.status_code}: {resp.text}")

    # ───────── simple endpoints ─────────
    def get_account_info(self) -> Dict[str, Any]:
        return self._request("GET", "/equity/account/info").json()

    def get_cash(self) -> Dict[str, Any]:
        return self._request("GET", "/equity/account/cash").json()

    def get_positions(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/equity/portfolio").json()

    # ───────── generic paginator for history endpoints ─────────
    def _paged_get(
        self,
        path: str,
        *,
        time_from: Optional[dt.datetime] = None,
        limit: int = 50,
        is_transactions: bool = False,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"limit": min(limit, 50)}

        # /history/transactions quirk: needs both time AND cursor=0 to start
        if is_transactions and time_from:
            params["time"] = _to_rfc3339_z(time_from)
            params["cursor"] = "0"

        items: List[Dict[str, Any]] = []
        next_path: Optional[str] = path

        while next_path:
            resp = self._request("GET", next_path, params=params if next_path == path else None, history=True)
            payload = resp.json()
            items.extend(payload.get("items", []))

            next_cursor = payload.get("nextPagePath")
            if not next_cursor:
                break

            # nextPagePath can be absolute or relative. For simplicity, always pass it as-is.
            if next_cursor.startswith("http"):
                next_path = next_cursor
            else:
                # Avoid double /api/v0
                if next_cursor.startswith("/api/v0/"):
                    next_cursor = next_cursor[len("/api/v0/"):]
                elif next_cursor.startswith("api/v0/"):
                    next_cursor = next_cursor[len("api/v0/"):]
                next_path = next_cursor.lstrip("/")

            # after first page, params are embedded in nextPagePath
            params = None

        logging.info(f"{path} fetched {len(items)} items")
        return items

    # ───────── public history helpers ─────────
    def get_transactions(self, time_from: Optional[dt.datetime] = None) -> List[Dict[str, Any]]:
        # Cash movements (often empty) – not trades.
        return self._paged_get("/history/transactions", time_from=time_from, is_transactions=True)

    def get_dividends(self, time_from: Optional[dt.datetime] = None) -> List[Dict[str, Any]]:
        # Dividends history (paginates)
        # Dividends endpoint doesn’t take `time` start; server handles the window via cursor.
        return self._paged_get("/history/dividends", time_from=None)

    def get_orders(self, time_from: Optional[dt.datetime] = None) -> List[Dict[str, Any]]:
        # Orders (buys/sells). No time param; just page until done.
        return self._paged_get("/equity/history/orders", time_from=None)

    # ───────── CSV export helpers (chunked to avoid 500s) ─────────
    def request_csv_export(self, time_from: dt.datetime, time_to: dt.datetime, include: Dict[str, bool] | None = None) -> int:
        body = {
            "timeFrom": _to_rfc3339_z(time_from),
            "timeTo": _to_rfc3339_z(time_to),
            "dataIncluded": include or {
                "includeTransactions": True,
                "includeOrders": True,
                "includeDividends": True,
                "includeInterest": True,
            },
        }
        resp = self._request("POST", "/history/exports", json=body)
        return resp.json()["reportId"]

    def request_csv_export_full(self, since: dt.datetime) -> List[int]:
        """Fire per-year exports to avoid server 500s; returns list of reportIds."""
        logging.info(f"Requesting a new full export...")
        end = dt.datetime.now(timezone.utc)
        cur = since.astimezone(timezone.utc)
        rids: List[int] = []
        while cur < end:
            year_end = min(dt.datetime(cur.year, 12, 31, 23, 59, 59, tzinfo=timezone.utc), end)
            rids.append(self.request_csv_export(cur, year_end))
            cur = dt.datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)
            logging.info(f"Sleeping for 30 seconds before it call the API again for {cur}")
            _sleep(30)
        return rids

    def get_exports(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/history/exports", history=True).json()

    # ───────── convenience: fetch everything safely ─────────
    def fetch_all_history(self, since: Optional[dt.datetime] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        One-shot “give me everything” that won’t blow rate limits:
          - orders (trades)
          - dividends
          - cash transactions (often empty)
        """
        if since and since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        # Transactions: only meaningful if you had cash moves; keep it
        tx = self.get_transactions(time_from=since) if since else self.get_transactions()

        # Orders & dividends don’t accept a start time cleanly; just paginate.
        od = self.get_orders()
        dv = self.get_dividends()
        return {"transactions": tx, "orders": od, "dividends": dv}

    def fetch_history_dataframe(
            self,
            since: dt.datetime,
            request_ids_to_skip: list[str] = None,
            include: Dict[str, bool] | None = None,
            poll_interval_sec: int = 65,
            post_gap_sec: int = 31,
            timeout_sec: int = 60 * 30,
            *,
            force_request: bool = False,
    ):
        """
        Export → download → merge → DataFrame.

        New behavior:
          - If an existing Completed export already covers *today* (UTC) for the
            current-year chunk, we reuse it and DON'T create a new one,
            unless force_request=True.
        """

        logging.info(f"Fetching full history of transactions from the Trading212 API since {since=}...")

        request_ids_to_skip = request_ids_to_skip if request_ids_to_skip is not None else []

        skip_ids: set[str] = set()
        if not force_request:
            skip_ids = {str(rid) for rid in request_ids_to_skip if rid is not None}
            if skip_ids:
                preview = ", ".join(sorted(skip_ids)[:5])
                if len(skip_ids) > 5:
                    preview += ", …"
                logging.info(
                    "Skipping re-download for %d previously cached export(s): %s",
                    len(skip_ids),
                    preview,
                )
        else:
            if request_ids_to_skip:
                logging.info(
                    "force_request=True – ignoring %d cached export id(s).",
                    len(request_ids_to_skip),
                )

        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        now = dt.datetime.now(timezone.utc)
        today_utc = now.date()

        include = include or {
            "includeTransactions": True,
            "includeOrders": True,
            "includeDividends": True,
            "includeInterest": True,
        }

        # -------- build calendar-year chunks [from, to] up to 'now'
        chunks: list[tuple[dt.datetime, dt.datetime]] = []
        cur = dt.datetime(max(since.year, since.year), 1, 1, tzinfo=timezone.utc)
        if since > cur:
            cur = since
        while cur < now:
            year_end = min(
                dt.datetime(cur.year, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
                now,
            )
            chunks.append((cur, year_end))
            cur = dt.datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)

        # -------- index existing exports
        DONE_STATUSES = {"Finished", "Completed", "Complete", "Succeeded"}

        def _norm_included(d: dict) -> tuple:
            keys = ("includeOrders", "includeTransactions", "includeDividends", "includeInterest")
            return tuple(bool(d.get(k, False)) for k in keys)

        want_di = _norm_included(include)
        existing = self.get_exports()  # call once
        completed_wanted: list[dict] = []
        for it in existing:
            try:
                if it.get("status") not in DONE_STATUSES:
                    continue
                if _norm_included(it.get("dataIncluded", {})) != want_di:
                    continue
                if not it.get("downloadLink"):
                    continue
                it["_from"] = dt.datetime.fromisoformat(it["timeFrom"].replace("Z", "+00:00")).astimezone(timezone.utc)
                it["_to"] = dt.datetime.fromisoformat(it["timeTo"].replace("Z", "+00:00")).astimezone(timezone.utc)
                completed_wanted.append(it)
            except Exception:
                continue

        def _is_skipped(it: dict) -> bool:
            rid = it.get("reportId")
            return str(rid) in skip_ids if rid is not None else False

        # helper: find exact match
        def _find_exact(frm: dt.datetime, to: dt.datetime) -> dict | None:
            matches = [it for it in completed_wanted if it["_from"] == frm and it["_to"] == to]
            if not matches:
                return None
            matches.sort(key=lambda it: (_is_skipped(it), it["_to"]))
            return matches[0]

        # helper: for the current-year chunk, reuse if any Completed export covers "today"
        def _find_covers_today_for_chunk(frm: dt.datetime) -> dict | None:
            # accept any export where _from <= frm (same year or earlier)
            # and its timeTo falls on today's date (UTC)
            candidates = [
                it for it in completed_wanted
                if it["_from"] <= frm and it["_to"].date() == today_utc and it["_from"].year == frm.year
            ]
            if not candidates:
                return None
            candidates.sort(key=lambda it: it["_to"], reverse=True)
            for cand in candidates:
                if not _is_skipped(cand):
                    return cand
            return candidates[0]

        # -------- reuse existing / decide which to create
        links_or_ids: list[tuple[int | None, str | None, dt.datetime, dt.datetime]] = []
        need_to_create: list[tuple[dt.datetime, dt.datetime]] = []
        reused_existing = 0
        skipped_existing = 0

        for idx, (frm, to) in enumerate(chunks):
            exact = _find_exact(frm, to)
            if exact:
                rid = exact.get("reportId")
                if _is_skipped(exact):
                    skipped_existing += 1
                    logging.info(
                        "Chunk %d/%d (%s → %s) already stored locally via export %s; skipping download.",
                        idx + 1,
                        len(chunks),
                        frm.date(),
                        to.date(),
                        rid,
                    )
                    continue
                reused_existing += 1
                logging.info(
                    "Chunk %d/%d (%s → %s) reusing completed export %s.",
                    idx + 1,
                    len(chunks),
                    frm.date(),
                    to.date(),
                    rid,
                )
                links_or_ids.append((rid, exact["downloadLink"], frm, to))
                continue

            is_last_chunk = (idx == len(chunks) - 1)
            if is_last_chunk and not force_request:
                covers = _find_covers_today_for_chunk(frm)
                if covers:
                    # treat as covered: reuse that link to avoid re-requesting
                    rid = covers.get("reportId")
                    if _is_skipped(covers):
                        skipped_existing += 1
                        logging.info(
                            "Chunk %d/%d (%s → %s) already satisfied locally by export %s covering %s → %s; skipping download.",
                            idx + 1,
                            len(chunks),
                            frm.date(),
                            to.date(),
                            rid,
                            covers["_from"].date(),
                            covers["_to"].date(),
                        )
                        continue
                    reused_existing += 1
                    logging.info(
                        "Chunk %d/%d (%s → %s) reusing export %s covering %s → %s.",
                        idx + 1,
                        len(chunks),
                        frm.date(),
                        to.date(),
                        rid,
                        covers["_from"].date(),
                        covers["_to"].date(),
                    )
                    links_or_ids.append((rid, covers["downloadLink"], covers["_from"], covers["_to"]))
                    continue

            logging.info(
                "Chunk %d/%d (%s → %s) not covered; scheduling new export request.",
                idx + 1,
                len(chunks),
                frm.date(),
                to.date(),
            )
            need_to_create.append((frm, to))

        # -------- create missing (respect POST 1/30s)
        created_ids: list[int] = []
        for i, (frm, to) in enumerate(need_to_create):
            rid = self.request_csv_export(frm, to, include=include)
            logging.info(f"New export was requested: request_id={rid}")
            created_ids.append(rid)
            links_or_ids.append((rid, None, frm, to))
            if i < len(need_to_create) - 1:
                _sleep(post_gap_sec)

        # -------- poll until new ones ready (respect GET 1/min)
        if created_ids:
            deadline = time.time() + timeout_sec
            pending = set(created_ids)
            while pending and time.time() < deadline:
                _sleep(poll_interval_sec)
                lst = self.get_exports()
                by_id = {it["reportId"]: it for it in lst}
                for i, (rid, link, frm, to) in enumerate(links_or_ids):
                    if rid in pending and rid in by_id:
                        it = by_id[rid]
                        if it.get("status") == "Completed" and it.get("downloadLink"):
                            links_or_ids[i] = (rid, it["downloadLink"], frm, to)
                            pending.discard(rid)
            if pending:
                raise ApiError(f"Timed out waiting for exports: {sorted(pending)}")

        # -------- download & merge
        import pandas as pd

        frames: list[pd.DataFrame] = []

        def _read_one(link: str, rid: int | None, frm: dt.datetime, to: dt.datetime):
            r = self._dl.get(link, timeout=60)
            r.raise_for_status()
            buf = r.content
            if len(buf) >= 4 and buf[:2] == b"PK":
                with zipfile.ZipFile(BytesIO(buf)) as zf:
                    for name in zf.namelist():
                        if not name.lower().endswith(".csv"):
                            continue
                        info = zf.getinfo(name)
                        if info.file_size == 0:
                            # empty CSV inside the zip
                            continue
                        with zf.open(name) as f:
                            try:
                                df = pd.read_csv(f)
                            except Exception as e:
                                logging.warning(
                                    f"Error while trying to pd.read_csv(BytesIO(buf)) after reading {link=}: {e}")
                                continue
                            if df.empty:
                                continue
                            df["_file_name"] = name
                            df["_reportId"] = rid
                            df["_chunk_from"] = frm
                            df["_chunk_to"] = to
                            frames.append(df)
            else:
                if len(buf) == 0:
                    return
                try:
                    df = pd.read_csv(BytesIO(buf))
                except Exception as e:
                    logging.warning(
                        f"Error while trying to pd.read_csv(BytesIO(buf)) after reading {link=}: {e}")
                    return
                if df.empty:
                    return
                df["_file_name"] = None
                df["_reportId"] = rid
                df["_chunk_from"] = frm
                df["_chunk_to"] = to
                frames.append(df)

        logging.info(
            "Preparing to download %d export(s): %d reused, %d newly requested. %d export(s) already cached locally.",
            len(links_or_ids),
            reused_existing,
            len(created_ids),
            skipped_existing,
        )

        for rid, link, frm, to in links_or_ids:
            if not link:
                raise ApiError(f"Export {rid} has no downloadLink.")
            _read_one(link, rid if isinstance(rid, int) else None, frm, to)

        if not frames:
            import pandas as pd
            return pd.DataFrame()

        out = pd.concat(frames, ignore_index=True)


        # for col in ("Time", "CreatedAt", "Date", "ExecutionTime"):
        #     if col in out.columns:
        #         out[col] = pd.to_datetime(out[col], errors="coerce", utc=True)

        logging.info(f"Found a total of {out.shape[0]} rows and {out.shape[1]} columns from {out['Time'].min()} to {out['Time'].max()}")


        return out

    # ───────── creds persistence (future-proof) ─────────
    def _save(self) -> None:
        with self._LOCK:
            self._creds_file.save()
