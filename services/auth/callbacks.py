import datetime as dt
import logging
from typing import Dict, Optional, Tuple

import starlette.status as status
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.responses import RedirectResponse

from mecon.etl.monzo_api_client import MonzoClient
from mecon.etl.true_layer_client_by_o3 import TrueLayerClient


LOGGER = logging.getLogger(__name__)

# Use a short window so stale or unsolicited redirects cannot overwrite creds.
CALLBACK_MAX_AGE_MINUTES = 10


def _parse_created_at(raw: Optional[str]) -> Optional[dt.datetime]:
    if not raw:
        return None
    try:
        return dt.datetime.fromisoformat(raw)
    except ValueError:
        return None


def _is_recent(raw: Optional[str]) -> bool:
    created_at = _parse_created_at(raw)
    if created_at is None:
        return False
    delta = dt.datetime.utcnow() - created_at
    return delta <= dt.timedelta(minutes=CALLBACK_MAX_AGE_MINUTES)


def _find_truelayer_session(creds: Dict, state: str) -> Optional[Tuple[str, str, Dict]]:
    tl_creds = creds.get("truelayer", {})
    for bank, details in tl_creds.get("sources", {}).items():
        transient = details.get("_transient_store", {})
        for provider_id, metadata in transient.items():
            if metadata.get("state") == state:
                return bank, provider_id, metadata
    return None


def _build_success_response(message: str) -> Response:
    return HTMLResponse(
        content=f"<html><body><h3>{message}</h3><p>You can close this window.</p></body></html>",
        status_code=status.HTTP_200_OK,
    )

def _build_success_redirect(to: str) -> Response:
    return RedirectResponse(url=to, status_code=302)


async def handle_callback(request: Request) -> Response:
    provider = request.path_params.get("provider")
    query_params = request.query_params

    if provider not in {"truelayer", "monzo", "trading212"}:
        return JSONResponse(
            {"error": f"Unknown provider '{provider}'"},
            status_code=status.HTTP_404_NOT_FOUND,
        )

    if "error" in query_params:
        # Propagate provider errors without touching local creds
        return JSONResponse(
            {"error": query_params.get("error"), "error_description": query_params.get("error_description")},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    try:
        from mecon.app import shiny_app  # TODO get the creds without importing shiny_app

        dataset = shiny_app.get_working_dataset()
        creds = dataset.creds
    except Exception as exc:  # pragma: no cover - defensive; unlikely during tests
        LOGGER.exception("Unable to load working dataset")
        return JSONResponse(
            {"error": "server_not_ready", "detail": str(exc)},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    if provider == "truelayer":
        return await _handle_truelayer_callback(query_params, creds)
    if provider == "monzo":
        return await _handle_monzo_callback(request, query_params, creds)

    # Placeholder: prevent silent success for providers we do not yet support.
    return JSONResponse(
        {"error": "provider_not_configured", "detail": "Trading212 callback handling not yet implemented."},
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
    )


async def _handle_truelayer_callback(query_params, creds: Dict) -> Response:
    code = query_params.get("code")
    state = query_params.get("state")

    if not code or not state:
        return JSONResponse(
            {"error": "invalid_request", "detail": "Missing code or state."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    session_info = _find_truelayer_session(creds, state)
    if session_info is None:
        return JSONResponse(
            {"error": "unauthorised", "detail": "No matching authorisation session."},
            status_code=status.HTTP_403_FORBIDDEN,
        )

    bank, provider_id, metadata = session_info
    if not _is_recent(metadata.get("created_at")):
        return JSONResponse(
            {"error": "expired", "detail": "Authorisation session is no longer valid."},
            status_code=status.HTTP_410_GONE,
        )

    client = TrueLayerClient(creds)
    try:
        client.exchange_code(bank=bank, code=code, returned_state=state, provider_id=provider_id)
    except Exception as exc:  # pragma: no cover - network/creds errors exercised in integration
        LOGGER.exception("TrueLayer code exchange failed")
        return JSONResponse(
            {"error": "exchange_failed", "detail": str(exc)},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    # return _build_success_response(f"TrueLayer: credentials stored for {bank}")
    logging.info(f"TrueLayer: credentials stored for {bank}")
    return _build_success_redirect(to="http://127.0.0.1:8003/auth/truelayer/")


async def _handle_monzo_callback(request: Request, query_params, creds: Dict) -> Response:
    code = query_params.get("code")
    state = query_params.get("state")

    if not code or not state:
        return JSONResponse(
            {"error": "invalid_request", "detail": "Missing code or state."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    pending = creds.get("monzo-api", {}).get("_pending_auth", {})
    if pending.get("state") != state:
        return JSONResponse(
            {"error": "unauthorised", "detail": "Unknown or mismatched authorisation session."},
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if not _is_recent(pending.get("created_at")):
        return JSONResponse(
            {"error": "expired", "detail": "Authorisation session is no longer valid."},
            status_code=status.HTTP_410_GONE,
        )

    client = MonzoClient(creds)
    try:
        # Full URL preserves any extra parameters Monzo may send back.
        client.set_authentication_code_from_url(str(request.url))
    except Exception as exc:  # pragma: no cover - network/creds errors exercised in integration
        LOGGER.exception("Monzo code exchange failed")
        return JSONResponse(
            {"error": "exchange_failed", "detail": str(exc)},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    # Clear pending flag so repeated callbacks are rejected.
    creds["monzo-api"].pop("_pending_auth", None)
    try:
        creds.save()
    except Exception:
        LOGGER.exception("Failed to persist Monzo credentials")

    # return _build_success_response("Monzo authorisation complete")
    logging.info("Monzo authorisation complete")
    return _build_success_redirect(to="http://127.0.0.1:8003/auth/monzo/")