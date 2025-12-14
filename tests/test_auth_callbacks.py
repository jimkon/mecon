import datetime as dt

from services.auth.callbacks import _find_truelayer_session, _is_recent, CALLBACK_MAX_AGE_MINUTES


def test_is_recent_accepts_fresh_timestamp():
    now = dt.datetime.utcnow()
    assert _is_recent(now.isoformat()) is True


def test_is_recent_rejects_old_timestamp():
    old = dt.datetime.utcnow() - dt.timedelta(minutes=CALLBACK_MAX_AGE_MINUTES + 1)
    assert _is_recent(old.isoformat()) is False


def test_find_truelayer_session_matches_by_state():
    creds = {
        "truelayer": {
            "sources": {
                "ob-hsbc": {
                    "_transient_store": {
                        "ob-hsbc": {"state": "abc", "created_at": dt.datetime.utcnow().isoformat()}
                    }
                },
                "ob-monzo": {"_transient_store": {"other": {"state": "zzz"}}},
            }
        }
    }

    bank, provider_id, metadata = _find_truelayer_session(creds, "abc")
    assert bank == "ob-hsbc"
    assert provider_id == "ob-hsbc"
    assert metadata["state"] == "abc"


def test_find_truelayer_session_returns_none_for_unknown_state():
    creds = {"truelayer": {"sources": {"ob-hsbc": {"_transient_store": {"ob-hsbc": {"state": "abc"}}}}}}
    assert _find_truelayer_session(creds, "missing") is None
