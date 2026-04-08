import os
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv


load_dotenv()

PRISM_SESSION_URL_TEMPLATE = (
    "https://prism.andrew-chat.com/api/v1/prism/get_session/{user_id}/ava"
)
AVA_SESSION_GET_URL_TEMPLATE = "https://ava.andrew-chat.com/api/v1/session/{user_id}"
AVA_CLOSE_SESSION_URL_TEMPLATE = (
    "https://ava.andrew-chat.com/api/v1/session/{user_id}"
)


def _extract_session_id(data: Any) -> str:
    """
    Accept multiple plausible shapes:
    - {"session_id": "..."}
    - {"sessionId": "..."}
    - {"id": "..."}   # Prism variant seen in logs
    - {"data": {"session_id": "..."}}
    - [{"session_id": "..."}] (first item)
    - "raw-session-id-string"
    """
    if data is None:
        return ""
    if isinstance(data, str):
        return data.strip()
    if isinstance(data, list):
        if not data:
            return ""
        return _extract_session_id(data[0])
    if isinstance(data, dict):
        for key in ("session_id", "sessionId", "id"):
            v = data.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        nested = data.get("data")
        sid = _extract_session_id(nested)
        if sid:
            return sid
    return ""


def _request_session(url: str, token: str) -> str:
    resp = requests.get(url, headers={"Authorization": token}, timeout=30)
    resp.raise_for_status()
    text = resp.text or ""
    # Try JSON first
    try:
        data = resp.json()
        sid = _extract_session_id(data)
        if sid:
            return sid
        raise RuntimeError(
            f"Session response did not include a usable session id. url={url} body={text[:400]}"
        )
    except ValueError:
        # Non-JSON but maybe plain session id string.
        sid = text.strip()
        if sid:
            return sid
        raise RuntimeError(
            f"Session response was not JSON and not a session id string. url={url} body={text[:400]}"
        )


def get_session_id(user_id: str, token: str) -> str:
    """
    Try Prism endpoint first; fallback to Ava session GET endpoint.
    """
    prism_url = PRISM_SESSION_URL_TEMPLATE.format(user_id=user_id)
    try:
        return _request_session(prism_url, token)
    except Exception as prism_exc:
        ava_url = AVA_SESSION_GET_URL_TEMPLATE.format(user_id=user_id)
        try:
            return _request_session(ava_url, token)
        except Exception as ava_exc:
            raise RuntimeError(
                "Unable to acquire session_id from both session endpoints. "
                f"prism_error={prism_exc}; ava_error={ava_exc}"
            ) from ava_exc


def close_session(user_id: str, session_id: str, token: str) -> None:
    """
    Best-effort close. This is optional and should not break phrasing.
    """
    url = AVA_CLOSE_SESSION_URL_TEMPLATE.format(user_id=user_id)
    resp = requests.post(
        url,
        headers={"Authorization": token, "Content-Type": "application/json"},
        json={"session_id": session_id},
        timeout=30,
    )
    # Best-effort means caller should not fail on close errors.
    if resp.status_code >= 400:
        return


def resolve_user_and_thread_id(app_user_id: str, thread_id: str) -> str:
    """
    Ava doc: user_id can be any string, but must remain consistent across auth/session/ws steps.
    We encode thread_id into user_id for current thread-scoped isolation strategy.
    NOTE: This is a pragmatic adapter until a first-class thread_id->session_id backend store exists.
    """
    thread_id = (thread_id or "").strip() or "default-thread"
    return f"{app_user_id}:{thread_id}"


def _session_cache_file() -> Path:
    p = (os.environ.get("AVA_SESSION_CACHE_FILE") or "").strip()
    if p:
        return Path(p)
    return Path(__file__).resolve().parent.parent / ".ava_session_cache.json"


def _load_session_cache() -> Dict[str, str]:
    path = _session_cache_file()
    if not path.exists():
        return {}
    try:
        import json

        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return {str(k): str(v) for k, v in raw.items()}
    except Exception:
        pass
    return {}


def _save_session_cache(cache: Dict[str, str]) -> None:
    path = _session_cache_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    import json

    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def session_cache_key(app_user_id: str, thread_id: str) -> str:
    return f"{app_user_id}::{thread_id}"


def get_or_create_thread_session(
    app_user_id: str,
    thread_id: str,
    token: str,
) -> tuple[str, str]:
    """
    Returns (ava_user_id, session_id), reusing cached session_id for app_user_id+thread_id when available.
    """
    ava_user_id = resolve_user_and_thread_id(app_user_id=app_user_id, thread_id=thread_id)
    key = session_cache_key(app_user_id, thread_id)
    cache = _load_session_cache()
    cached_session = (cache.get(key) or "").strip()
    if cached_session:
        return ava_user_id, cached_session

    new_session = get_session_id(user_id=ava_user_id, token=token)
    cache[key] = new_session
    _save_session_cache(cache)
    return ava_user_id, new_session


def invalidate_thread_session(app_user_id: str, thread_id: str) -> None:
    key = session_cache_key(app_user_id, thread_id)
    cache = _load_session_cache()
    if key in cache:
        del cache[key]
        _save_session_cache(cache)

