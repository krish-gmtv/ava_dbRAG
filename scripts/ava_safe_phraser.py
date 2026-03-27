import json
import os
from typing import Any, Dict

from validate_ava_output_v1 import validate_ava_output

from ava_auth import resolve_token
from ava_session_manager import (
    close_session,
    get_or_create_thread_session,
    invalidate_thread_session,
)
from ava_ws_client import stream_chat_text


def build_mode_aware_ws_message(final_response: Dict[str, Any]) -> str:
    mode = (final_response.get("mode") or "").strip().lower()
    if mode == "precise":
        contract_description = (
            "Visible output contract (precise):\n"
            "- Opening request summary line (no heading required)\n"
            "- 'Key results:' section\n"
            "- Optional 'Notes:' section (only when notes exist)\n"
            "- 'Next:' section"
        )
    elif mode == "semantic":
        contract_description = (
            "Visible output contract (semantic):\n"
            "- Opening request summary line (no heading required)\n"
            "- One or two summary paragraphs\n"
            "- 'Highlights:' section\n"
            "- 'Next:' section"
        )
    else:
        contract_description = (
            "Visible output contract:\n"
            "- Opening summary line\n"
            "- 'Next:' section"
        )

    # Keep Ava input minimal and mode-focused instead of dumping entire payload.
    if mode == "precise":
        payload = {
            "mode": "precise",
            "request_summary": final_response.get("request_summary"),
            "kpi_snapshot": final_response.get("kpi_snapshot"),
            "supporting_details": final_response.get("supporting_details"),
            "data_coverage_notes": final_response.get("data_coverage_notes"),
            "suggested_next_question": final_response.get("suggested_next_question"),
        }
    else:
        payload = {
            "mode": "semantic",
            "request_summary": final_response.get("request_summary"),
            "executive_summary": final_response.get("executive_summary"),
            "trend_narrative": final_response.get("trend_narrative"),
            "highlights": final_response.get("highlights"),
            "suggested_next_question": final_response.get("suggested_next_question"),
            "confidence_note": final_response.get("confidence_note"),
        }

    return (
        "You are a response phrasing layer.\n"
        "Hard constraints:\n"
        "1) Do not change numbers or percentages.\n"
        "2) Do not invent metrics.\n"
        "3) Preserve null/NA meaning exactly (e.g., 'N/A' stays 'N/A').\n"
        f"4) Follow this exact visible structure:\n{contract_description}\n"
        "5) Output plain text only.\n\n"
        "Structured response JSON (source of truth):\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )


def safe_ws_phrase(
    final_response: Dict[str, Any],
    app_user_id: str,
    thread_id: str,
    strict_validation: bool,
) -> str:
    token = resolve_token()
    if not app_user_id.strip():
        raise RuntimeError("app_user_id is required for Ava thread-scoped session handling.")

    app_user_id = app_user_id.strip()
    user_id, session_id = get_or_create_thread_session(
        app_user_id=app_user_id,
        thread_id=thread_id,
        token=token,
    )
    message = build_mode_aware_ws_message(final_response)

    receive_timeout_sec = float(os.environ.get("AVA_WS_RECEIVE_TIMEOUT_SEC", "60"))
    idle_timeout_sec = float(os.environ.get("AVA_WS_IDLE_TIMEOUT_SEC", "4"))
    try:
        full_text, _frames = stream_chat_text(
            token=token,
            user_id=user_id,
            session_id=session_id,
            message=message,
            receive_timeout_sec=receive_timeout_sec,
            max_idle_sec=idle_timeout_sec,
        )
    except Exception as first_exc:
        # Session may be stale/invalid. Invalidate cache, reacquire once, retry once.
        invalidate_thread_session(app_user_id=app_user_id, thread_id=thread_id)
        user_id, session_id = get_or_create_thread_session(
            app_user_id=app_user_id,
            thread_id=thread_id,
            token=token,
        )
        try:
            full_text, _frames = stream_chat_text(
                token=token,
                user_id=user_id,
                session_id=session_id,
                message=message,
                receive_timeout_sec=receive_timeout_sec,
                max_idle_sec=idle_timeout_sec,
            )
        except Exception as retry_exc:
            raise RuntimeError(
                f"WS phrasing failed after one session refresh retry. first_error={first_exc}; retry_error={retry_exc}"
            ) from retry_exc

    # Validate phrasing output; if strict_validation is enabled, enforce headings/numeric fidelity.
    report = validate_ava_output(
        final_response=final_response,
        phrased_text=full_text,
        strict_headings=strict_validation,
    )
    if not report["is_valid"]:
        raise RuntimeError(f"Ava phrasing validation failed: {report}")

    # Optional close session
    if os.environ.get("AVA_CLOSE_SESSION", "false").strip().lower() in {"1", "true", "yes"}:
        try:
            close_session(user_id=user_id, session_id=session_id, token=token)
        except Exception:
            # best-effort only
            pass
        # Important: if we closed the session but keep the cached session_id,
        # the next request will likely reuse a stale session and can block on WS timeouts.
        try:
            invalidate_thread_session(app_user_id=app_user_id, thread_id=thread_id)
        except Exception:
            # best-effort only
            pass

    return full_text

