"""
Saved-report orchestration flow (v1).

This module exists to keep saved-report planning/execution behavior out of the main chat
pipeline function, reducing orchestration gravity and making the report product boundary
more explicit.
"""

from __future__ import annotations

import re
from http import HTTPStatus
from typing import Any, Callable, Dict, Optional


TimeoutRegex = re.compile(
    r"(read\s+timed\s+out|timed\s+out\s+after\s+retry|timed\s+out|timeout|etimedout|readtimeout|connect\s+timeout)",
    flags=re.IGNORECASE,
)


def try_handle_saved_report_request(
    *,
    query: str,
    rewritten_query: str,
    use_ava: bool,
    strict_validation: bool,
    force_precise: bool,
    developer_mode: bool,
    thread_id: str,
    app_user_id: str,
    attach_structured_payload: Callable[[Dict[str, Any], bool], None],
    update_thread_ctx: Callable[[str, Dict[str, Any]], None],
    saved_report_clarification_response: Callable[..., Dict[str, Any]],
    temporary_service_fallback_response: Callable[..., Dict[str, Any]],
    plan_saved_report: Callable[[str], Optional[Dict[str, Any]]],
    execute_saved_report_plan: Callable[..., Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    If the rewritten_query matches a saved report template, execute that flow and return
    a response shaped for /api/chat. Otherwise return None to let the caller proceed
    with legacy routing.
    """
    saved_plan = plan_saved_report(rewritten_query)
    if saved_plan is None:
        return None

    if not saved_plan.get("ready_to_execute"):
        clar = saved_report_clarification_response(
            query=query,
            executed_query=rewritten_query,
            thread_id=thread_id,
            app_user_id=app_user_id,
            plan=saved_plan,
        )
        attach_structured_payload(clar, developer_mode)
        return clar

    try:
        pipeline_output = execute_saved_report_plan(
            rewritten_query,
            saved_plan,
            use_ava=use_ava,
            strict_validation=strict_validation,
            thread_id=thread_id,
            app_user_id=app_user_id,
            force_precise=force_precise,
        )
    except Exception as exc:
        err_text = str(exc)
        if TimeoutRegex.search(err_text):
            fb = temporary_service_fallback_response(
                query=query,
                thread_id=thread_id,
                app_user_id=app_user_id,
            )
            attach_structured_payload(fb, developer_mode)
            return fb
        return {
            "error": "Saved report execution failed",
            "details": str(exc),
            "_http_status": int(HTTPStatus.INTERNAL_SERVER_ERROR),
        }

    update_thread_ctx(thread_id, pipeline_output)
    phrasing = (pipeline_output.get("phrasing") or {}) if isinstance(pipeline_output, dict) else {}
    final_response = (
        (pipeline_output.get("final_response") or {}) if isinstance(pipeline_output, dict) else {}
    )
    execution_plan = (
        (pipeline_output.get("execution_plan") or {}) if isinstance(pipeline_output, dict) else {}
    )

    response = {
        "query": query,
        "executed_query": rewritten_query,
        "thread_id": thread_id,
        "app_user_id": app_user_id,
        "display_text": phrasing.get("text", ""),
        "phrasing_mode": phrasing.get("mode", "unknown"),
        "source_mode": final_response.get("mode", "unknown"),
        "selected_handler": pipeline_output.get("selected_handler"),
        "report_template_id": execution_plan.get("report_template_id"),
        "force_precise": force_precise,
        "raw": pipeline_output,
    }
    attach_structured_payload(response, developer_mode)
    return response

