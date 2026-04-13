"""
Chat pipeline orchestration (v1).

Implementation moved from top-level ``chat_pipeline_v1.py`` to improve navigation.
The top-level module remains as a thin compatibility wrapper.
"""

from __future__ import annotations

import re
from http import HTTPStatus
from typing import Any, Callable, Dict, Optional


TimeoutRegex = re.compile(
    r"(read\s+timed\s+out|timed\s+out\s+after\s+retry|timed\s+out|timeout|etimedout|readtimeout|connect\s+timeout)",
    flags=re.IGNORECASE,
)


def process_chat_request(
    *,
    query: str,
    use_ava: bool,
    strict_validation: bool,
    force_precise: bool,
    developer_mode: bool,
    thread_id: str,
    app_user_id: str,
    # added helpers objects after file restructure (owned by chat_ui_server_v1)
    get_thread_ctx: Callable[[str], Dict[str, Any]],
    thread_ctx_lock: Any,
    is_gratitude: Callable[[str], bool],
    gratitude_response: Callable[..., Dict[str, Any]],
    is_affirmative: Callable[[str], bool],
    build_list_upsheets_followup_query: Callable[[Dict[str, Any]], str],
    build_trend_followup_query: Callable[[Dict[str, Any]], str],
    should_guardrail_query: Callable[[str], bool],
    is_greeting: Callable[[str], bool],
    guardrail_response: Callable[..., Dict[str, Any]],
    temporary_service_fallback_response: Callable[..., Dict[str, Any]],
    saved_report_clarification_response: Callable[..., Dict[str, Any]],
    attach_structured_payload: Callable[[Dict[str, Any], bool], None],
    update_thread_ctx: Callable[[str, Dict[str, Any]], None],
    # saved report path
    plan_saved_report: Callable[[str], Optional[Dict[str, Any]]],
    execute_saved_report_plan: Callable[..., Dict[str, Any]],
    # failback to pipeline execution for queries without saved plans
    run_pipeline: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Returns a response payload shaped for ``/api/chat``.
    Raises only for unexpected programmer errors; runtime exceptions are converted into
    safe JSON responses consistent with current server behavior.
    """
    thread_ctx = get_thread_ctx(thread_id)

    # Follow-through behavior: if the previous response suggested a next step and user affirms(for now that is a yes),
    # execute that recommendation for the same buyer/period.
    if is_gratitude(query):
        g = gratitude_response(
            query=query,
            thread_id=thread_id,
            app_user_id=app_user_id,
        )
        attach_structured_payload(g, developer_mode)
        return g

    rewritten_query = query
    if is_affirmative(query):
        #Taking user input in the same sequence that is a follow up so thread.
        if bool(thread_ctx.get("pending_listing_followup")):
            candidate = build_list_upsheets_followup_query(thread_ctx)
            if candidate:
                rewritten_query = candidate
                with thread_ctx_lock:
                    thread_ctx["pending_listing_followup"] = False
                    thread_ctx["pending_trend_followup"] = False
        elif bool(thread_ctx.get("pending_trend_followup")):
            candidate = build_trend_followup_query(thread_ctx)
            if candidate:
                rewritten_query = candidate
                with thread_ctx_lock:
                    thread_ctx["pending_listing_followup"] = False
                    thread_ctx["pending_trend_followup"] = False

    saved_plan = plan_saved_report(rewritten_query)
    if saved_plan is not None:
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
            #In case it takes too long to work through the saved report execution, we want to return a fallback response instead of an error.
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
        phrasing = (
            (pipeline_output.get("phrasing") or {}) if isinstance(pipeline_output, dict) else {}
        )
        final_response = (
            (pipeline_output.get("final_response") or {})
            if isinstance(pipeline_output, dict)
            else {}
        )
        execution_plan = (
            (pipeline_output.get("execution_plan") or {})
            if isinstance(pipeline_output, dict)
            else {}
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
#Off topic drift check and fallback to guardrail response if needed. This is the last check before running the potentially expensive pipeline execution.
    if should_guardrail_query(rewritten_query):
        mode = "greeting" if is_greeting(query) else "offtopic"
        gr = guardrail_response(
            query=query,
            thread_id=thread_id,
            app_user_id=app_user_id,
            mode=mode,
        )
        attach_structured_payload(gr, developer_mode)
        return gr

    try:
        pipeline_output = run_pipeline(
            query=rewritten_query,
            use_ava=use_ava,
            strict_validation=strict_validation,
            force_precise=force_precise,
            thread_id=thread_id,
            app_user_id=app_user_id,
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
            "error": "Chat pipeline failed",
            "details": str(exc),
            "_http_status": int(HTTPStatus.INTERNAL_SERVER_ERROR),
        }

    update_thread_ctx(thread_id, pipeline_output)

    phrasing = (pipeline_output.get("phrasing") or {}) if isinstance(pipeline_output, dict) else {}
    final_response = (
        (pipeline_output.get("final_response") or {})
        if isinstance(pipeline_output, dict)
        else {}
    )
    execution_plan = (
        (pipeline_output.get("execution_plan") or {})
        if isinstance(pipeline_output, dict)
        else {}
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

