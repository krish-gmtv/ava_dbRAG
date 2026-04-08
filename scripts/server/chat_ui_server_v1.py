import argparse
import json
import re
import subprocess
import sys
import threading
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

from structured_report_v1 import (
    build_developer_diagnostics,
    build_structured_report,
    build_structured_report_from_ui_fallback,
)
from template_executor_v1 import execute_saved_report_plan
from template_report_orchestrator_v1 import plan_saved_report
from chat_pipeline_v1 import process_chat_request

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
UI_HTML_PATH = ROOT_DIR / "ui" / "chat_ui_v1.html"
THREAD_CONTEXT: Dict[str, Dict[str, Any]] = {}
THREAD_CONTEXT_LOCK = threading.RLock()

BUYER_PATTERN = re.compile(r"\bbuyer\s*\d+\b", re.IGNORECASE)
DOMAIN_KEYWORDS = (
    "buyer",
    "upsheet",
    "upsheets",
    "opportunit",
    "close rate",
    "conversion",
    "lead",
    "kpi",
    "quarter",
    "q1",
    "q2",
    "q3",
    "q4",
    "201",
    "202",
)
GREETING_WORDS = ("hello", "hi", "hey", "good morning", "good afternoon", "good evening")
AFFIRM_WORDS = {"yes", "yeah", "yep", "sure", "ok", "okay", "do it", "go ahead"}


def attach_structured_payload(resp: Dict[str, Any], developer_mode: bool) -> None:
    raw = resp.get("raw")
    if isinstance(raw, dict) and isinstance(raw.get("final_response"), dict):
        resp["structured_report"] = build_structured_report(raw["final_response"])
    else:
        resp["structured_report"] = build_structured_report_from_ui_fallback(
            display_text=str(resp.get("display_text") or ""),
            source_mode=str(resp.get("source_mode") or ""),
        )
    if developer_mode:
        resp["developer"] = build_developer_diagnostics(raw if isinstance(raw, dict) else {})
    else:
        resp.pop("developer", None)


def run_pipeline(
    query: str,
    use_ava: bool,
    strict_validation: bool,
    force_precise: bool,
    thread_id: str,
    app_user_id: str,
) -> Dict[str, Any]:
    script_path = SCRIPTS_DIR / "execute_answer_with_ava_v1.py"
    cmd = [sys.executable, str(script_path), "--query", query]

    if use_ava:
        cmd.append("--use-ava")
    if strict_validation:
        cmd.append("--strict-validation")
    if force_precise:
        cmd.append("--force-precise")
    if thread_id.strip():
        cmd.extend(["--thread-id", thread_id.strip()])
    if app_user_id.strip():
        cmd.extend(["--app-user-id", app_user_id.strip()])

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            "Pipeline execution failed.\n"
            f"exit_code={proc.returncode}\n"
            f"stderr={proc.stderr.strip()}\n"
            f"stdout={proc.stdout.strip()}"
        )
    if not proc.stdout.strip():
        raise RuntimeError("Pipeline returned empty output.")
    return json.loads(proc.stdout)


def should_guardrail_query(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return True
    # Very short acknowledgements should never trigger retrieval.
    if q in {"ok", "okay", "yes", "yeah", "yep", "no", "nope", "thanks", "thank you"}:
        return True
    if len(q) < 8:
        return True

    has_buyer = BUYER_PATTERN.search(q) is not None
    has_domain_signal = any(k in q for k in DOMAIN_KEYWORDS)
    return not (has_buyer or has_domain_signal)


def is_affirmative(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False

    # remove punctuation and normalize whitespace
    q = re.sub(r"[^a-z0-9\s]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    if not q:
        return False

    # normalize stretched acknowledgements like "yeaaahh" -> "yeah"
    q_norm = re.sub(r"(.)\1{2,}", r"\1", q)
    q_compact = q_norm.replace(" ", "")

    direct_ack_set = {
        "yes",
        "yeah",
        "yea",
        "yep",
        "yup",
        "sure",
        "ok",
        "okay",
        "alright",
        "fine",
        "proceed",
        "continue",
    }
    if q_norm in direct_ack_set or q_compact in direct_ack_set:
        return True

    # stretched affirmatives like "yeaaahh", "yessss", "yaaa"
    if re.fullmatch(r"y(e|a)+h*", q_compact) or re.fullmatch(r"yes+", q_compact):
        return True

    # phrase-level positive intent
    positive_phrases = (
        "go ahead",
        "do it",
        "get it done",
        "carry on",
        "please proceed",
        "yes please",
        "sounds good",
        "lets do it",
        "let s do it",
        "that works",
    )
    return any(p in q_norm for p in positive_phrases)


def is_greeting(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    for g in GREETING_WORDS:
        if " " in g:
            if g in q:
                return True
        else:
            if re.search(rf"\b{re.escape(g)}\b", q):
                return True
    return False


def is_how_are_you_query(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    patterns = (
        r"\bhow are you\b",
        r"\bhow r you\b",
        r"\bhow you doing\b",
        r"\bhow are you doing\b",
        r"\bhow is it going\b",
        r"\bhow s it going\b",
    )
    return any(re.search(p, q) for p in patterns)


def _prev_quarter(year: int, quarter: int) -> tuple[int, int]:
    if quarter > 1:
        return year, quarter - 1
    return year - 1, 4


def build_list_upsheets_followup_query(ctx: Dict[str, Any]) -> str:
    """Rewrite 'yes' after a semantic summary into a precise upsheets listing query."""
    buyer_id = ctx.get("buyer_id")
    year = ctx.get("period_year")
    quarter = ctx.get("period_quarter")
    if buyer_id is None or year is None or quarter is None:
        return ""
    return f"List all upsheets for Buyer {buyer_id} in Q{quarter} {year}?"


def build_trend_followup_query(ctx: Dict[str, Any]) -> str:
    buyer_id = ctx.get("buyer_id")
    year = ctx.get("period_year")
    quarter = ctx.get("period_quarter")
    if buyer_id is None or year is None or quarter is None:
        return ""
    py, pq = _prev_quarter(int(year), int(quarter))
    return f"How did Buyer {buyer_id} perform in Q{pq} {py}?"


def get_thread_ctx(thread_id: str) -> Dict[str, Any]:
    key = (thread_id or "").strip()
    if not key:
        # Do not share a global "__default__" bucket across concurrent clients.
        # Without a stable thread_id, follow-up state should not persist anyway.
        return {}
    with THREAD_CONTEXT_LOCK:
        if key not in THREAD_CONTEXT:
            THREAD_CONTEXT[key] = {}
        return THREAD_CONTEXT[key]


def extract_period_parts(
    timeframe: Dict[str, Any],
    request_summary: str,
) -> Dict[str, Any]:
    raw_tf = str(timeframe.get("raw_text") or "").lower()
    start_tf = str(timeframe.get("start") or "")
    granularity = str(timeframe.get("granularity") or "").lower()
    summary = (request_summary or "").lower()

    quarter = None
    year = None

    q_match = re.search(r"\bq([1-4])\b", raw_tf) or re.search(r"\bq([1-4])\b", summary)
    y_match = re.search(r"\b(19\d{2}|20\d{2})\b", raw_tf) or re.search(
        r"\b(19\d{2}|20\d{2})\b",
        summary,
    )

    if q_match:
        try:
            quarter = int(q_match.group(1))
        except ValueError:
            quarter = None
    if y_match:
        try:
            year = int(y_match.group(1))
        except ValueError:
            year = None

    # Fallback: derive quarter/year from the normalized timeframe start date.
    if start_tf and re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_tf):
        try:
            start_dt = datetime.strptime(start_tf, "%Y-%m-%d")
            if year is None:
                year = start_dt.year
            if quarter is None and granularity in ("quarter", "range"):
                quarter = ((start_dt.month - 1) // 3) + 1
        except ValueError:
            pass

    return {
        "period_year": year,
        "period_quarter": quarter,
    }


def detect_pending_followups(suggested_next: str) -> Dict[str, bool]:
    text = (suggested_next or "").strip().lower()
    if not text:
        return {
            "pending_listing_followup": False,
            "pending_trend_followup": False,
        }

    # After semantic summaries, we offer Postgres row listings (upsheets/opportunities), not KPI SQL.
    pending_listing = (
        "list upsheets" in text
        or ("postgres" in text and "list" in text)
        or ("say yes" in text and "upsheets" in text)
        or ("row listing" in text and "period" in text)
    )
    pending_trend = (
        "quarter-over-quarter trend" in text
        or "quarter over quarter trend" in text
        or (
            "previous quarter" in text
            and (
                "compare" in text
                or "comparison" in text
                or "side by side" in text
                or "kpi" in text
                or "close rate" in text
            )
        )
        or ("trend" in text and "buyer" in text)
    )

    return {
        "pending_listing_followup": pending_listing,
        "pending_trend_followup": pending_trend,
    }


def update_thread_ctx(thread_id: str, pipeline_output: Dict[str, Any]) -> None:
    with THREAD_CONTEXT_LOCK:
        ctx = get_thread_ctx(thread_id)
        plan = (
            (pipeline_output.get("execution_plan") or {})
            if isinstance(pipeline_output, dict)
            else {}
        )
        entity = (plan.get("entity") or {}) if isinstance(plan, dict) else {}
        timeframe = (plan.get("timeframe") or {}) if isinstance(plan, dict) else {}
        final_response = (
            (pipeline_output.get("final_response") or {})
            if isinstance(pipeline_output, dict)
            else {}
        )
        request_summary = str(final_response.get("request_summary") or "")
        suggested_next = (final_response.get("suggested_next_question") or "").lower()

        ctx["buyer_id"] = entity.get("resolved_id")
        period_parts = extract_period_parts(timeframe=timeframe, request_summary=request_summary)
        if period_parts["period_quarter"] is not None:
            ctx["period_quarter"] = period_parts["period_quarter"]
        if period_parts["period_year"] is not None:
            ctx["period_year"] = period_parts["period_year"]

        pending = detect_pending_followups(suggested_next)
        ctx["pending_listing_followup"] = pending["pending_listing_followup"]
        ctx["pending_trend_followup"] = pending["pending_trend_followup"]


def is_gratitude(query: str) -> bool:
    q = (query or "").strip().lower()
    q = re.sub(r"[^a-z0-9\s]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    if not q:
        return False
    phrases = (
        "thanks",
        "thank you",
        "thx",
        "ty",
        "ok thanks",
        "okay thanks",
        "thanks ok",
        "appreciate it",
        "much appreciated",
        "cheers",
    )
    if q in phrases:
        return True
    if (q.startswith("thanks") or q.startswith("thank you")) and len(q) <= 48:
        return True
    return False


def gratitude_response(
    query: str,
    thread_id: str,
    app_user_id: str,
) -> Dict[str, Any]:
    return {
        "query": query,
        "thread_id": thread_id,
        "app_user_id": app_user_id,
        "display_text": (
            "You're welcome. Ask another buyer question any time, for example:\n"
            "- How did Buyer 1 perform in Q1 2018?\n"
            "- List all upsheets for Buyer 2 in Q1 2018"
        ),
        "phrasing_mode": "ui_gratitude",
        "source_mode": "gratitude",
        "selected_handler": None,
        "report_template_id": None,
        "raw": {
            "execution_plan": None,
            "selected_handler": None,
            "final_response": None,
            "phrasing": {
                "mode": "ui_gratitude",
                "text": "Short gratitude acknowledgement.",
            },
        },
    }


def guardrail_response(
    query: str,
    thread_id: str,
    app_user_id: str,
    mode: str = "offtopic",
) -> Dict[str, Any]:
    q_lower = (query or "").strip().lower()

    def time_of_day_phrase() -> str:
        hour = datetime.now().hour
        if 5 <= hour < 12:
            return "this morning"
        if 12 <= hour < 17:
            return "this afternoon"
        if 17 <= hour < 22:
            return "this evening"
        return "right now"

    def time_of_day_greeting() -> str:
        hour = datetime.now().hour
        if 5 <= hour < 12:
            return "Good morning"
        if 12 <= hour < 17:
            return "Good afternoon"
        return "Good evening"

    def how_are_you_line() -> str:
        hour = datetime.now().hour
        if 5 <= hour < 12:
            return "Fuelled up to help you."
        if 12 <= hour < 17:
            return "Pulling through the afternoon slumber, but helping you will surely fuel me up."
        return "Clearing the evening checklist? Let's get it together."

    if is_how_are_you_query(query):
        text = (
            f"{how_are_you_line()}\n\n"
            "I can help with buyer KPI questions from this dataset.\n"
            "Try one of these:\n"
            "- How did Buyer 1 perform in Q1 2018?\n"
            "- What was Buyer 2's close rate in Q1 2018?\n"
            "- List all upsheets for Buyer 2 in Q1 2018"
        )
    elif mode == "greeting":
        text = (
            f"{time_of_day_greeting()}! Happy to help.\n\n"
            "I can answer buyer KPI questions from this dataset.\n"
            "Try one of these:\n"
            "- How did Buyer 1 perform in Q1 2018?\n"
            "- What was Buyer 2's close rate in Q1 2018?\n"
            "- List all upsheets for Buyer 2 in Q1 2018"
        )
    elif any(k in q_lower for k in ("hungry", "lunch", "dinner", "eat", "food")):
        text = (
            f"That sounds like a good call {time_of_day_phrase()}.\n\n"
            "I can only help with buyer performance and KPI questions for this dataset.\n"
            "If you want, ask me one of these and I will run it directly:\n"
            "- How did Buyer 1 perform in Q1 2018?\n"
            "- What was Buyer 2's close rate in Q1 2018?\n"
            "- List all upsheets for Buyer 2 in Q1 2018"
        )
    else:
        text = (
            "I can help with buyer performance questions for this dataset, "
            "but I need a specific buyer and period to run the right retrieval.\n\n"
            "Try one of these:\n"
            "- How did Buyer 1 perform in Q1 2018?\n"
            "- What was Buyer 2's close rate in Q1 2018?\n"
            "- List all upsheets for Buyer 2 in Q1 2018"
        )

    return {
        "query": query,
        "thread_id": thread_id,
        "app_user_id": app_user_id,
        "display_text": text,
        "phrasing_mode": "ui_guardrail",
        "source_mode": "guardrail",
        "selected_handler": None,
        "report_template_id": None,
        "raw": {
            "execution_plan": None,
            "selected_handler": None,
            "final_response": None,
            "phrasing": {
                "mode": "ui_guardrail",
                "text": "Guardrail clarification response returned before retrieval.",
            },
        },
    }


def saved_report_clarification_response(
    *,
    query: str,
    executed_query: str,
    thread_id: str,
    app_user_id: str,
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    missing = plan.get("missing_required_slots") or []
    parts: list[str] = []
    if "buyer" in missing:
        parts.append('Please name a buyer (for example "Buyer 2") so I can run this saved report.')
    if "timeframe" in missing:
        parts.append("Please add a period such as Q1 2019 or an explicit date range.")
    msg = " ".join(parts) if parts else "Please add the missing details so I can run this saved report."
    ep = {
        "report_template_id": plan.get("template_id"),
        "saved_report_plan": plan,
    }
    return {
        "query": query,
        "executed_query": executed_query,
        "thread_id": thread_id,
        "app_user_id": app_user_id,
        "display_text": msg,
        "phrasing_mode": "ui_saved_report_clarification",
        "source_mode": "saved_report_clarification",
        "selected_handler": None,
        "report_template_id": plan.get("template_id"),
        "raw": {
            "execution_plan": ep,
            "selected_handler": None,
            "final_response": None,
            "phrasing": {
                "mode": "ui_saved_report_clarification",
                "text": msg,
            },
        },
    }


def temporary_service_fallback_response(
    query: str,
    thread_id: str,
    app_user_id: str,
) -> Dict[str, Any]:
    return {
        "query": query,
        "thread_id": thread_id,
        "app_user_id": app_user_id,
        "display_text": (
            "I could not complete semantic retrieval right now due to a temporary upstream timeout.\n\n"
            "Please try again in a few seconds, or ask a precise query like:\n"
            "- What was Buyer 1's close rate in Q1 2018?\n"
            "- List all upsheets for Buyer 1 in Q1 2018"
        ),
        "phrasing_mode": "ui_service_fallback",
        "source_mode": "fallback",
        "selected_handler": None,
        "report_template_id": None,
        "raw": {
            "execution_plan": None,
            "selected_handler": None,
            "final_response": None,
            "phrasing": {
                "mode": "ui_service_fallback",
                "text": "Temporary semantic retrieval timeout fallback.",
            },
        },
    }


class ChatHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, status: int, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/chat"):
            if not UI_HTML_PATH.exists():
                self._send_html(
                    HTTPStatus.NOT_FOUND,
                    "<h1>UI file not found</h1><p>Create ui/chat_ui_v1.html first.</p>",
                )
                return
            self._send_html(HTTPStatus.OK, UI_HTML_PATH.read_text(encoding="utf-8"))
            return

        if parsed.path == "/health":
            self._send_json(
                HTTPStatus.OK,
                {"status": "ok", "service": "chat-ui-server-v1"},
            )
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/chat":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length) if content_length > 0 else b""
        try:
            payload = json.loads(raw_body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Body must be valid JSON."})
            return

        query = str(payload.get("query") or "").strip()
        if not query:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "query is required."})
            return

        use_ava = bool(payload.get("use_ava", False))
        strict_validation = bool(payload.get("strict_validation", False))
        force_precise = bool(payload.get("force_precise", False))
        developer_mode = bool(payload.get("developer_mode", False))
        thread_id = str(payload.get("thread_id") or "").strip()
        app_user_id = str(payload.get("app_user_id") or "").strip()
        response = process_chat_request(
            query=query,
            use_ava=use_ava,
            strict_validation=strict_validation,
            force_precise=force_precise,
            developer_mode=developer_mode,
            thread_id=thread_id,
            app_user_id=app_user_id,
            get_thread_ctx=get_thread_ctx,
            thread_ctx_lock=THREAD_CONTEXT_LOCK,
            is_gratitude=is_gratitude,
            gratitude_response=gratitude_response,
            is_affirmative=is_affirmative,
            build_list_upsheets_followup_query=build_list_upsheets_followup_query,
            build_trend_followup_query=build_trend_followup_query,
            should_guardrail_query=should_guardrail_query,
            is_greeting=is_greeting,
            guardrail_response=guardrail_response,
            temporary_service_fallback_response=temporary_service_fallback_response,
            saved_report_clarification_response=saved_report_clarification_response,
            attach_structured_payload=attach_structured_payload,
            update_thread_ctx=update_thread_ctx,
            plan_saved_report=plan_saved_report,
            execute_saved_report_plan=execute_saved_report_plan,
            run_pipeline=run_pipeline,
        )
        status = int(response.pop("_http_status", HTTPStatus.OK))
        self._send_json(status, response)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Local UI server for Ava DB RAG v1 chat integration."
    )
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), ChatHandler)
    print(f"Chat UI server running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

