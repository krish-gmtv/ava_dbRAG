import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from ava_session_manager import get_session_id

from precise_sql_templates_v1 import (
    combine_precise_extra_args,
    kpi_range_cli_from_plan,
    precise_cli_args_from_plan,
)

logger = logging.getLogger(__name__)


SCRIPTS_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPTS_DIR.parent
SHADOW_LOG_PATH = ROOT_DIR / "logs" / "route_advisor_shadow_v1.jsonl"


HANDLER_TO_SCRIPT = {
    # precise
    "precise_list_buyer_upsheets": SCRIPTS_DIR / "precise_list_buyer_upsheets.py",
    "precise_list_buyer_opportunities": SCRIPTS_DIR / "precise_list_buyer_opportunities.py",
    "precise_get_buyer_quarter_kpis": SCRIPTS_DIR / "precise_get_buyer_quarter_kpis.py",
    # semantic
    "semantic_buyer_performance_summary": SCRIPTS_DIR / "semantic_search_pinecone_final.py",
}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def parse_advisor_json(raw_text: str) -> Dict[str, Any]:
    """
    Parse a strict JSON object from advisor text.
    The text must be a single JSON object; wrapped prose is rejected.
    """
    s = (raw_text or "").strip()
    if not s:
        raise ValueError("Advisor returned empty text.")
    payload = json.loads(s)
    if not isinstance(payload, dict):
        raise ValueError("Advisor JSON must be an object.")

    mode = str(payload.get("recommended_mode") or "").strip().lower()
    family = str(payload.get("query_family") or "").strip()
    conf_raw = payload.get("confidence")
    try:
        confidence = float(conf_raw)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    explanation = str(payload.get("explanation") or "").strip()

    if mode not in {"precise", "semantic"}:
        raise ValueError("Advisor JSON missing valid recommended_mode.")
    if not family:
        raise ValueError("Advisor JSON missing query_family.")

    return {
        "recommended_mode": mode,
        "query_family": family,
        "confidence": confidence,
        "explanation": explanation,
    }


def parse_advisor_payload(payload: Any) -> Dict[str, Any]:
    """
    Parse advisor output from either a strict JSON string or an already-decoded dict.
    """
    if isinstance(payload, dict):
        return parse_advisor_json(json.dumps(payload, ensure_ascii=False))
    return parse_advisor_json(str(payload))


def call_ava_route_advisor_http(query: str, user_id: str, token: str) -> Dict[str, Any]:
    # Ava doc: token is raw Authorization header (no Bearer prefix).
    session_id = get_session_id(user_id=user_id, token=token)

    prompt = (
        "You are a route-classification advisor for analytics chat queries.\n"
        "Return ONLY one JSON object with keys:\n"
        "recommended_mode: 'precise' or 'semantic'\n"
        "query_family: one of buyer_quarter_kpis, buyer_performance_summary, list_buyer_upsheets, list_buyer_opportunities\n"
        "confidence: number 0..1\n"
        "explanation: short reason\n\n"
        "Rules:\n"
        "- Exact KPI/count/rate/value/date-bounded metric requests -> precise\n"
        "- Narrative/summary/trend/interpretive requests -> semantic\n"
        "- If ambiguous but includes explicit KPI/count/rate/value language, choose precise.\n\n"
        f"Query: {query}"
    )

    prism_resp = requests.post(
        "https://ava.andrew-chat.com/api/v1/prism",
        headers={
            "Authorization": token,
            "Content-Type": "application/json",
        },
        json={
            "user_id": user_id,
            "session_id": session_id,
            "message": prompt,
        },
        timeout=30,
    )
    prism_resp.raise_for_status()
    parts = prism_resp.json()
    if isinstance(parts, list):
        parsed: Optional[Dict[str, Any]] = None
        last_err: Optional[Exception] = None
        for p in parts:
            try:
                parsed = parse_advisor_payload(p)
                break
            except Exception as exc:
                last_err = exc
        if parsed is None:
            raise RuntimeError(
                f"Advisor response parts did not contain a valid JSON object. last_error={last_err}"
            )
    else:
        parsed = parse_advisor_payload(parts)
    return {
        "session_id": session_id,
        **parsed,
    }


def log_shadow_event(event: Dict[str, Any]) -> None:
    SHADOW_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SHADOW_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def run_python_json(
    script_path: Path,
    query: str,
    extra_args: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Run a python script that prints JSON to stdout.
    Returns parsed JSON dict or raises on failure.
    """
    if not script_path.exists():
        raise FileNotFoundError(f"Handler script not found: {script_path}")

    cmd = [sys.executable, str(script_path), "--query", query]
    if extra_args:
        cmd.extend(extra_args)
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )

    if proc.returncode != 0:
        raise RuntimeError(
            "Handler failed.\n"
            f"script={script_path}\n"
            f"exit_code={proc.returncode}\n"
            f"stderr={proc.stderr.strip()}\n"
            f"stdout={proc.stdout.strip()}"
        )

    out = proc.stdout.strip()
    if not out:
        raise RuntimeError(f"Handler produced no output: {script_path}")

    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "Handler output is not valid JSON.\n"
            f"script={script_path}\n"
            f"error={exc}\n"
            f"raw_output={out[:2000]}"
        ) from exc


def resolve_force_precise_handler(
    plan: Dict[str, Any],
    planner_v2_enabled: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """
    When the user requests SQL / precise data, map the router plan to a precise handler
    or return an explanatory error (never silently fall back to semantic).
    """
    rp = plan.get("retrieval_plan") or {}
    family = rp.get("query_family") or ""
    handler = rp.get("handler") or ""

    if isinstance(handler, str) and handler.startswith("precise_"):
        # Aggregated KPI SQL is not part of the "precise = direct table lookup" contract.
        if handler == "precise_get_buyer_quarter_kpis":
            return None, (
                "Use SQL / precise data is for direct table listings (upsheets, opportunities), "
                "not KPI aggregates. Turn off the toggle for KPI and performance summaries."
            )
        return handler, None

    if handler != "semantic_buyer_performance_summary":
        return None, (
            f"Use SQL / precise data is on, but the router produced unknown handler "
            f"'{handler}'. Turn off the toggle for normal routing."
        )

    if family == "list_buyer_upsheets":
        return "precise_list_buyer_upsheets", None

    if family == "buyer_quarter_kpis":
        return None, (
            "KPI and metric questions use semantic retrieval (precomputed summaries). "
            "Use SQL / precise for listing upsheets or opportunities, or turn off the toggle."
        )

    if family == "list_buyer_opportunities":
        if planner_v2_enabled:
            return "precise_list_buyer_opportunities", None
        return None, (
            "This question does not have a precise SQL path yet (buyer opportunities). "
            "Turn off Use SQL / precise data to use semantic search, or rephrase using "
            "buyer upsheets or KPIs with Buyer N and a period (for example Q1 2026)."
        )

    if family == "buyer_performance_summary":
        return None, (
            "Performance summaries use semantic retrieval. "
            "Use SQL / precise only for direct listings, e.g. 'List upsheets for Buyer N in Q1 2018' "
            "or 'List opportunities for Buyer N in Q1 2018', or turn off the toggle."
        )

    return None, (
        "This question does not have a mapped precise SQL path (upsheets or opportunities listings). "
        "Turn off Use SQL / precise data for semantic search."
    )


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "End-to-end execution flow: route a query, run the selected handler, "
            "and return a combined normalized payload."
        )
    )
    parser.add_argument("--query", type=str, required=True, help="User query to execute.")
    parser.add_argument(
        "--force-precise",
        action="store_true",
        help=(
            "After routing, force a precise(SQL) handler when supported. "
            "If no SQL path exists, the response explains why instead of using semantic search."
        ),
    )
    args = parser.parse_args()

    q = args.query.strip()
    if not q:
        raise SystemExit("Query must not be empty.")

    # 1) Route
    router_script = SCRIPTS_DIR / "intent_router_v1.py"
    logger.info("Routing query with intent_router_v1.py...")
    plan = run_python_json(router_script, q)

    handler = ((plan.get("retrieval_plan") or {}).get("handler")) if isinstance(plan, dict) else None
    if not isinstance(handler, str) or not handler:
        raise RuntimeError("Router plan missing retrieval_plan.handler")

    # Option A (shadow mode): deterministic router remains source of truth.
    # Ava advisor emits recommendation only for analysis/logging.
    shadow_enabled = env_bool("AVA_ROUTE_ADVISOR_SHADOW", default=False)
    shadow_event: Optional[Dict[str, Any]] = None
    if shadow_enabled:
        shadow_event = {
            "event": "advisor_shadow",
            "ts": datetime.now(timezone.utc).isoformat(),
            "query": q,
            "router_pre_override": {
                "mode": plan.get("mode"),
                "handler": handler,
                "query_family": ((plan.get("retrieval_plan") or {}).get("query_family")),
                "reason_codes": list(plan.get("reason_codes") or []),
            },
            "advisor": None,
            "status": "skipped",
            "error": None,
        }
        try:
            token = os.environ.get("AVA_TOKEN", "").strip()
            if not token:
                raise RuntimeError("AVA_TOKEN is not set for advisor shadow mode.")
            advisor_user_id = (
                os.environ.get("AVA_ROUTE_ADVISOR_USER_ID", "").strip()
                or "route-advisor-shadow"
            )
            advisor = call_ava_route_advisor_http(query=q, user_id=advisor_user_id, token=token)
            shadow_event["advisor"] = advisor
            shadow_event["status"] = "ok"
            logger.info(
                "Advisor shadow recommendation: mode=%s family=%s confidence=%.3f",
                advisor.get("recommended_mode"),
                advisor.get("query_family"),
                float(advisor.get("confidence") or 0.0),
            )
        except Exception as exc:
            shadow_event["status"] = "error"
            shadow_event["error"] = str(exc)
            logger.warning("Advisor shadow call failed: %s", exc)

    if args.force_precise:
        # Parameterized SQL for opportunities (NL slots → plan → precise_list_buyer_opportunities).
        # Set PRECISE_PLANNER_V2=false to restore legacy force-precise mapping for opportunities only.
        planner_v2_enabled = env_bool("PRECISE_PLANNER_V2", default=True)
        eff_handler, force_err = resolve_force_precise_handler(
            plan,
            planner_v2_enabled=planner_v2_enabled,
        )
        if force_err:
            if shadow_enabled and shadow_event is not None:
                shadow_event["executed"] = {
                    "mode": "force_precise_blocked",
                    "handler": None,
                    "query_family": ((plan.get("retrieval_plan") or {}).get("query_family")),
                    "force_precise": True,
                }
                adv = shadow_event.get("advisor") or {}
                adv_mode = str(adv.get("recommended_mode") or "").strip().lower()
                shadow_event["disagreement"] = bool(adv_mode and adv_mode != "force_precise_blocked")
                try:
                    log_shadow_event(shadow_event)
                except Exception as log_exc:
                    logger.warning("Advisor shadow log write failed: %s", log_exc)
            blocked = {
                "execution_plan": plan,
                "selected_handler": None,
                "handler_output": None,
                "force_precise_unavailable_reason": force_err,
            }
            print(json.dumps(blocked, indent=2, ensure_ascii=False))
            return
        rp_cur = plan.get("retrieval_plan") or {}
        if eff_handler != handler:
            rc = list(plan.get("reason_codes") or [])
            rc.append("decision:force_precise_user")
            plan = {
                **plan,
                "mode": "precise",
                "retrieval_plan": {**rp_cur, "handler": eff_handler},
                "reason_codes": sorted(set(rc)),
                "report_template_id": "buyer_detail_v1_precise",
            }
        handler = eff_handler

    if shadow_enabled and shadow_event is not None:
        shadow_event["executed"] = {
            "mode": plan.get("mode"),
            "handler": handler,
            "query_family": ((plan.get("retrieval_plan") or {}).get("query_family")),
            "force_precise": bool(args.force_precise),
        }
        adv = shadow_event.get("advisor") or {}
        adv_mode = str(adv.get("recommended_mode") or "").strip().lower()
        exec_mode = str(plan.get("mode") or "").strip().lower()
        shadow_event["disagreement"] = bool(adv_mode and exec_mode and adv_mode != exec_mode)
        try:
            log_shadow_event(shadow_event)
        except Exception as log_exc:
            logger.warning("Advisor shadow log write failed: %s", log_exc)

    script_path = HANDLER_TO_SCRIPT.get(handler)
    if script_path is None:
        raise RuntimeError(
            f"Unknown handler '{handler}'. "
            f"Known handlers: {sorted(HANDLER_TO_SCRIPT.keys())}"
        )

    # 2) Execute handler
    logger.info("Executing handler=%s via %s", handler, script_path.name)
    extra_args = combine_precise_extra_args(handler, plan)
    handler_output = run_python_json(
        script_path, q, extra_args=extra_args if extra_args else None
    )

    # 3) Combined normalized payload
    combined = {
        "execution_plan": plan,
        "selected_handler": handler,
        "handler_output": handler_output,
    }

    print(json.dumps(combined, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

