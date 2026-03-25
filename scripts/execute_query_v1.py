import argparse
import json
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


SCRIPTS_DIR = Path(__file__).resolve().parent


HANDLER_TO_SCRIPT = {
    # precise
    "precise_list_buyer_upsheets": SCRIPTS_DIR / "precise_list_buyer_upsheets.py",
    "precise_get_buyer_quarter_kpis": SCRIPTS_DIR / "precise_get_buyer_quarter_kpis.py",
    # semantic
    "semantic_buyer_performance_summary": SCRIPTS_DIR / "semantic_search_pinecone_final.py",
}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def kpi_range_cli_from_plan(handler: str, plan: Dict[str, Any]) -> List[str]:
    """
    Pass router-normalized calendar windows into precise_get_buyer_quarter_kpis.

    The intent router stores ISO start/end for both ``granularity=quarter`` and
    ``granularity=range``. The KPI script only parses a subset of phrasings from
    the raw query string; forwarding the plan dates keeps SQL aligned with routing
    when wording does not match ``Q1 2026`` or ``between ... and ...`` patterns.
    """
    if handler != "precise_get_buyer_quarter_kpis":
        return []
    tf = plan.get("timeframe") or {}
    gran = str(tf.get("granularity") or "").strip().lower()
    if gran not in ("quarter", "range"):
        return []
    start = tf.get("start")
    end = tf.get("end")
    if isinstance(start, str) and isinstance(end, str):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start) and re.fullmatch(
            r"\d{4}-\d{2}-\d{2}", end
        ):
            return ["--start-date", start, "--end-date", end]
    return []


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


def resolve_force_precise_handler(plan: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    When the user requests SQL / precise data, map the router plan to a precise handler
    or return an explanatory error (never silently fall back to semantic).
    """
    rp = plan.get("retrieval_plan") or {}
    family = rp.get("query_family") or ""
    handler = rp.get("handler") or ""

    if isinstance(handler, str) and handler.startswith("precise_"):
        return handler, None

    if handler != "semantic_buyer_performance_summary":
        return None, (
            f"Use SQL / precise data is on, but the router produced unknown handler "
            f"'{handler}'. Turn off the toggle for normal routing."
        )

    entity = plan.get("entity") or {}
    buyer_id = entity.get("resolved_id")
    tf = plan.get("timeframe") or {}
    gran = (tf.get("granularity") or "").strip().lower()

    if family == "list_buyer_upsheets":
        return "precise_list_buyer_upsheets", None

    if family == "buyer_quarter_kpis":
        return "precise_get_buyer_quarter_kpis", None

    if family == "list_buyer_opportunities":
        return None, (
            "This question does not have a precise SQL path yet (buyer opportunities). "
            "Turn off Use SQL / precise data to use semantic search, or rephrase using "
            "buyer upsheets or KPIs with Buyer N and a period (for example Q1 2026)."
        )

    if family == "buyer_performance_summary":
        if buyer_id is None:
            return None, (
                "Precise SQL needs a buyer reference such as 'Buyer 119' in your question."
            )
        if gran == "quarter":
            return "precise_get_buyer_quarter_kpis", None
        if gran == "range":
            return "precise_get_buyer_quarter_kpis", None
        if gran == "year":
            return None, (
                "Precise KPI SQL needs a specific quarter (for example Q1 2026), not only a year. "
                "Turn off Use SQL / precise data for a broader semantic answer, or add a quarter."
            )
        return None, (
            "Precise SQL needs a period in your question: a quarter (Q1 2026), a date range, "
            "or rephrase as KPIs or upsheets with Buyer N and dates."
        )

    return None, (
        "This question does not have a mapped precise SQL path. "
        "Turn off Use SQL / precise data for semantic search, or ask about KPIs or upsheets "
        "with Buyer N and a period."
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

    if args.force_precise:
        eff_handler, force_err = resolve_force_precise_handler(plan)
        if force_err:
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

    script_path = HANDLER_TO_SCRIPT.get(handler)
    if script_path is None:
        raise RuntimeError(
            f"Unknown handler '{handler}'. "
            f"Known handlers: {sorted(HANDLER_TO_SCRIPT.keys())}"
        )

    # 2) Execute handler
    logger.info("Executing handler=%s via %s", handler, script_path.name)
    kpi_extras = kpi_range_cli_from_plan(handler, plan)
    handler_output = run_python_json(
        script_path, q, extra_args=kpi_extras if kpi_extras else None
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

