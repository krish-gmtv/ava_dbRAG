import argparse
import json
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from _bootstrap_repo_root import ensure_repo_root_on_syspath

ensure_repo_root_on_syspath()

from intent_router_v1 import period_from_execution_plan
from scripts.reporting.semantic_catalog_v1 import format_available_periods_line
from scripts.reporting.semantic_quality_v1 import evaluate_semantic_quality


logger = logging.getLogger(__name__)


def _honest_no_semantic_summary(buyer_label: str, period_label: str) -> str:
    return (
        f"I couldn't find a reliable precomputed quarterly summary for {buyer_label} "
        f"in {period_label}. Try another quarter or year, or use the SQL / precise toggle "
        f"for Postgres row listings (upsheets or opportunities)."
    )


def _no_semantic_summary_with_hints(
    *,
    buyer_label: str,
    buyer_id: Any,
    requested_period_label: str,
    best_period_label: Any,
    reasons: List[str],
) -> str:
    base = _honest_no_semantic_summary(buyer_label, requested_period_label)
    bid = None
    try:
        bid = int(buyer_id) if buyer_id is not None else None
    except Exception:
        bid = None

    # If we fail-closed due to period mismatch, say so explicitly.
    if any("metadata_mismatch_fail_closed" in r for r in reasons) or any(
        str(r).startswith("top_match_period_") for r in reasons
    ):
        bp = str(best_period_label or "").strip()
        if bp:
            base = (
                f"{base}\n\n"
                f"Note: the closest semantic match appears to be for {bp}, but you requested "
                f"{requested_period_label}. I’m not showing the wrong-quarter summary."
            )

    if bid is not None:
        line = format_available_periods_line(bid)
        if line:
            base = f"{base}\n\n{line}"
    return base


_RETRIEVAL_WEAK_STATUS = (
    "Semantic retrieval for this buyer and period is limited: match quality or metadata "
    "alignment did not meet the stronger evidence bar."
)
SCRIPTS_DIR = Path(__file__).resolve().parent


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def run_execute_query(query: str, force_precise: bool = False) -> Dict[str, Any]:
    """
    Run execute_query_v1.py and parse its combined JSON output.
    """
    script_path = SCRIPTS_DIR / "execute_query_v1.py"
    cmd = [sys.executable, str(script_path), "--query", query]
    if force_precise:
        cmd.append("--force-precise")
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "execute_query_v1.py failed.\n"
            f"exit_code={proc.returncode}\n"
            f"stderr={proc.stderr.strip()}\n"
            f"stdout={proc.stdout.strip()}"
        )
    out = proc.stdout.strip()
    if not out:
        raise RuntimeError("execute_query_v1.py produced empty output.")
    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "execute_query_v1.py output is not valid JSON.\n"
            f"error={exc}\n"
            f"raw_output={out[:2000]}"
        ) from exc


def parse_num(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip()
        if not v or v.lower() == "null":
            return None
        try:
            return float(v)
        except ValueError:
            return None
    return None


def format_pct(value: Any) -> str:
    n = parse_num(value)
    if n is None:
        return "N/A"
    return f"{n:.2f}%"


def format_amount(value: Any) -> str:
    n = parse_num(value)
    if n is None:
        return "N/A"
    return f"{n:.2f}"


def buyer_label_for_report(
    buyer_id: Any,
    buyer_name_fallback: str,
    entity: Optional[Dict[str, Any]],
) -> str:
    """
    Prefer numeric Buyer id so successive questions read the same way
    (Buyer 1 … vs Buyer 2 …) regardless of display name from retrieval.
    """
    rid = buyer_id
    if rid is None and entity:
        rid = entity.get("resolved_id")
    if rid is not None:
        return f"Buyer {rid}"
    name = (buyer_name_fallback or "").strip()
    if name:
        return name
    return "the requested buyer"


def period_phrase_for_report(
    params: Dict[str, Any], plan: Optional[Dict[str, Any]] = None
) -> str:
    plan_py: Optional[int] = None
    plan_pq: Optional[int] = None
    if plan:
        plan_py, plan_pq = period_from_execution_plan(plan)
    year = params.get("period_year")
    quarter = params.get("period_quarter")
    if year is None:
        year = plan_py
    if quarter is None:
        quarter = plan_pq
    start_date = params.get("start_date") or params.get("period_start")
    end_date = params.get("end_date") or params.get("period_end")
    if year is not None and quarter is not None:
        return f"Q{quarter} {year}"
    if year is not None and quarter is None:
        return f"{year}"
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    tf = (plan or {}).get("timeframe") or {}
    ts = tf.get("start")
    te = tf.get("end")
    if ts and te:
        return f"{ts} to {te}"
    raw = tf.get("raw_text")
    if isinstance(raw, str):
        m = re.search(r"\b(19\d{2}|20\d{2})\b", raw)
        if m:
            return m.group(1)
    return "the requested period"


def infer_kpi_subject_from_query(q: str) -> str:
    ql = (q or "").lower()
    if "close rate" in ql:
        return "close rate"
    if "conversion" in ql:
        return "conversion rate"
    if "total sale" in ql or ("sale" in ql and "value" in ql):
        return "total sale value"
    if "total lead" in ql or "lead count" in ql:
        return "total leads"
    if "kpi" in ql or "metric" in ql or "metrics" in ql:
        return "KPI metrics"
    return "performance metrics"


def subject_phrase_precise(query_type: str, input_query: str) -> str:
    qt = (query_type or "").strip()
    if qt == "list_buyer_upsheets":
        return "upsheets"
    if qt == "list_buyer_opportunities":
        return "opportunities"
    if qt == "buyer_quarter_kpis":
        return infer_kpi_subject_from_query(input_query)
    return "query results"


def subject_phrase_semantic(plan: Dict[str, Any], input_query: str) -> str:
    rp = plan.get("retrieval_plan") or {}
    family = str(rp.get("query_family") or "").strip()
    intent = str(plan.get("intent") or "").strip()
    q = (input_query or "").lower()
    if family == "list_buyer_upsheets":
        return "upsheets"
    if family == "list_buyer_opportunities" or intent == "buyer_opportunity_listing":
        return "opportunities"
    if family == "buyer_quarter_kpis":
        return infer_kpi_subject_from_query(input_query)
    if family == "buyer_performance_summary":
        if "trend" in q:
            return "performance trend"
        if "overall" in q or "doing" in q:
            return "overall performance"
        return "performance summary"
    if "trend" in q:
        return "performance trend"
    return "performance summary"


def standard_report_line(buyer_label: str, subject_phrase: str, period_phrase: str) -> str:
    """Shared headline grammar for precise listings and semantic summaries."""
    return f"{buyer_label} {subject_phrase} for {period_phrase}."


def render_precise(plan: Dict[str, Any], handler_output: Dict[str, Any]) -> Dict[str, Any]:
    params = handler_output.get("params", {}) or {}
    result = handler_output.get("result", {}) or {}
    query_type = handler_output.get("query_type", "")

    buyer_id = params.get("buyer_id")
    input_query = (handler_output.get("input_query") or "").lower().strip()
    entity = plan.get("entity") or {}

    period_phrase = period_phrase_for_report(params, plan)
    buyer_label = buyer_label_for_report(buyer_id, "", entity)
    subject = subject_phrase_precise(query_type, input_query)
    request_summary = standard_report_line(buyer_label, subject, period_phrase)

    kpi_snapshot: Dict[str, Any]
    supporting_details: Dict[str, Any]

    if query_type == "buyer_quarter_kpis":
        kpi_snapshot = {
            "close_rate": format_pct(result.get("close_rate")),
            "lead_to_opportunity_conversion_rate": format_pct(
                result.get("lead_to_opportunity_conversion_rate")
            ),
            "total_sale_value": format_amount(result.get("total_sale_value")),
        }
        supporting_details = {
            "opportunity_upsheets": result.get("opportunity_upsheets"),
            "delivered_opportunity_upsheets": result.get(
                "delivered_opportunity_upsheets"
            ),
            "total_leads": result.get("total_leads"),
            "total_upsheets": result.get("total_upsheets"),
            "total_opportunities": result.get("total_opportunities"),
        }
    elif query_type == "list_buyer_upsheets":
        rows = result.get("rows", []) or []
        kpi_snapshot = {
            "row_count": result.get("row_count", 0),
        }
        supporting_details = {
            "first_rows_preview": rows[:3],
        }
    elif query_type == "list_buyer_opportunities":
        rows = result.get("rows", []) or []
        row_count = result.get("row_count", len(rows))
        kpi_snapshot = {
            "total_opportunities": row_count,
        }
        supporting_details = {
            "first_rows_preview": rows[:3],
        }
    else:
        # Safe fallback for any future precise handler.
        kpi_snapshot = {"result_type": query_type or "precise"}
        supporting_details = {"raw_result": result}

    notes: List[str] = []
    if query_type == "buyer_quarter_kpis":
        oppty_base = parse_num(result.get("opportunity_upsheets"))
        close_rate = parse_num(result.get("close_rate"))
        if oppty_base == 0 and close_rate is None:
            notes.append(
                "Close rate is N/A because opportunity_upsheets is zero in this period."
            )
    if query_type == "list_buyer_opportunities":
        notes.append(
            "Rows filtered by opportunities.created_at (not upsheet insert date)."
        )
    provenance = handler_output.get("provenance", {}) or {}
    database = provenance.get("database")
    if database:
        notes.append(f"Source database: {database}")
    notes_obj = handler_output.get("notes", {}) or {}
    safety_note = notes_obj.get("safety")
    if isinstance(safety_note, str) and safety_note:
        notes.append(safety_note)

    return {
        "mode": "precise",
        "request_summary": request_summary,
        "kpi_snapshot": kpi_snapshot,
        "supporting_details": supporting_details,
        "data_coverage_notes": notes,
        "suggested_next_question": (
            "Would you like a Postgres row listing for the same buyer and period? "
            "Reply yes to list upsheets, or ask to list opportunities for the same quarter."
        ),
    }


def render_semantic(plan: Dict[str, Any], handler_output: Dict[str, Any]) -> Dict[str, Any]:
    params = handler_output.get("params", {}) or {}
    result = handler_output.get("result", {}) or {}
    matches = result.get("matches", []) or []
    best = matches[0] if matches else {}

    buyer_id = params.get("buyer_id")
    input_query = handler_output.get("input_query") or ""
    buyer_name = (best.get("buyer_name") or "").strip()
    entity = plan.get("entity") or {}

    period_phrase = period_phrase_for_report(params, plan)
    buyer_label = buyer_label_for_report(buyer_id, buyer_name, entity)
    subject = subject_phrase_semantic(plan, input_query)
    request_summary = standard_report_line(buyer_label, subject, period_phrase)

    period_year = params.get("period_year")
    period_quarter = params.get("period_quarter")
    period_label = (
        f"Q{period_quarter} {period_year}"
        if period_year is not None and period_quarter is not None
        else period_phrase
    )

    quality = evaluate_semantic_quality(plan, handler_output)
    semantic_quality_payload = {
        "confidence_level": quality.confidence_level,
        "render_mode": quality.render_mode,
        "reasons": quality.reasons,
        "metadata_aligned": quality.metadata_aligned,
    }

    next_q = (
        "Would you like a Postgres row listing for the same buyer and period? "
        "Reply yes to list upsheets, or ask to list opportunities for the same quarter."
    )
    base_response: Dict[str, Any] = {
        "mode": "semantic",
        "request_summary": request_summary,
        "suggested_next_question": next_q,
        "semantic_quality": semantic_quality_payload,
    }

    raw_snippet = (best.get("summary_snippet") or "").strip()
    best_period_label = best.get("period_label")
    score = best.get("score")

    def _score_highlights() -> List[str]:
        hl: List[str] = []
        if buyer_name and best_period_label:
            hl.append(f"Top matched document: {buyer_name} - {best_period_label}.")
        if score is not None:
            try:
                hl.append(f"Top similarity score: {float(score):.4f}.")
            except (TypeError, ValueError):
                pass
        return hl

    strong_confidence_note = (
        "Performance and KPI narrative here comes from semantic retrieval (precomputed quarterly summaries). "
        "The SQL / precise toggle is for raw Postgres row listings (upsheets, opportunities), not this summary."
    )
    weak_confidence_note = (
        "Semantic retrieval produced limited evidence for this answer. "
        "The SQL / precise toggle is for raw Postgres row listings (upsheets, opportunities)."
    )
    none_confidence_note = (
        "No precomputed quarterly summary passed quality checks for this request. "
        "Use SQL / precise for raw row listings when appropriate."
    )

    if quality.render_mode == "no_semantic_summary":
        highlights_none: List[str] = []
        # Even when failing closed, show what the top match was for debugging/trust.
        highlights_none.extend(_score_highlights())
        return {
            **base_response,
            "executive_summary": _no_semantic_summary_with_hints(
                buyer_label=buyer_label,
                buyer_id=buyer_id,
                requested_period_label=period_label,
                best_period_label=best_period_label,
                reasons=quality.reasons,
            ),
            "trend_narrative": "",
            "key_drivers": [],
            "highlights": highlights_none,
            "retrieval_status": (
                "No reliable semantic summary was retrieved at the current quality thresholds."
            ),
            "available_evidence": [],
            "confidence_note": none_confidence_note,
        }

    if quality.render_mode == "weak_semantic":
        highlights_w = _score_highlights()
        if raw_snippet and raw_snippet.lower() != "no semantic summary found.":
            executive = f"{_RETRIEVAL_WEAK_STATUS} Retrieved excerpt: {raw_snippet}"
        else:
            executive = (
                f"{_RETRIEVAL_WEAK_STATUS} No summary text was available on the top vector match."
            )
        return {
            **base_response,
            "executive_summary": executive,
            "trend_narrative": "",
            "key_drivers": [],
            "highlights": highlights_w,
            "available_evidence": list(highlights_w),
            "confidence_note": weak_confidence_note,
        }

    executive_summary = raw_snippet
    highlights_f = _score_highlights()
    trend_narrative = (
        f"The retrieved quarterly summary for {buyer_label} in {period_label} indicates: "
        f"{executive_summary}"
    )

    return {
        **base_response,
        "executive_summary": executive_summary,
        "trend_narrative": trend_narrative,
        "key_drivers": [],
        "highlights": highlights_f,
        "confidence_note": strong_confidence_note,
    }


def render_answer(plan: Dict[str, Any], handler_output: Dict[str, Any]) -> Dict[str, Any]:
    mode = (plan.get("mode") or "").strip().lower()
    if mode == "precise":
        return render_precise(plan, handler_output)
    return render_semantic(plan, handler_output)


def render_from_combined_payload(combined: Dict[str, Any]) -> Dict[str, Any]:
    blocked = combined.get("force_precise_unavailable_reason")
    if isinstance(blocked, str) and blocked.strip():
        plan = combined.get("execution_plan", {}) or {}
        final_response = {
            "mode": "force_precise_unavailable",
            "request_summary": "Use SQL / precise is not available for this question.",
            "executive_summary": blocked.strip(),
            "trend_narrative": "",
            "highlights": [],
            "suggested_next_question": (
                "Turn off Use SQL / precise data to get KPI or performance answers (semantic). "
                "With the toggle on, ask for raw rows, e.g. 'List upsheets for Buyer N in Q1 2026' "
                "or 'List opportunities for Buyer N in Q1 2026'."
            ),
        }
        return {
            "execution_plan": plan,
            "selected_handler": None,
            "final_response": final_response,
        }

    plan = combined.get("execution_plan", {}) or {}
    handler_output = combined.get("handler_output", {}) or {}
    final_response = render_answer(plan, handler_output)
    return {
        "execution_plan": plan,
        "selected_handler": combined.get("selected_handler"),
        "final_response": final_response,
    }


def load_json_file(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(
        description=(
            "Render deterministic user-facing answers from execution_plan + handler_output."
        )
    )
    parser.add_argument(
        "--input-json",
        type=str,
        default="",
        help="Path to JSON produced by execute_query_v1.py.",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="",
        help="Optional natural-language query. If provided, this script runs execute_query_v1.py first.",
    )
    parser.add_argument(
        "--force-precise",
        action="store_true",
        help="Forward to execute_query_v1: require SQL path when possible (see chat UI toggle).",
    )
    args = parser.parse_args()

    if bool(args.input_json) == bool(args.query):
        raise SystemExit("Provide exactly one of --input-json or --query.")

    if args.query:
        combined = run_execute_query(args.query.strip(), force_precise=args.force_precise)
    else:
        input_path = Path(args.input_json)
        if not input_path.exists():
            raise SystemExit(f"Input JSON file not found: {input_path}")
        combined = load_json_file(input_path)

    rendered = render_from_combined_payload(combined)
    print(json.dumps(rendered, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
