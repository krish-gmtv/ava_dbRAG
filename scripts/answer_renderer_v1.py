import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)
SCRIPTS_DIR = Path(__file__).resolve().parent


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def run_execute_query(query: str) -> Dict[str, Any]:
    """
    Run execute_query_v1.py and parse its combined JSON output.
    """
    script_path = SCRIPTS_DIR / "execute_query_v1.py"
    cmd = [sys.executable, str(script_path), "--query", query]
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


def render_precise(plan: Dict[str, Any], handler_output: Dict[str, Any]) -> Dict[str, Any]:
    params = handler_output.get("params", {}) or {}
    result = handler_output.get("result", {}) or {}
    query_type = handler_output.get("query_type", "")

    buyer_id = params.get("buyer_id")
    year = params.get("period_year")
    quarter = params.get("period_quarter")
    start_date = params.get("start_date") or params.get("period_start")
    end_date = params.get("end_date") or params.get("period_end")
    input_query = (handler_output.get("input_query") or "").lower().strip()

    period_label = (
        f"Q{quarter} {year}" if year is not None and quarter is not None else None
    )
    if period_label:
        if "close rate" in input_query and query_type == "buyer_quarter_kpis":
            request_summary = f"Buyer {buyer_id} close rate for {period_label}."
        else:
            request_summary = f"Buyer {buyer_id} performance for {period_label}."
    elif start_date and end_date:
        request_summary = (
            f"Buyer {buyer_id} records for period {start_date} to {end_date}."
        )
    else:
        request_summary = f"Buyer {buyer_id} precise result."

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
        "suggested_next_question": "Would you like a quarter-over-quarter trend for this buyer?",
    }


def render_semantic(plan: Dict[str, Any], handler_output: Dict[str, Any]) -> Dict[str, Any]:
    params = handler_output.get("params", {}) or {}
    result = handler_output.get("result", {}) or {}
    matches = result.get("matches", []) or []
    best = matches[0] if matches else {}

    buyer_id = params.get("buyer_id")
    period_year = params.get("period_year")
    period_quarter = params.get("period_quarter")
    period_label = (
        f"Q{period_quarter} {period_year}"
        if period_year is not None and period_quarter is not None
        else "requested period"
    )
    buyer_name = (best.get("buyer_name") or "").strip()
    if buyer_name:
        request_summary = (
            f"Semantic performance summary for {buyer_name} in {period_label}."
        )
    elif buyer_id is not None:
        request_summary = (
            f"Semantic performance summary for Buyer {buyer_id} in {period_label}."
        )
    else:
        request_summary = f"Semantic performance summary for the requested buyer in {period_label}."

    executive_summary = best.get("summary_snippet") or "No semantic summary found."
    best_period_label = best.get("period_label")
    score = best.get("score")

    highlights: List[str] = []
    if buyer_name and best_period_label:
        highlights.append(
            f"Top matched document: {buyer_name} - {best_period_label}."
        )
    if score is not None:
        try:
            highlights.append(f"Top similarity score: {float(score):.4f}.")
        except (TypeError, ValueError):
            pass

    # Deterministic, non-numeric narrative paraphrase:
    # trend_narrative should not be identical to executive_summary.
    # We keep it grounded by referencing the retrieved snippet verbatim.
    trend_narrative = f"The retrieved quarterly summary for {buyer_name or ('Buyer ' + str(buyer_id) if buyer_id is not None else 'the requested buyer')} in {period_label} indicates: {executive_summary}"

    # Safe MVP: semantic handler currently provides only a summary snippet,
    # so we do not fabricate driver-level bullet points.
    key_drivers: List[str] = [
        "Driver-level details are not included in this semantic result (summary snippet only)."
    ]

    return {
        "mode": "semantic",
        "request_summary": request_summary,
        "executive_summary": executive_summary,
        "trend_narrative": trend_narrative,
        "key_drivers": key_drivers,
        "highlights": highlights,
        "confidence_note": "Narrative is based on semantic retrieval over precomputed quarterly KPI summaries.",
        "suggested_next_question": "Would you like exact KPI values from direct SQL for this same period?",
    }


def render_answer(plan: Dict[str, Any], handler_output: Dict[str, Any]) -> Dict[str, Any]:
    mode = (plan.get("mode") or "").strip().lower()
    if mode == "precise":
        return render_precise(plan, handler_output)
    return render_semantic(plan, handler_output)


def render_from_combined_payload(combined: Dict[str, Any]) -> Dict[str, Any]:
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
    args = parser.parse_args()

    if bool(args.input_json) == bool(args.query):
        raise SystemExit("Provide exactly one of --input-json or --query.")

    if args.query:
        combined = run_execute_query(args.query.strip())
    else:
        input_path = Path(args.input_json)
        if not input_path.exists():
            raise SystemExit(f"Input JSON file not found: {input_path}")
        combined = load_json_file(input_path)

    rendered = render_from_combined_payload(combined)
    print(json.dumps(rendered, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
