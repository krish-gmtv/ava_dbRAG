import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, Optional, Tuple


SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))


def run_json(script_path: str, args: list[str]) -> Dict[str, Any]:
    cmd = [sys.executable, script_path, *args]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{os.path.basename(script_path)} failed.\n"
            f"exit_code={proc.returncode}\n"
            f"stderr={proc.stderr.strip()}\n"
            f"stdout={proc.stdout.strip()}"
        )
    out = proc.stdout.strip()
    if not out:
        raise RuntimeError(f"{os.path.basename(script_path)} returned empty output.")
    return json.loads(out)


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


def assert_keys(obj: Dict[str, Any], keys: set[str], context: str) -> None:
    missing = [k for k in keys if k not in obj]
    if missing:
        raise AssertionError(f"{context}: missing keys: {missing}. Got keys={list(obj.keys())}")


def validate_semantic(final_response: Dict[str, Any]) -> None:
    assert_keys(
        final_response,
        {
            "mode",
            "request_summary",
            "executive_summary",
            "trend_narrative",
            "key_drivers",
            "highlights",
            "confidence_note",
            "suggested_next_question",
            "semantic_quality",
        },
        context="semantic final_response",
    )
    if not isinstance(final_response["key_drivers"], list):
        raise AssertionError("semantic final_response.key_drivers must be a list.")
    if not isinstance(final_response["highlights"], list):
        raise AssertionError("semantic final_response.highlights must be a list.")
    if not isinstance(final_response["executive_summary"], str):
        raise AssertionError("semantic final_response.executive_summary must be a string.")
    if not isinstance(final_response["trend_narrative"], str):
        raise AssertionError("semantic final_response.trend_narrative must be a string.")
    if not isinstance(final_response.get("semantic_quality"), dict):
        raise AssertionError("semantic final_response.semantic_quality must be a dict.")


def validate_precise(
    plan: Dict[str, Any],
    handler_output: Dict[str, Any],
    final_response: Dict[str, Any],
) -> None:
    assert_keys(
        final_response,
        {
            "mode",
            "request_summary",
            "kpi_snapshot",
            "supporting_details",
            "data_coverage_notes",
            "suggested_next_question",
        },
        context="precise final_response",
    )
    if final_response["mode"] != "precise":
        raise AssertionError("precise final_response.mode must be 'precise'.")
    if not isinstance(final_response["kpi_snapshot"], dict):
        raise AssertionError("precise final_response.kpi_snapshot must be a dict.")
    if not isinstance(final_response["supporting_details"], dict):
        raise AssertionError("precise final_response.supporting_details must be a dict.")
    if not isinstance(final_response["data_coverage_notes"], list):
        raise AssertionError("precise final_response.data_coverage_notes must be a list.")

    query_type = handler_output.get("query_type")
    result = handler_output.get("result", {}) or {}

    # Validate KPI fields without hallucination
    if query_type == "buyer_quarter_kpis":
        expected_keys = {
            "close_rate",
            "lead_to_opportunity_conversion_rate",
            "total_sale_value",
        }
        got_keys = set(final_response["kpi_snapshot"].keys())
        if got_keys != expected_keys:
            raise AssertionError(
                f"precise KPI snapshot keys mismatch. expected={sorted(expected_keys)} got={sorted(got_keys)}"
            )

        handler_close_rate = result.get("close_rate")
        handler_l2o = result.get("lead_to_opportunity_conversion_rate")
        handler_total_sale_value = result.get("total_sale_value")

        if format_pct(handler_close_rate) != final_response["kpi_snapshot"]["close_rate"]:
            raise AssertionError(
                "close_rate formatting mismatch (expected based on handler_output.result.close_rate)."
            )
        if format_pct(handler_l2o) != final_response["kpi_snapshot"]["lead_to_opportunity_conversion_rate"]:
            raise AssertionError(
                "lead_to_opportunity_conversion_rate formatting mismatch (expected based on handler_output.result)."
            )
        if format_amount(handler_total_sale_value) != final_response["kpi_snapshot"]["total_sale_value"]:
            raise AssertionError(
                "total_sale_value formatting mismatch (expected based on handler_output.result)."
            )

    elif query_type == "list_buyer_upsheets":
        if "row_count" not in final_response["kpi_snapshot"]:
            raise AssertionError("list_buyer_upsheets: expected row_count in kpi_snapshot.")
        if final_response["kpi_snapshot"]["row_count"] != result.get("row_count"):
            raise AssertionError("list_buyer_upsheets: row_count mismatch vs handler_output.result.row_count.")
        sd = final_response["supporting_details"]
        if "first_rows_preview" not in sd:
            raise AssertionError("list_buyer_upsheets: expected supporting_details.first_rows_preview.")
    else:
        # Safe fallback for future handlers
        pass

    # Ensure request summary exists
    if not isinstance(final_response["request_summary"], str) or not final_response["request_summary"]:
        raise AssertionError("precise final_response.request_summary must be a non-empty string.")


def validate(plan: Dict[str, Any], handler_output: Dict[str, Any], final_response: Dict[str, Any]) -> None:
    if plan.get("mode") != final_response.get("mode"):
        raise AssertionError(
            f"mode mismatch: plan.mode={plan.get('mode')} final_response.mode={final_response.get('mode')}"
        )
    if final_response["mode"] == "semantic":
        validate_semantic(final_response)
    elif final_response["mode"] == "precise":
        validate_precise(plan, handler_output, final_response)
    else:
        raise AssertionError(f"Unexpected mode: {final_response.get('mode')}")


def run_one_query_case(query: str, sleep_seconds: float) -> Tuple[bool, str]:
    execute_script = os.path.join(SCRIPTS_DIR, "execute_query_v1.py")
    renderer_script = os.path.join(SCRIPTS_DIR, "answer_renderer_v1.py")

    combined = run_json(execute_script, ["--query", query])

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        tmp.write(json.dumps(combined))
        tmp_path = tmp.name

    try:
        rendered = run_json(renderer_script, ["--input-json", tmp_path])
        final_response = rendered.get("final_response", {}) or {}
        plan = rendered.get("execution_plan", {}) or combined.get("execution_plan", {}) or {}
        handler_output = combined.get("handler_output", {}) or {}

        validate(plan, handler_output, final_response)
        return True, "ok"
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def looks_like_missing_dependency(err: str) -> bool:
    e = err.lower()
    return (
        "no module named 'psycopg2'" in e
        or "pinecone client library is not installed" in e
        or "no module named 'pinecone'" in e
    )


def describe_missing_dependency(err: str) -> str:
    e = err.lower()
    if "no module named 'psycopg2'" in e:
        return "Missing Python dependency: psycopg2"
    if "pinecone client library is not installed" in e:
        return "Missing Python dependency: pinecone"
    if "no module named 'pinecone'" in e:
        return "Missing Python dependency: pinecone"
    return "Missing dependency (unrecognized)."


def main() -> None:
    parser = argparse.ArgumentParser(description="Validation tests for answer_renderer_v1.")
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    parser.add_argument("--max-tests", type=int, default=12)
    parser.add_argument("--skip-precise", action="store_true")
    parser.add_argument("--skip-semantic", action="store_true")
    args = parser.parse_args()

    # Keep the suite small but meaningful.
    # Expected modes inferred from your router + handlers:
    tests = [
        # Semantic
        ("How did Buyer 1 perform in Q1 2018?", "semantic"),
        ("How did Buyer 2 perform in Q1 2018?", "semantic"),
        ("What is Buyer 1's trend this year?", "semantic"),
        ("How is Buyer 1 doing overall?", "semantic"),
        ("How is Buyer 3 doing overall?", "semantic"),
        ("How did Buyer 3 perform in Q2 2018?", "semantic"),
        # Precise
        ("What was Buyer 2's close rate in Q1 2018?", "precise"),  # edge: close_rate null -> N/A
        ("What was Buyer 1's close rate in Q1 2018?", "precise"),
        ("List all upsheets for Buyer 2 in Q1 2018", "precise"),
        ("List all upsheets for Buyer 1 between 2018-01-01 to 2018-03-31", "precise"),
    ]

    total = min(args.max_tests, len(tests))
    passed = 0
    skipped = 0

    for i in range(total):
        q, expected_mode = tests[i]
        if expected_mode == "precise" and args.skip_precise:
            continue
        if expected_mode == "semantic" and args.skip_semantic:
            continue

        print(f"[{i+1}/{total}] Testing: {q}")
        try:
            ok, msg = run_one_query_case(q, args.sleep_seconds)
            if ok:
                passed += 1
                print(f"  PASS: {msg}")
            else:
                print(f"  FAIL: {msg}")
        except Exception as exc:
            err_str = str(exc)
            if looks_like_missing_dependency(err_str):
                skipped += 1
                print(f"  SKIP (missing dependency): {describe_missing_dependency(err_str)}")
                continue
            print(f"  FAIL: {exc}")

    print(f"Summary: passed={passed}/{total} skipped={skipped}")
    if passed + skipped != total:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

