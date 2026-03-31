import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


SCRIPTS_DIR = Path(__file__).resolve().parent


def run_json(script_path: Path, args: List[str], env_overrides: Dict[str, str] | None = None) -> Dict[str, Any]:
    cmd = [sys.executable, str(script_path), *args]
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{script_path.name} failed.\n"
            f"exit_code={proc.returncode}\n"
            f"stderr={proc.stderr.strip()}\n"
            f"stdout={proc.stdout.strip()}"
        )
    out = proc.stdout.strip()
    if not out:
        raise RuntimeError(f"{script_path.name} returned empty output.")
    return json.loads(out)


def assert_has_keys(obj: Dict[str, Any], keys: List[str], context: str) -> None:
    missing = [k for k in keys if k not in obj]
    if missing:
        raise AssertionError(f"{context}: missing keys {missing}")


def validate_phraser_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
    assert_has_keys(payload, ["execution_plan", "selected_handler", "final_response", "phrasing"], "top-level")

    final_response = payload.get("final_response", {}) or {}
    phrasing = payload.get("phrasing", {}) or {}

    assert_has_keys(
        phrasing,
        ["mode", "text", "validation", "error"],
        "phrasing",
    )

    mode = (final_response.get("mode") or "").strip().lower()
    if mode not in {"semantic", "precise"}:
        raise AssertionError(f"Unexpected final_response.mode: {mode}")

    text = phrasing.get("text")
    if not isinstance(text, str) or not text.strip():
        raise AssertionError("phrasing.text must be a non-empty string.")

    validation = phrasing.get("validation", {}) or {}
    assert_has_keys(validation, ["is_valid", "errors", "warnings"], "phrasing.validation")
    if validation.get("is_valid") is not True:
        raise AssertionError(f"phrasing.validation.is_valid must be True. Got {validation}")

    phrasing_mode = phrasing.get("mode")
    if phrasing_mode not in {"deterministic", "ava", "deterministic_fallback"}:
        raise AssertionError(f"Unexpected phrasing.mode: {phrasing_mode}")

    return True, f"mode={mode}, phrasing_mode={phrasing_mode}"


def run_one(query: str, use_ava: bool, strict_validation: bool) -> Dict[str, Any]:
    script = SCRIPTS_DIR / "execute_answer_with_ava_v1.py"
    args = ["--query", query]
    if use_ava:
        args.append("--use-ava")
    if strict_validation:
        args.append("--strict-validation")
    return run_json(script, args)


def build_test_input_json(path: Path, mode: str = "semantic") -> None:
    if mode == "precise":
        payload = {
            "execution_plan": {"mode": "precise"},
            "selected_handler": "precise_get_buyer_quarter_kpis",
            "final_response": {
                "mode": "precise",
                "request_summary": "Buyer 2 close rate for Q1 2018.",
                "kpi_snapshot": {
                    "close_rate": "N/A",
                    "lead_to_opportunity_conversion_rate": "100.00%",
                    "total_sale_value": "0.00",
                },
                "supporting_details": {"opportunity_upsheets": 0},
                "data_coverage_notes": [
                    "Close rate is N/A because opportunity_upsheets is zero in this period."
                ],
                "suggested_next_question": (
                    "Would you like a Postgres row listing for the same buyer and period? "
                    "Reply yes to list upsheets, or ask to list opportunities for the same quarter."
                ),
            },
        }
    else:
        payload = {
            "execution_plan": {"mode": "semantic"},
            "selected_handler": "semantic_buyer_performance_summary",
            "final_response": {
                "mode": "semantic",
                "request_summary": "Buyer 1 performance summary for Q1 2018.",
                "executive_summary": "Buyer1 User1 recorded low workload in 2018 Q1.",
                "trend_narrative": "The retrieved quarterly summary for Buyer 1 in Q1 2018 indicates: Buyer1 User1 recorded low workload in 2018 Q1.",
                "key_drivers": [
                    "Driver-level details are not included in this semantic result (summary snippet only)."
                ],
                "highlights": ["Top matched document: Buyer1 User1 - Q1 2018."],
                "confidence_note": (
                    "Performance and KPI narrative here comes from semantic retrieval (precomputed quarterly summaries). "
                    "The SQL / precise toggle is for raw Postgres row listings (upsheets, opportunities), not this summary."
                ),
                "suggested_next_question": (
                    "Would you like a Postgres row listing for the same buyer and period? "
                    "Reply yes to list upsheets, or ask to list opportunities for the same quarter."
                ),
            },
        }
    path.write_text(json.dumps(payload), encoding="utf-8")


def run_failure_path_tests(log_file: Path | None) -> Tuple[int, int]:
    """
    Returns (passed, failed) for targeted failure-path checks.
    These tests are transport/safety focused and do not depend on retrieval.
    """
    passed = 0
    failed = 0
    tmp = SCRIPTS_DIR / "_tmp_ava_failure_test_input.json"
    build_test_input_json(tmp, mode="semantic")
    phraser = SCRIPTS_DIR / "ava_phraser_v1.py"

    cases = [
        {
            "name": "force_fallback_flag",
            "env": {"AVA_FORCE_FALLBACK": "true"},
            "expect_mode": "deterministic_fallback",
            "expect_error_contains": "Ava call disabled by environment flags",
        },
        {
            "name": "invalid_token_ws_path",
            "env": {"AVA_FORCE_FALLBACK": "false", "AVA_TOKEN": "invalid-token"},
            "expect_mode": "deterministic_fallback",
            "expect_error_any": [
                "WS phrasing failed",
                "Missing dependency websocket-client",
                "401",
                "403",
                "WS receive",
            ],
        },
    ]

    for c in cases:
        event = {
            "event": "failure_path_case",
            "ts": now_iso(),
            "name": c["name"],
            "status": "unknown",
            "details": {},
        }
        try:
            out = run_json(
                phraser,
                ["--input-json", str(tmp), "--use-ava", "--strict-validation"],
                env_overrides=c["env"],
            )
            phrasing = out.get("phrasing", {}) or {}
            mode = phrasing.get("mode")
            err = str(phrasing.get("error") or "")
            if mode != c["expect_mode"]:
                raise AssertionError(
                    f"Expected phrasing.mode={c['expect_mode']} got {mode}"
                )
            expect_contains = c.get("expect_error_contains")
            expect_any = c.get("expect_error_any")
            if expect_contains and expect_contains not in err:
                raise AssertionError(
                    f"Expected error containing '{expect_contains}', got '{err}'"
                )
            if expect_any and not any(x in err for x in expect_any):
                raise AssertionError(
                    f"Expected error containing one of {expect_any}, got '{err}'"
                )
            passed += 1
            event["status"] = "pass"
            event["details"] = {"phrasing_mode": mode, "error": err}
        except Exception as exc:
            failed += 1
            event["status"] = "fail"
            event["details"] = {"message": str(exc)}
        if log_file:
            append_jsonl(log_file, event)

    try:
        tmp.unlink(missing_ok=True)
    except Exception:
        pass

    return passed, failed


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def append_jsonl(log_file: Path, record: Dict[str, Any]) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Safety test suite for Ava phrasing layer.\n"
            "Default runs deterministic-only. Add --use-ava to test Ava calls with fallback."
        )
    )
    parser.add_argument("--use-ava", action="store_true", help="Enable Ava phrasing call.")
    parser.add_argument("--strict-validation", action="store_true", help="Enable strict validation mode.")
    parser.add_argument("--max-tests", type=int, default=10, help="Max queries to run from suite.")
    parser.add_argument("--sleep-seconds", type=float, default=0.3, help="Sleep between tests.")
    parser.add_argument(
        "--log-file",
        type=str,
        default="",
        help="Optional JSONL audit log path (writes one record per test + one summary record).",
    )
    parser.add_argument(
        "--run-failure-paths",
        action="store_true",
        help="Run targeted failure-path tests (force fallback, invalid token).",
    )
    args = parser.parse_args()

    tests: List[str] = [
        "How did Buyer 1 perform in Q1 2018?",
        "How did Buyer 2 perform in Q1 2018?",
        "How is Buyer 1 doing overall?",
        "How is Buyer 3 doing overall?",
        "What is Buyer 1's trend this year?",
        "What was Buyer 2's close rate in Q1 2018?",
        "What was Buyer 1's close rate in Q1 2018?",
        "List all upsheets for Buyer 2 in Q1 2018",
        "List all upsheets for Buyer 1 between 2018-01-01 to 2018-03-31",
        "How did Buyer 3 perform in Q2 2018?",
    ]

    total = min(args.max_tests, len(tests))
    passed = 0
    failed = 0
    fallback_count = 0
    ava_count = 0
    det_count = 0

    print(
        f"Running {total} tests | use_ava={args.use_ava} | strict_validation={args.strict_validation}"
    )
    log_file = Path(args.log_file) if args.log_file else None
    run_started_at = now_iso()

    if log_file:
        append_jsonl(
            log_file,
            {
                "event": "run_started",
                "ts": run_started_at,
                "config": {
                    "use_ava": args.use_ava,
                    "strict_validation": args.strict_validation,
                    "max_tests": args.max_tests,
                    "sleep_seconds": args.sleep_seconds,
                    "total_tests_selected": total,
                },
            },
        )

    for i in range(total):
        q = tests[i]
        print(f"[{i + 1}/{total}] {q}")
        case_started_at = now_iso()
        event: Dict[str, Any] = {
            "event": "test_case",
            "ts": case_started_at,
            "index": i + 1,
            "query": q,
            "status": "unknown",
            "details": {},
        }
        try:
            payload = run_one(q, args.use_ava, args.strict_validation)
            ok, msg = validate_phraser_payload(payload)
            if ok:
                passed += 1
                p_mode = ((payload.get("phrasing") or {}).get("mode") or "").strip()
                if p_mode == "deterministic":
                    det_count += 1
                elif p_mode == "ava":
                    ava_count += 1
                elif p_mode == "deterministic_fallback":
                    fallback_count += 1
                print(f"  PASS: {msg}")
                event["status"] = "pass"
                event["details"] = {
                    "message": msg,
                    "phrasing_mode": p_mode,
                    "selected_handler": payload.get("selected_handler"),
                    "mode": (payload.get("final_response", {}) or {}).get("mode"),
                    "validation": ((payload.get("phrasing") or {}).get("validation") or {}),
                    "error": (payload.get("phrasing") or {}).get("error"),
                }
            else:
                failed += 1
                print("  FAIL: validation returned false")
                event["status"] = "fail"
                event["details"] = {"message": "validation returned false"}
        except Exception as exc:
            failed += 1
            print(f"  FAIL: {exc}")
            event["status"] = "fail"
            event["details"] = {"message": str(exc)}

        if log_file:
            append_jsonl(log_file, event)

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    summary_line = (
        "Summary: "
        f"passed={passed}/{total} failed={failed} "
        f"deterministic={det_count} ava={ava_count} fallback={fallback_count}"
    )
    print(summary_line)

    fp_passed = 0
    fp_failed = 0
    if args.run_failure_paths:
        print("Running failure-path tests...")
        fp_passed, fp_failed = run_failure_path_tests(log_file)
        print(f"Failure-path summary: passed={fp_passed} failed={fp_failed}")

    if log_file:
        append_jsonl(
            log_file,
            {
                "event": "run_finished",
                "ts": now_iso(),
                "summary": {
                    "started_at": run_started_at,
                    "passed": passed,
                    "failed": failed,
                    "total": total,
                    "deterministic": det_count,
                    "ava": ava_count,
                    "fallback": fallback_count,
                    "failure_path_passed": fp_passed,
                    "failure_path_failed": fp_failed,
                },
            },
        )

    if failed > 0 or fp_failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

