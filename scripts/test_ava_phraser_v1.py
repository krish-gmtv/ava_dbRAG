import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


SCRIPTS_DIR = Path(__file__).resolve().parent


def run_json(script_path: Path, args: List[str]) -> Dict[str, Any]:
    cmd = [sys.executable, str(script_path), *args]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
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
                },
            },
        )

    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

