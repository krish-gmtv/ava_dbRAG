import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from validate_ava_output_v1 import validate_ava_output
from ava_safe_phraser import safe_ws_phrase
from structured_report_v1 import build_structured_report


load_dotenv()
SCRIPTS_DIR = Path(__file__).resolve().parent


def run_json(script_path: Path, args: list[str]) -> Dict[str, Any]:
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


def deterministic_phrase(final_response: Dict[str, Any]) -> str:
    mode = (final_response.get("mode") or "").strip().lower()
    if mode == "force_precise_unavailable":
        summary = final_response.get("request_summary", "")
        detail = final_response.get("executive_summary", "")
        next_q = final_response.get("suggested_next_question", "")
        lines = [str(summary or ""), "", str(detail or "")]
        if next_q:
            lines.extend(["", f"Next: {next_q}"])
        return "\n".join(lines).strip()

    if mode == "precise":
        request_summary = final_response.get("request_summary", "")
        kpi_snapshot = final_response.get("kpi_snapshot", {}) or {}
        notes = final_response.get("data_coverage_notes", []) or []
        next_q = final_response.get("suggested_next_question", "")

        lines = [request_summary, ""]
        lines.append("Key results:")
        for k, v in kpi_snapshot.items():
            lines.append(f"- {k.replace('_', ' ').title()}: {v}")
        if notes:
            lines.append("")
            lines.append("Notes:")
            for n in notes:
                lines.append(f"- {n}")
        if next_q:
            lines.append("")
            lines.append(f"Next: {next_q}")
        return "\n".join(lines).strip()

    # semantic fallback
    request_summary = final_response.get("request_summary", "")
    executive_summary = final_response.get("executive_summary", "")
    trend_narrative = final_response.get("trend_narrative", "")
    highlights = final_response.get("highlights", []) or []
    next_q = final_response.get("suggested_next_question", "")

    lines = [request_summary, "", executive_summary]
    rs = (final_response.get("retrieval_status") or "").strip()
    if rs:
        lines.extend(["", rs])
    if trend_narrative and trend_narrative != executive_summary:
        lines.extend(["", trend_narrative])
    kd = final_response.get("key_drivers") or []
    if kd:
        lines.append("")
        lines.append("Key drivers:")
        for item in kd:
            lines.append(f"- {item}")
    if highlights:
        lines.append("")
        lines.append("Highlights:")
        for h in highlights:
            lines.append(f"- {h}")
    if next_q:
        lines.append("")
        lines.append(f"Next: {next_q}")
    return "\n".join(lines).strip()


def call_ava_phraser(
    final_response: Dict[str, Any],
    app_user_id: str,
    thread_id: str,
    strict_validation: bool,
) -> str:
    """
    Ava websocket-based phrasing call with safety checks.
    If websocket-client or websocket flow fails, caller should catch and fallback.
    """
    return safe_ws_phrase(
        final_response=final_response,
        app_user_id=app_user_id,
        thread_id=thread_id,
        strict_validation=strict_validation,
    )


def get_combined_payload(
    input_json: str,
    query: str,
    force_precise: bool = False,
) -> Dict[str, Any]:
    if bool(input_json) == bool(query):
        raise SystemExit("Provide exactly one of --input-json or --query.")
    if input_json:
        p = Path(input_json)
        if not p.exists():
            raise SystemExit(f"Input JSON file not found: {p}")
        return json.loads(p.read_text(encoding="utf-8"))
    ar_args = ["--query", query]
    if force_precise:
        ar_args.append("--force-precise")
    return run_json(SCRIPTS_DIR / "answer_renderer_v1.py", ar_args)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Controlled Ava phrasing layer. Safe default is deterministic phrasing; "
            "enable Ava call explicitly with --use-ava."
        )
    )
    parser.add_argument("--input-json", type=str, default="")
    parser.add_argument("--query", type=str, default="")
    parser.add_argument("--use-ava", action="store_true")
    parser.add_argument("--strict-validation", action="store_true")
    parser.add_argument(
        "--thread-id",
        type=str,
        default="",
        help="Thread identifier to isolate Ava sessions (optional).",
    )
    parser.add_argument(
        "--app-user-id",
        type=str,
        default="",
        help="Application-level user id (separate from Ava auth credentials).",
    )
    parser.add_argument(
        "--force-precise",
        action="store_true",
        help=(
            "Require direct SQL when supported (user toggle). Does not change Ava phrasing; "
            "only affects retrieval via answer_renderer → execute_query."
        ),
    )
    args = parser.parse_args()

    rendered = get_combined_payload(
        args.input_json,
        args.query.strip(),
        force_precise=args.force_precise,
    )
    final_response = rendered.get("final_response", {}) or {}
    fallback_text = deterministic_phrase(final_response)

    phrasing_mode = "deterministic"
    phrased_text = fallback_text
    ava_error: Optional[str] = None
    thread_id = (
        args.thread_id.strip()
        or os.environ.get("AVA_THREAD_ID", "").strip()
        or "thread-001"
    )
    app_user_id = (
        args.app_user_id.strip()
        or os.environ.get("AVA_APP_USER_ID", "").strip()
        or "app-user-default"
    )

    force_fallback = os.environ.get("AVA_FORCE_FALLBACK", "false").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    ava_enabled = os.environ.get("AVA_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    transport_enabled = os.environ.get("AVA_STREAMING_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
    }

    if args.use_ava and not force_fallback and ava_enabled and transport_enabled:
        try:
            candidate = call_ava_phraser(
                final_response=final_response,
                app_user_id=app_user_id,
                thread_id=thread_id,
                strict_validation=args.strict_validation,
            )
            report = validate_ava_output(
                final_response=final_response,
                phrased_text=candidate,
                strict_headings=args.strict_validation,
            )
            if report["is_valid"]:
                phrased_text = candidate
                phrasing_mode = "ava"
            else:
                phrasing_mode = "deterministic_fallback"
                ava_error = f"Validation failed: {report}"
        except Exception as exc:
            phrasing_mode = "deterministic_fallback"
            ava_error = str(exc)
    elif args.use_ava:
        phrasing_mode = "deterministic_fallback"
        ava_error = (
            "Ava call disabled by environment flags: "
            f"AVA_FORCE_FALLBACK={force_fallback}, "
            f"AVA_ENABLED={ava_enabled}, "
            f"AVA_STREAMING_ENABLED={transport_enabled}"
        )

    validation_report = validate_ava_output(
        final_response=final_response,
        phrased_text=phrased_text,
        strict_headings=args.strict_validation,
    )
    if args.strict_validation and not validation_report["is_valid"]:
        raise SystemExit(f"Validation failed in strict mode: {validation_report}")

    output = {
        "execution_plan": rendered.get("execution_plan"),
        "selected_handler": rendered.get("selected_handler"),
        "final_response": final_response,
        "structured_report": build_structured_report(final_response),
        "phrasing": {
            "mode": phrasing_mode,
            "text": phrased_text,
            "validation": validation_report,
            "error": ava_error,
        },
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

