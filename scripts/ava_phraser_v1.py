import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

from validate_ava_output_v1 import validate_ava_output


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
    if trend_narrative and trend_narrative != executive_summary:
        lines.extend(["", trend_narrative])
    if highlights:
        lines.append("")
        lines.append("Highlights:")
        for h in highlights:
            lines.append(f"- {h}")
    if next_q:
        lines.append("")
        lines.append(f"Next: {next_q}")
    return "\n".join(lines).strip()


def build_ava_prompt(final_response: Dict[str, Any]) -> str:
    return (
        "You are a response phrasing layer. Rewrite the provided structured response into concise, natural language.\n"
        "Hard constraints:\n"
        "1) Do not change numbers or percentages.\n"
        "2) Do not invent metrics.\n"
        "3) Preserve null/NA meaning.\n"
        "4) Keep answer brief and user-facing.\n\n"
        f"Structured response JSON:\n{json.dumps(final_response, ensure_ascii=False)}"
    )


def call_ava_phraser(final_response: Dict[str, Any]) -> str:
    """
    Optional Ava phrasing call.
    Endpoint is intentionally configurable and OFF by default.
    """
    url = os.environ.get("AVA_PHRASER_URL", "").strip()
    token = os.environ.get("AVA_TOKEN", "").strip()
    if not url:
        raise RuntimeError("AVA_PHRASER_URL is not set.")
    if not token:
        raise RuntimeError("AVA_TOKEN is not set.")

    prompt = build_ava_prompt(final_response)
    resp = requests.post(
        url,
        headers={
            "Authorization": token,
            "Content-Type": "application/json",
        },
        json={
            "prompt": prompt,
            "temperature": 0,
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    # Flexible parsing to support different response shapes.
    if isinstance(data, dict):
        if isinstance(data.get("text"), str):
            return data["text"].strip()
        if isinstance(data.get("response"), str):
            return data["response"].strip()
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            c0 = choices[0]
            if isinstance(c0, dict):
                msg = c0.get("message")
                if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                    return msg["content"].strip()
                if isinstance(c0.get("text"), str):
                    return c0["text"].strip()
    raise RuntimeError("Unable to parse Ava phrasing response.")


def get_combined_payload(input_json: str, query: str) -> Dict[str, Any]:
    if bool(input_json) == bool(query):
        raise SystemExit("Provide exactly one of --input-json or --query.")
    if input_json:
        p = Path(input_json)
        if not p.exists():
            raise SystemExit(f"Input JSON file not found: {p}")
        return json.loads(p.read_text(encoding="utf-8"))
    return run_json(SCRIPTS_DIR / "answer_renderer_v1.py", ["--query", query])


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
    args = parser.parse_args()

    rendered = get_combined_payload(args.input_json, args.query.strip())
    final_response = rendered.get("final_response", {}) or {}
    fallback_text = deterministic_phrase(final_response)

    phrasing_mode = "deterministic"
    phrased_text = fallback_text
    ava_error: Optional[str] = None

    if args.use_ava:
        try:
            candidate = call_ava_phraser(final_response)
            report = validate_ava_output(final_response, candidate)
            if report["is_valid"]:
                phrased_text = candidate
                phrasing_mode = "ava"
            else:
                phrasing_mode = "deterministic_fallback"
                ava_error = f"Validation failed: {report}"
        except Exception as exc:
            phrasing_mode = "deterministic_fallback"
            ava_error = str(exc)

    validation_report = validate_ava_output(final_response, phrased_text)
    if args.strict_validation and not validation_report["is_valid"]:
        raise SystemExit(f"Validation failed in strict mode: {validation_report}")

    output = {
        "execution_plan": rendered.get("execution_plan"),
        "selected_handler": rendered.get("selected_handler"),
        "final_response": final_response,
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

