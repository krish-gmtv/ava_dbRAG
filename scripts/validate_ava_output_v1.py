import argparse
import json
import re
from typing import Any, Dict, List, Set, Tuple


FORBIDDEN_WORDS = {
    "estimated",
    "guess",
    "approximate",
    "probably",
    "might be",
    "i think",
}


def extract_numeric_tokens(text: str) -> Set[str]:
    """
    Extract numeric-looking tokens from text.
    Examples:
    - 100
    - 100.00
    - 100.00%
    """
    tokens = re.findall(r"-?\d+(?:\.\d+)?%?", text)
    return set(tokens)


def collect_allowed_value_tokens(final_response: Dict[str, Any]) -> Set[str]:
    """
    Collect allowed numeric tokens from structured final_response values.
    This helps prevent phrasing from introducing unseen numeric claims.
    """
    tokens: Set[str] = set()

    def walk(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, (int, float)):
            tokens.add(str(value))
            tokens.add(f"{float(value):.2f}")
            return
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return
            # Include embedded numeric tokens from display-formatted strings.
            for t in extract_numeric_tokens(s):
                tokens.add(t)
            return
        if isinstance(value, list):
            for v in value:
                walk(v)
            return
        if isinstance(value, dict):
            for v in value.values():
                walk(v)
            return

    walk(final_response)
    return tokens


def validate_ava_output(
    final_response: Dict[str, Any],
    phrased_text: str,
    strict_headings: bool = False,
) -> Dict[str, Any]:
    """
    Basic safety checks for Ava phrasing output.
    Returns a dict:
    {
      "is_valid": bool,
      "errors": [...],
      "warnings": [...]
    }
    """
    errors: List[str] = []
    warnings: List[str] = []

    if not isinstance(phrased_text, str) or not phrased_text.strip():
        errors.append("Phrased text is empty.")
        return {"is_valid": False, "errors": errors, "warnings": warnings}

    lower_text = phrased_text.lower()
    for w in FORBIDDEN_WORDS:
        if w in lower_text:
            msg = f"Found hedge term '{w}' in phrased text."
            if strict_headings:
                errors.append(msg)
            else:
                warnings.append(msg)

    mode = (final_response.get("mode") or "").strip().lower()
    if mode not in {"precise", "semantic", "force_precise_unavailable"}:
        warnings.append("Unknown final_response mode.")

    if mode == "force_precise_unavailable":
        return {"is_valid": True, "errors": errors, "warnings": warnings}

    # Ensure the request summary still appears conceptually in the output.
    request_summary = str(final_response.get("request_summary") or "").strip()
    if request_summary:
        # Soft check: at least a few words overlap.
        req_words = [w for w in re.findall(r"\w+", request_summary.lower()) if len(w) > 2]
        if req_words:
            overlap = sum(1 for w in req_words if w in lower_text)
            if overlap == 0:
                warnings.append("No overlap detected with request_summary wording.")

    # Numeric guardrail: if phrased text includes numeric tokens,
    # they should already exist in the structured output.
    output_nums = extract_numeric_tokens(phrased_text)
    allowed_nums = collect_allowed_value_tokens(final_response)
    unexpected = sorted(n for n in output_nums if n not in allowed_nums)
    if unexpected:
        msg = f"Found numeric tokens not present in structured output: {unexpected[:10]}"
        if strict_headings:
            errors.append(msg)
        else:
            warnings.append(msg)

    # Stronger check for precise mode: primary metric text should be represented.
    if mode == "precise":
        kpi_snapshot = final_response.get("kpi_snapshot", {}) or {}
        if isinstance(kpi_snapshot, dict):
            primary_values = [str(v) for v in kpi_snapshot.values() if v is not None]
            if primary_values and not any(v in phrased_text for v in primary_values):
                msg = "Precise phrasing does not include any KPI snapshot value verbatim."
                if strict_headings:
                    errors.append(msg)
                else:
                    warnings.append(msg)

    # Optional strict template adherence checks.
    if strict_headings:
        heading_positions: Dict[str, int] = {}
        for h in ("key results:", "notes:", "highlights:", "next:"):
            pos = lower_text.find(h)
            if pos >= 0:
                heading_positions[h] = pos

        if mode == "precise":
            # Deterministic precise phrasing includes these headings.
            if "key results:" not in lower_text:
                errors.append("Missing required heading 'Key results:' for precise mode.")
            if "next:" not in lower_text:
                errors.append("Missing required heading 'Next:' for precise mode.")
            if final_response.get("data_coverage_notes"):
                if "notes:" not in lower_text:
                    errors.append("Missing required heading 'Notes:' for precise mode.")

            # Enforce heading order where headings exist.
            if "key results:" in heading_positions and "next:" in heading_positions:
                if heading_positions["key results:"] > heading_positions["next:"]:
                    errors.append("Heading order invalid for precise mode: 'Key results:' must appear before 'Next:'.")
            if "notes:" in heading_positions and "next:" in heading_positions:
                if heading_positions["notes:"] > heading_positions["next:"]:
                    errors.append("Heading order invalid for precise mode: 'Notes:' must appear before 'Next:'.")

            # Reject semantic-only heading in precise mode.
            if "highlights:" in heading_positions:
                errors.append("Unexpected heading 'Highlights:' in precise mode output.")
        elif mode == "semantic":
            if "next:" not in lower_text:
                errors.append("Missing required heading 'Next:' for semantic mode.")
            highlights = final_response.get("highlights") or []
            if isinstance(highlights, list) and highlights:
                if "highlights:" not in lower_text:
                    errors.append("Missing required heading 'Highlights:' for semantic mode.")
                elif "next:" in heading_positions and heading_positions["highlights:"] > heading_positions["next:"]:
                    errors.append("Heading order invalid for semantic mode: 'Highlights:' must appear before 'Next:'.")

            # Reject precise-only headings in semantic mode.
            if "key results:" in heading_positions:
                errors.append("Unexpected heading 'Key results:' in semantic mode output.")
            if "notes:" in heading_positions:
                errors.append("Unexpected heading 'Notes:' in semantic mode output.")

    return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate Ava phrasing output against structured final_response."
    )
    parser.add_argument("--final-response-json", type=str, required=True)
    parser.add_argument("--phrased-text", type=str, required=True)
    args = parser.parse_args()

    final_response = json.loads(args.final_response_json)
    report = validate_ava_output(final_response, args.phrased_text)
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

