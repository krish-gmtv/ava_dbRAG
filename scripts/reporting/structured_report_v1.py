"""
Deterministic structured report: maps ``final_response`` into fixed report shapes for UI
rendering. Does not invent phrasing—only copies fields and turns dicts into ordered rows.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

REPORT_SCHEMA_VERSION = "1.0"


def _str(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _optional_str(d: Dict[str, Any], key: str) -> Optional[str]:
    s = _str(d.get(key))
    return s if s else None


def _snapshot_to_rows(snapshot: Dict[str, Any]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for key in sorted((snapshot or {}).keys()):
        rows.append({"metric_key": str(key), "value": _str(snapshot.get(key))})
    return rows


def _rows_to_table(rows: Any) -> Dict[str, Any]:
    """
    Convert list-of-dicts into a simple table contract:
      { columns: [...], rows: [[...], ...] }
    """
    if not isinstance(rows, list) or not rows:
        return {"columns": [], "rows": []}
    dict_rows = [r for r in rows if isinstance(r, dict)]
    if not dict_rows:
        return {"columns": [], "rows": []}
    # Deterministic column order: keys from first row, then any new keys alphabetically.
    first_keys = list(dict_rows[0].keys())
    extra_keys = sorted({k for r in dict_rows[1:] for k in r.keys()} - set(first_keys))
    cols = [str(k) for k in first_keys] + [str(k) for k in extra_keys]
    out_rows: List[List[str]] = []
    for r in dict_rows[:25]:
        out_rows.append([_str(r.get(c)) for c in cols])
    return {"columns": cols, "rows": out_rows}


def _preview_tables_to_sections(x: Any) -> List[Dict[str, Any]]:
    if not isinstance(x, list) or not x:
        return []
    out: List[Dict[str, Any]] = []
    for item in x:
        if not isinstance(item, dict):
            continue
        title = _str(item.get("title")) or _str(item.get("block_id")) or "Row preview"
        table = _rows_to_table(item.get("rows"))
        if table.get("columns"):
            out.append({"title": title, "table": table})
    return out


def build_structured_report(final_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map renderer ``final_response`` into a stable report contract for the UI.

    Modes:
    - precise: request_summary, kpi_table (from kpi_snapshot), notes, suggested_next_question
    - semantic: request_summary, executive_summary, trend_narrative?, highlights,
      confidence_note, key_drivers, suggested_next_question
    - force_precise_unavailable: request_summary, executive_summary, suggested_next_question
    """
    mode = _str(final_response.get("mode")).lower() or "unknown"

    if mode == "precise":
        snapshot = final_response.get("kpi_snapshot") or {}
        if not isinstance(snapshot, dict):
            snapshot = {}
        notes = final_response.get("data_coverage_notes") or []
        note_list = [str(n) for n in notes if _str(n)]
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "report_kind": "precise",
            "sections": {
                "request_summary": _optional_str(final_response, "request_summary"),
                "kpi_table": _snapshot_to_rows(snapshot),
                "notes": note_list,
                "suggested_next_question": _optional_str(
                    final_response, "suggested_next_question"
                ),
            },
        }

    if mode == "force_precise_unavailable":
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "report_kind": "force_precise_unavailable",
            "sections": {
                "request_summary": _optional_str(final_response, "request_summary"),
                "executive_summary": _optional_str(final_response, "executive_summary"),
                "suggested_next_question": _optional_str(
                    final_response, "suggested_next_question"
                ),
            },
        }

    if mode == "saved_report":
        ks = final_response.get("kpi_snapshot") or {}
        if not isinstance(ks, dict):
            ks = {}
        kpi_narr = _optional_str(final_response, "kpi_narrative")
        row_preview_tables = _preview_tables_to_sections(final_response.get("row_preview_tables"))
        highlights = final_response.get("highlights") or []
        hl_list = [str(h) for h in highlights if _str(h)]
        notes = final_response.get("notes") or []
        note_list = [str(n) for n in notes if _str(n)]
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "report_kind": "saved_report",
            "sections": {
                "request_summary": _optional_str(final_response, "request_summary"),
                "executive_summary": _optional_str(final_response, "executive_summary"),
                "kpi_narrative": kpi_narr,
                "kpi_table": _snapshot_to_rows(ks),
                "row_preview_tables": row_preview_tables,
                "highlights": hl_list,
                "notes": note_list,
                "suggested_next_question": _optional_str(
                    final_response, "suggested_next_question"
                ),
                "template_id": _optional_str(final_response, "template_id"),
            },
        }

    if mode == "semantic":
        trend = _optional_str(final_response, "trend_narrative")
        exec_s = _optional_str(final_response, "executive_summary")
        if trend and exec_s and trend == exec_s:
            trend = None
        highlights = final_response.get("highlights") or []
        hl_list = [str(h) for h in highlights if _str(h)]
        kd = final_response.get("key_drivers") or []
        kd_list = [str(x) for x in kd if _str(x)]
        ae = final_response.get("available_evidence") or []
        ae_list = [str(x) for x in ae if _str(x)]
        sections: Dict[str, Any] = {
            "request_summary": _optional_str(final_response, "request_summary"),
            "executive_summary": exec_s,
            "trend_narrative": trend,
            "highlights": hl_list,
            "confidence_note": _optional_str(final_response, "confidence_note"),
            "key_drivers": kd_list,
            "suggested_next_question": _optional_str(
                final_response, "suggested_next_question"
            ),
        }
        rs = _optional_str(final_response, "retrieval_status")
        if rs:
            sections["retrieval_status"] = rs
        if ae_list:
            sections["available_evidence"] = ae_list
        sq = final_response.get("semantic_quality")
        if isinstance(sq, dict) and sq:
            sections["semantic_quality"] = sq
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "report_kind": "semantic",
            "sections": sections,
        }

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "report_kind": "unknown",
        "sections": {
            "request_summary": _optional_str(final_response, "request_summary"),
            "executive_summary": _optional_str(final_response, "executive_summary")
            or _optional_str(final_response, "body"),
            "suggested_next_question": _optional_str(
                final_response, "suggested_next_question"
            ),
        },
    }


def build_structured_report_from_ui_fallback(
    *,
    display_text: str,
    source_mode: str,
) -> Dict[str, Any]:
    """Non-retrieval UI responses (guardrail, gratitude, fallback): single body section."""
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "report_kind": "ui_message",
        "sections": {
            "message": _str(display_text),
            "source_mode": _str(source_mode),
        },
    }


def build_developer_diagnostics(pipeline_output: Dict[str, Any]) -> Dict[str, Any]:
    """Structured fields for developer mode only (no synthesis)."""
    ph = (pipeline_output.get("phrasing") or {}) if isinstance(pipeline_output, dict) else {}
    final_response = (
        (pipeline_output.get("final_response") or {})
        if isinstance(pipeline_output, dict)
        else {}
    )
    plan = (
        (pipeline_output.get("execution_plan") or {})
        if isinstance(pipeline_output, dict)
        else {}
    )
    out: Dict[str, Any] = {
        "phrasing_mode": _str(ph.get("mode")),
        "final_response_mode": _str(final_response.get("mode")),
        "selected_handler": pipeline_output.get("selected_handler"),
        "saved_report_runtime_version": pipeline_output.get("saved_report_runtime_version")
        or plan.get("saved_report_runtime_version"),
        "report_template_id": plan.get("report_template_id"),
        "reason_codes": plan.get("reason_codes"),
        "execution_plan": plan,
        "phrasing_validation": ph.get("validation"),
        "phrasing_error": ph.get("error"),
    }
    if isinstance(pipeline_output.get("template_block_runs"), list):
        out["template_block_runs"] = pipeline_output["template_block_runs"]
    if isinstance(pipeline_output.get("template_block_outputs_v2"), list):
        out["template_block_outputs_v2"] = pipeline_output["template_block_outputs_v2"]
    if isinstance(pipeline_output.get("prompt_modules"), dict):
        out["prompt_modules"] = pipeline_output["prompt_modules"]
    sq = final_response.get("semantic_quality")
    if isinstance(sq, dict) and sq:
        out["semantic_quality"] = sq
    return out
