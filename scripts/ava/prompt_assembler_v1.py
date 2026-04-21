"""
Prompt assembler (v1).

Deterministically composes the final LLM prompt for saved_report runs from:
- the selected prompt modules (governed library)
- typed block outputs (executive_summary text, highlights items, notes items, kpi snapshot)
- the saved report's structured final_response (request summary, template id)

Output:
- ``build_saved_report_prompt_payload`` returns a stable JSON payload that represents
  exactly what the LLM is allowed to see (data contract). This is used for auditing.
- ``build_saved_report_ws_message`` returns the final text message to send to Ava.

Guarantees:
- Given the same inputs, the exact same payload and message are produced.
- No invented fields; modules are referenced by (module_id, version).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from prompt_modules_v1 import PROMPT_MODULES, PromptModule, get_module


SAVED_REPORT_MODULE_SELECTION: Tuple[str, ...] = (
    "executive_summary",
    "kpi_narrative",
    "highlights",
    "notes",
)

ASSEMBLER_VERSION = "v1_2026-04-14"


def _stringify_items(items: Any) -> List[str]:
    if not items:
        return []
    if not isinstance(items, list):
        return []
    out: List[str] = []
    for x in items:
        s = str(x).strip()
        if s:
            out.append(s)
    return out


def _find_block_payload(
    block_outputs: List[Dict[str, Any]], block_type: str
) -> Optional[Dict[str, Any]]:
    for bo in block_outputs or []:
        if not isinstance(bo, dict):
            continue
        if str(bo.get("block_type") or "").strip() == block_type:
            return bo.get("payload") or {}
    return None


def build_typed_payload(
    *,
    final_response: Dict[str, Any],
    block_outputs: List[Dict[str, Any]],
    buyer_label: str,
    period_label: str,
) -> Dict[str, Any]:
    """
    Build the typed data payload that prompt modules are allowed to read.
    Only typed fields are included; no free-form text beyond provided snippets.
    """
    exec_payload = _find_block_payload(block_outputs, "executive_summary") or {}
    highlights_payload = _find_block_payload(block_outputs, "highlights") or {}
    notes_payload = _find_block_payload(block_outputs, "notes") or {}
    kpi_payload = _find_block_payload(block_outputs, "kpi_table") or {}

    kpi_snapshot = {}
    snap = kpi_payload.get("snapshot") if isinstance(kpi_payload, dict) else {}
    if isinstance(snap, dict):
        kpi_snapshot = {str(k): v for k, v in snap.items()}

    return {
        "buyer_label": str(buyer_label or "").strip(),
        "period_label": str(period_label or "").strip(),
        "template_id": str(final_response.get("template_id") or "").strip(),
        "request_summary": str(final_response.get("request_summary") or "").strip(),
        "executive_summary_snippet": str(exec_payload.get("text") or "").strip(),
        "highlights_items": _stringify_items(highlights_payload.get("items")),
        "notes_items": _stringify_items(notes_payload.get("items")),
        "kpi_snapshot": kpi_snapshot,
        "semantic_quality": final_response.get("semantic_quality") or None,
    }


def _module_to_brief(module: PromptModule) -> Dict[str, Any]:
    return {
        "module_id": module.module_id,
        "version": module.version,
        "section_key": module.section_key,
        "purpose": module.purpose,
        "instructions": list(module.instructions),
        "required_inputs": list(module.required_inputs),
        "output_contract": module.output_contract,
        "guardrails": list(module.guardrails),
    }


def select_modules(module_ids: Tuple[str, ...] = SAVED_REPORT_MODULE_SELECTION) -> List[PromptModule]:
    return [get_module(mid) for mid in module_ids]


def build_saved_report_prompt_payload(
    *,
    final_response: Dict[str, Any],
    block_outputs: List[Dict[str, Any]],
    buyer_label: str,
    period_label: str,
    module_ids: Tuple[str, ...] = SAVED_REPORT_MODULE_SELECTION,
) -> Dict[str, Any]:
    """
    Deterministic assembled payload (data contract) sent to the phrasing layer.
    Shape is stable and used for dev diagnostics + the Ava WS message body.
    """
    modules = select_modules(module_ids)
    typed_payload = build_typed_payload(
        final_response=final_response,
        block_outputs=block_outputs,
        buyer_label=buyer_label,
        period_label=period_label,
    )

    module_briefs = [_module_to_brief(m) for m in modules]
    module_versions = {m.module_id: m.version for m in modules}

    return {
        "assembler_version": ASSEMBLER_VERSION,
        "modules_used": module_versions,
        "modules": module_briefs,
        "data": typed_payload,
        "style_guide": {
            "tone": "neutral, manager-facing",
            "allowed_formatting": "plain text, short bullet lists where specified by module",
            "disallowed": [
                "invented metrics",
                "causal claims unsupported by inputs",
                "reordering or renaming the fixed report sections",
            ],
        },
        "hard_constraints": [
            "Do not change numbers or percentages.",
            "Do not invent fields not present in 'data'.",
            "Follow each module's output_contract exactly.",
        ],
    }


def build_saved_report_ws_message(
    *,
    final_response: Dict[str, Any],
    block_outputs: List[Dict[str, Any]],
    buyer_label: str,
    period_label: str,
    module_ids: Tuple[str, ...] = SAVED_REPORT_MODULE_SELECTION,
) -> str:
    """
    Deterministic text message body to send to Ava for saved_report runs.
    Keeps the section contract visible and attaches the assembled payload as JSON.
    """
    payload = build_saved_report_prompt_payload(
        final_response=final_response,
        block_outputs=block_outputs,
        buyer_label=buyer_label,
        period_label=period_label,
        module_ids=module_ids,
    )

    contract_lines = [
        "You are a response phrasing layer governed by versioned prompt modules.",
        "Hard constraints:",
        "1) Do not change numbers or percentages.",
        "2) Do not invent metrics or fields not present in 'data'.",
        "3) Preserve null/NA meaning exactly.",
        "4) Follow each module's output_contract exactly.",
        "5) Output plain text only. No JSON, no code blocks.",
        "",
        "Visible output contract (saved_report):",
        "- Opening request summary line (no heading required)",
        "- 'Executive summary' section (module: executive_summary)",
        "- 'KPI snapshot' section (module: kpi_narrative + bullet list from data.kpi_snapshot)",
        "- 'Highlights' section (module: highlights)",
        "- 'Notes' section (module: notes)",
        "- 'Next:' section",
        "",
        "Assembled prompt payload (source of truth):",
        json.dumps(payload, ensure_ascii=False),
    ]
    return "\n".join(contract_lines)
