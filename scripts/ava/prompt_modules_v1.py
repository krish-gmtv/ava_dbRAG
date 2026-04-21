"""
Governed prompt module registry (v1).

Each module is a versioned, auditable unit with:
- purpose (what the section is for)
- instructions (what the LLM must do inside the section)
- required_inputs (which typed payload keys must be present to run this module)
- output_contract (a short, textual description of the expected output shape)
- guardrails (do/don't rules)

Modules are addressed by (module_id, version). Templates reference these IDs;
the prompt assembler composes final prompts from the selected modules + typed payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple


@dataclass(frozen=True)
class PromptModule:
    module_id: str
    version: str
    section_key: str  # which saved_report section this module fills
    purpose: str
    instructions: Tuple[str, ...]
    required_inputs: Tuple[str, ...]
    output_contract: str
    guardrails: Tuple[str, ...] = field(default_factory=tuple)


EXECUTIVE_SUMMARY_V1 = PromptModule(
    module_id="executive_summary",
    version="v1",
    section_key="executive_summary",
    purpose=(
        "Explain the buyer’s quarterly performance in 2–5 sentences using only "
        "provided typed inputs (narrative snippet, bands, KPI highlights)."
    ),
    instructions=(
        "Describe the quarter's performance using only the provided inputs.",
        "Do not invent numbers or trends not present in the inputs.",
        "If a narrative snippet is provided, you may lightly rephrase it; otherwise summarize the bands.",
        "Keep to 2–5 sentences; no bullet lists in this section.",
    ),
    required_inputs=("period_label", "buyer_label"),
    output_contract=(
        "Plain prose, 2–5 sentences. No JSON, no headings, no KPI tables."
    ),
    guardrails=(
        "Do not fabricate KPI values.",
        "Do not claim causation unless an input explicitly supports it.",
        "If no reliable semantic narrative is provided, state that plainly and stop.",
    ),
)


KPI_NARRATIVE_V1 = PromptModule(
    module_id="kpi_narrative",
    version="v1",
    section_key="kpi_snapshot",
    purpose=(
        "Explain the KPI snapshot in 1–2 sentences using only the provided KPI "
        "snapshot (precise) and the buyer/period labels."
    ),
    instructions=(
        "Write 1–2 sentences summarizing what stands out in the KPI snapshot.",
        "Only reference fields that exist in data.kpi_snapshot.",
        "If key counts are zero (e.g. total_leads, total_upsheets), state that plainly.",
        "Do not add any numbers that are not present in data.kpi_snapshot.",
    ),
    required_inputs=("buyer_label", "period_label", "kpi_snapshot"),
    output_contract=(
        "Plain prose, 1–2 sentences. No bullet lists. No tables. No headings."
    ),
    guardrails=(
        "Do not compute or derive new metrics.",
        "Do not infer causes; only describe observed values.",
        "Do not change formatting of numbers (keep as provided).",
    ),
)


HIGHLIGHTS_V1 = PromptModule(
    module_id="highlights",
    version="v1",
    section_key="highlights",
    purpose="List short factual highlights derived strictly from provided typed inputs.",
    instructions=(
        "Produce 1–5 concise highlights.",
        "Each highlight must be traceable to a field in the provided payload.",
        "Prefer quantified statements when numeric values are present.",
    ),
    required_inputs=("highlights_items",),
    output_contract=(
        "A short bullet list (1–5 bullets). One fact per bullet. No narrative prose."
    ),
    guardrails=(
        "Do not invent items beyond the provided highlights_items.",
        "Do not re-phrase numbers.",
    ),
)


NOTES_V1 = PromptModule(
    module_id="notes",
    version="v1",
    section_key="notes",
    purpose=(
        "Disclose retrieval/quality caveats and provenance notes strictly as provided."
    ),
    instructions=(
        "Preserve all provided notes verbatim or near-verbatim.",
        "Do not add new caveats or speculative claims.",
    ),
    required_inputs=("notes_items",),
    output_contract=(
        "A short bullet list of provided notes. One note per bullet."
    ),
    guardrails=(
        "Never synthesize a note not present in notes_items.",
    ),
)


PROMPT_MODULES: Dict[str, PromptModule] = {
    EXECUTIVE_SUMMARY_V1.module_id: EXECUTIVE_SUMMARY_V1,
    KPI_NARRATIVE_V1.module_id: KPI_NARRATIVE_V1,
    HIGHLIGHTS_V1.module_id: HIGHLIGHTS_V1,
    NOTES_V1.module_id: NOTES_V1,
}


def get_module(module_id: str) -> PromptModule:
    if module_id not in PROMPT_MODULES:
        raise KeyError(f"Unknown prompt module: {module_id}")
    return PROMPT_MODULES[module_id]


def list_modules() -> Tuple[str, ...]:
    return tuple(sorted(PROMPT_MODULES.keys()))


def module_summary() -> Dict[str, str]:
    """Compact map of module_id -> version for diagnostics/reporting."""
    return {m.module_id: m.version for m in PROMPT_MODULES.values()}
