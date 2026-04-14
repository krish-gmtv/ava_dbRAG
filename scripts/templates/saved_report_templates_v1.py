"""
Saved report-template registry (v1).

Templates are **not** ad-hoc workflows like email automation; they are fixed **report
_blueprints_**: which slots exist, which data blocks may run, and the **section order**
every time. Conditional behavior (like HubSpot-style branches) is expressed as optional
blocks gated by flags such as ``requires_explicit_user_request``—the executor decides
whether to run them, not Ava.

Ava's role stays constrained: rephrase **within** sections; do not invent sections or
reorder the template layout.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Tuple


@dataclass(frozen=True)
class DataBlockSpec:
    """One allowed retrieval / assembly step the template executor may run."""

    block_id: str
    description: str
    block_type: str
    # How this block is usually satisfied given current product routing.
    retrieval_mode: str  # semantic | precise | composition
    query_family_hint: str
    # Which UI section key this block intends to fill.
    output_key: str
    # Canonical name: source mode for the block's truth. Keep retrieval_mode for backward-compat.
    source_mode: str = ""
    # Optional enforcement hints owned by the template spec.
    runtime_rules: Tuple[str, ...] = ()
    requires_explicit_user_request: bool = False

    def __post_init__(self) -> None:  # type: ignore[override]
        # Ensure source_mode defaults to retrieval_mode if not explicitly set.
        if not self.source_mode:
            object.__setattr__(self, "source_mode", self.retrieval_mode)


@dataclass(frozen=True)
class SavedReportTemplate:
    template_id: str
    display_name: str
    purpose: str
    # Phrases in user text (lowercased match). Longest phrase wins at match time.
    trigger_phrases: Tuple[str, ...]
    required_slots: FrozenSet[str]
    optional_slots: FrozenSet[str]
    # Fixed UI / report section keys, in order. Executor fills each from data blocks.
    section_order: Tuple[str, ...]
    data_blocks: Tuple[DataBlockSpec, ...]
    # Blocks that must never run unless the user clearly asks (e.g. raw listings).
    disallowed_without_explicit_request: FrozenSet[str]
    phrasing_rules: Tuple[str, ...]


BUYER_PERFORMANCE_REPORT_V1 = SavedReportTemplate(
    template_id="buyer_performance_report_v1",
    display_name="Buyer performance report",
    purpose="Manager-facing buyer performance report with fixed structure every time.",
    trigger_phrases=(
        "buyer performance report",
        "buyer summary report",
        "performance report on buyer",
        "buyer report",
        "buyer summary",
        "buyer performance",
    ),
    required_slots=frozenset({"buyer"}),
    optional_slots=frozenset({"timeframe"}),
    section_order=(
        "request_summary",
        "executive_summary",
        "kpi_snapshot",
        "highlights",
        "notes",
        "next_steps",
    ),
    data_blocks=(
        DataBlockSpec(
            block_id="semantic_quarterly_narrative",
            description="Quarterly narrative from semantic retrieval (precomputed summaries).",
            block_type="executive_summary",
            retrieval_mode="semantic",
            query_family_hint="buyer_performance_summary",
            output_key="executive_summary",
        ),
        DataBlockSpec(
            block_id="kpi_snapshot_quarter",
            description="Buyer-quarter KPI table sourced from precise SQL aggregates (not narrative).",
            block_type="kpi_table",
            retrieval_mode="precise",
            query_family_hint="buyer_quarter_kpis",
            output_key="kpi_snapshot",
        ),
        DataBlockSpec(
            block_id="row_listing_upsheets",
            description="Raw Postgres upsheet rows; precise listing path.",
            block_type="row_listing",
            retrieval_mode="precise",
            query_family_hint="list_buyer_upsheets",
            output_key="kpi_snapshot",
            requires_explicit_user_request=True,
        ),
    ),
    disallowed_without_explicit_request=frozenset(
        {"row_listing_upsheets", "row_listing_opportunities"}
    ),
    phrasing_rules=(
        "Keep the same section order and headings as the template on every run.",
        "Ava may rephrase wording inside a section; she must not add, remove, or reorder sections.",
        "If a data block did not run, the executor leaves that section empty or marks it skipped—Ava must not fabricate numbers.",
    ),
)

SAVED_REPORT_TEMPLATES: Dict[str, SavedReportTemplate] = {
    BUYER_PERFORMANCE_REPORT_V1.template_id: BUYER_PERFORMANCE_REPORT_V1,
}


def get_template(template_id: str) -> SavedReportTemplate:
    if template_id not in SAVED_REPORT_TEMPLATES:
        raise KeyError(f"Unknown saved report template: {template_id}")
    return SAVED_REPORT_TEMPLATES[template_id]


def list_template_ids() -> Tuple[str, ...]:
    return tuple(sorted(SAVED_REPORT_TEMPLATES.keys()))
