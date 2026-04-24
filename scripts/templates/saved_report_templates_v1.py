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

from typing import Any, Dict, Tuple

from template_docs_v1 import load_template_docs_from_dir, resolve_templates_dir
from template_schema_v1 import DataBlockSpec, SavedReportTemplate


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
    prompt_modules=(
        "executive_summary",
        "kpi_narrative",
        "highlights",
        "notes",
    ),
)

SAVED_REPORT_TEMPLATES: Dict[str, SavedReportTemplate] = {
    BUYER_PERFORMANCE_REPORT_V1.template_id: BUYER_PERFORMANCE_REPORT_V1,
}

# Allow JSON template docs to override built-in Python templates.
_json_templates = load_template_docs_from_dir(resolve_templates_dir())
if _json_templates:
    SAVED_REPORT_TEMPLATES.update(_json_templates)


def get_template(template_id: str) -> SavedReportTemplate:
    if template_id not in SAVED_REPORT_TEMPLATES:
        raise KeyError(f"Unknown saved report template: {template_id}")
    return SAVED_REPORT_TEMPLATES[template_id]


def list_template_ids() -> Tuple[str, ...]:
    return tuple(sorted(SAVED_REPORT_TEMPLATES.keys()))
