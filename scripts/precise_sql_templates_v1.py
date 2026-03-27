"""
Natural language → structured slots → parameterized Postgres (fixed SQL templates).

Flow
----
1. ``intent_router_v1`` parses the user query into an **execution plan** (buyer id,
   timeframe: quarter / date range / year hint, query family, mode).
2. This module turns that plan into **CLI arguments** for ``precise_*.py`` scripts.
3. Each ``precise_*.py`` runs **only** hand-written, parameterized SQL — no LLM-generated
   SQL strings.

**Not** routed here: aggregated KPI metrics (those use semantic retrieval / precomputed summaries).
Direct SQL precise paths are **row listings** (upsheets, opportunities).

Adding a new precise feature
----------------------------
1. Add ``scripts/precise_<thing>.py`` with ``--query`` plus explicit flags (e.g. ``--buyer-id``,
   ``--start-date`` / ``--end-date``).
2. Register the handler in ``execute_query_v1.HANDLER_TO_SCRIPT``.
3. If needed, add or extend a ``query_family`` in ``intent_router_v1.classify_query_family``
   / ``build_execution_plan``.
4. Extend ``precise_cli_args_from_plan`` (below) so the router’s normalized timeframe and
   entity feed your handler’s flags.
5. Append a row to ``PRECISE_TEMPLATE_REGISTRY`` for documentation and tooling.

This is the same pattern as structured tools (e.g. flight status): **slots from NL, one SQL shape per tool**.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, TypedDict


class PreciseTemplateEntry(TypedDict):
    handler: str
    script: str
    query_families: List[str]
    sql_summary: str
    slot_keys: List[str]


# Human-readable registry; keep in sync with HANDLER_TO_SCRIPT + precise_*.py SQL.
PRECISE_TEMPLATE_REGISTRY: List[PreciseTemplateEntry] = [
    {
        "handler": "precise_list_buyer_upsheets",
        "script": "precise_list_buyer_upsheets.py",
        "query_families": ["list_buyer_upsheets"],
        "sql_summary": "Row listing of upsheets for a buyer in a date window.",
        "slot_keys": ["buyer_id", "start_date", "end_date", "granularity"],
    },
    {
        "handler": "precise_list_buyer_opportunities",
        "script": "precise_list_buyer_opportunities.py",
        "query_families": ["list_buyer_opportunities"],
        "sql_summary": (
            "Opportunity rows for a buyer; period filter uses opportunities.created_at."
        ),
        "slot_keys": ["buyer_id", "start_date", "end_date", "granularity"],
    },
]


def extract_slots_from_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalized slots extracted from the router execution plan (no subprocess, no SQL).
    Use for logging, tests, and future template dispatch.
    """
    entity = plan.get("entity") or {}
    rp = plan.get("retrieval_plan") or {}
    tf = plan.get("timeframe") or {}

    raw_buyer = entity.get("resolved_id")
    buyer_id: Optional[int] = None
    if isinstance(raw_buyer, int):
        buyer_id = raw_buyer
    elif isinstance(raw_buyer, str) and raw_buyer.strip().isdigit():
        buyer_id = int(raw_buyer.strip())

    start = tf.get("start")
    end = tf.get("end")
    gran = str(tf.get("granularity") or "").strip().lower() or None

    return {
        "query_family": rp.get("query_family"),
        "handler": rp.get("handler"),
        "buyer_id": buyer_id,
        "timeframe": {
            "granularity": gran,
            "start": start if isinstance(start, str) else None,
            "end": end if isinstance(end, str) else None,
            "raw_text": tf.get("raw_text"),
        },
    }


def kpi_range_cli_from_plan(handler: str, plan: Dict[str, Any]) -> List[str]:
    """
    Pass router-normalized calendar windows into precise_get_buyer_quarter_kpis.

    The intent router stores ISO start/end for both ``granularity=quarter`` and
    ``granularity=range``. The KPI script only parses a subset of phrasings from
    the raw query string; forwarding the plan dates keeps SQL aligned with routing
    when wording does not match ``Q1 2026`` or ``between ... and ...`` patterns.
    """
    if handler != "precise_get_buyer_quarter_kpis":
        return []
    tf = plan.get("timeframe") or {}
    gran = str(tf.get("granularity") or "").strip().lower()
    if gran not in ("quarter", "range"):
        return []
    start = tf.get("start")
    end = tf.get("end")
    if isinstance(start, str) and isinstance(end, str):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start) and re.fullmatch(
            r"\d{4}-\d{2}-\d{2}", end
        ):
            return ["--start-date", start, "--end-date", end]
    return []


def precise_cli_args_from_plan(handler: str, plan: Dict[str, Any]) -> List[str]:
    """
    Map execution plan → CLI flags for all ``precise_*`` handlers.

    This is the main bridge from NL-derived structure to Postgres template scripts.
    """
    if not str(handler or "").startswith("precise_"):
        return []
    args: List[str] = []
    entity = plan.get("entity") or {}
    _raw_buyer = entity.get("resolved_id")
    buyer_id: Optional[int] = None
    if isinstance(_raw_buyer, int):
        buyer_id = _raw_buyer
    elif isinstance(_raw_buyer, str) and _raw_buyer.strip().isdigit():
        buyer_id = int(_raw_buyer.strip())
    if buyer_id is not None:
        args.extend(["--buyer-id", str(buyer_id)])

    tf = plan.get("timeframe") or {}
    gran = str(tf.get("granularity") or "").strip().lower()
    start = tf.get("start")
    end = tf.get("end")
    if gran in {"quarter", "range"} and isinstance(start, str) and isinstance(end, str):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start) and re.fullmatch(
            r"\d{4}-\d{2}-\d{2}", end
        ):
            args.extend(["--start-date", start, "--end-date", end])
    return args


def combine_precise_extra_args(handler: str, plan: Dict[str, Any]) -> List[str]:
    """
    Prefer full plan-based CLI args; if those are empty, fall back to KPI-only date forwarding.
    """
    planner_extras = precise_cli_args_from_plan(handler, plan)
    kpi_extras = kpi_range_cli_from_plan(handler, plan)
    return planner_extras if planner_extras else kpi_extras
