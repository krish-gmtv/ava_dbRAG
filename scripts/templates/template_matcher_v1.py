"""
Rule-based matcher: user message → saved template id + extracted slots.

v1 is intentionally simple (substring triggers). Later you can add an Ava classifier
that only chooses among registered template ids.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from intent_router_v1 import parse_buyer_id, parse_timeframe
from saved_report_templates_v1 import SAVED_REPORT_TEMPLATES, SavedReportTemplate, get_template


def match_saved_report_template(query: str) -> Optional[str]:
    """
    Return the template_id with the strongest trigger match (longest trigger phrase wins).
    """
    q = (query or "").strip().lower()
    if not q:
        return None
    best_id: Optional[str] = None
    best_score = 0
    for tid, tpl in SAVED_REPORT_TEMPLATES.items():
        score = 0
        for phrase in tpl.trigger_phrases:
            if phrase in q:
                score = max(score, len(phrase))
        if score > best_score:
            best_score = score
            best_id = tid
    return best_id if best_score > 0 else None


def extract_template_slots(query: str, template: SavedReportTemplate) -> Dict[str, Any]:
    slots: Dict[str, Any] = {}
    need_buyer = "buyer" in template.required_slots or "buyer" in template.optional_slots
    need_timeframe = (
        "timeframe" in template.required_slots or "timeframe" in template.optional_slots
    )
    if need_buyer:
        slots["buyer_id"] = parse_buyer_id(query)
    if need_timeframe:
        slots["timeframe"] = parse_timeframe(query)
    return slots


def missing_required_slots(template: SavedReportTemplate, slots: Dict[str, Any]) -> List[str]:
    missing: List[str] = []
    if "buyer" in template.required_slots and slots.get("buyer_id") is None:
        missing.append("buyer")
    if "timeframe" in template.required_slots:
        tf = slots.get("timeframe") or {}
        if not tf.get("raw_text") and not tf.get("start"):
            missing.append("timeframe")
    return missing


def explicit_listing_requested(query: str) -> bool:
    q = (query or "").lower()
    return any(
        p in q
        for p in (
            "list upsheet",
            "list all upsheet",
            "upsheets",
            "list opportunity",
            "opportunities for buyer",
            "raw rows",
            "postgres row",
        )
    )
