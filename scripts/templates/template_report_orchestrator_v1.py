"""
Template-driven report **planning** (v1).

Produces a deterministic JSON plan: template, slots, section order, and which data blocks
are intended to run. Execution is implemented in ``template_executor_v1`` (per-block
``answer_renderer_v1`` calls) and wired from ``chat_ui_server_v1`` when the plan is ready.

Flow:
  user query → match template → fill slots → execute allowed blocks → merge
  ``final_response`` (mode ``saved_report``) → deterministic or Ava phrasing → UI.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from intent_router_v1 import parse_buyer_id, parse_timeframe
from saved_report_templates_v1 import get_template
from template_matcher_v1 import (
    explicit_listing_requested,
    extract_template_slots,
    match_saved_report_template,
    missing_required_slots,
)

SAVED_REPORT_PLAN_CONTRACT_VERSION = "v2_template_contract_2026-04-14"


def _loose_buyer_id_for_forced_template(query: str) -> Optional[int]:
    """
    When a UI template is explicitly selected, allow shorthand like:
      "119 Q2 2021"
    without requiring the word "Buyer".
    """
    q = (query or "").strip()
    if not q:
        return None
    # If the query already includes Buyer <id>, use the canonical parser.
    bid = parse_buyer_id(q)
    if bid is not None:
        return bid
    # Only apply shorthand if it looks like a period is present.
    if not re.search(r"\bq[1-4]\b", q, re.IGNORECASE) and not re.search(
        r"\b(19\d{2}|20\d{2})\b", q
    ):
        return None
    # Extract standalone integers, then subtract obvious timeframe numbers (Qx and YYYY).
    nums = [int(x) for x in re.findall(r"\b(\d{1,4})\b", q)]
    if not nums:
        return None
    qmatch = re.search(r"\bq([1-4])\b", q, re.IGNORECASE)
    if qmatch:
        try:
            qn = int(qmatch.group(1))
            nums = [n for n in nums if n != qn]
        except ValueError:
            pass
    nums = [n for n in nums if not (1900 <= n <= 2099)]
    # If there is exactly one remaining number, treat it as buyer_id.
    if len(nums) != 1:
        return None
    return nums[0]


def _blocks_to_run(
    template_id: str,
    *,
    user_query: str,
) -> List[Dict[str, Any]]:
    tpl = get_template(template_id)
    q = user_query or ""
    want_listing = explicit_listing_requested(q)
    out: List[Dict[str, Any]] = []
    for block in tpl.data_blocks:
        if block.requires_explicit_user_request:
            status = "selected" if want_listing else "skipped_not_requested"
        elif block.block_id in tpl.disallowed_without_explicit_request:
            status = "skipped"
        else:
            status = "selected"
        row = {"status": status, **asdict(block)}
        out.append(row)
    return out


def plan_saved_report(user_query: str) -> Optional[Dict[str, Any]]:
    """
    If the query matches a saved template, return a structured plan (no I/O).
    Otherwise return None so the caller can fall back to legacy query-driven routing.
    """
    q = (user_query or "").strip()
    if not q:
        return None
    tid = match_saved_report_template(q)
    if tid is None:
        # Routing convergence: if the user clearly asks a buyer-quarter performance question,
        # prefer the saved-report template even without explicit "report" phrasing.
        ql = q.lower()
        # Be tolerant to common misspellings and phrasing.
        wants_performance = (
            ("perform" in ql)
            or ("performance" in ql)
            or ("perfomance" in ql)  # common typo: missing 'r'
            or ("performance summary" in ql)
        )
        buyer_id = parse_buyer_id(q)
        tf = parse_timeframe(q) or {}
        has_period = bool(tf.get("raw_text") or tf.get("start") or tf.get("end"))
        if wants_performance and (buyer_id is not None) and has_period:
            tid = "buyer_performance_report_v1"
        else:
            return None
    tpl = get_template(tid)
    slots = extract_template_slots(q, tpl)
    missing = missing_required_slots(tpl, slots)
    return {
        "kind": "saved_report_plan_v1",
        "contract_version": SAVED_REPORT_PLAN_CONTRACT_VERSION,
        "template_id": tid,
        "display_name": tpl.display_name,
        "purpose": tpl.purpose,
        "slots": slots,
        "missing_required_slots": missing,
        "section_order": list(tpl.section_order),
        "phrasing_rules": list(tpl.phrasing_rules),
        "prompt_modules": list(tpl.prompt_modules),
        "data_blocks": _blocks_to_run(tid, user_query=q),
        "ready_to_execute": len(missing) == 0,
    }


def plan_saved_report_for_template(user_query: str, template_id: str) -> Optional[Dict[str, Any]]:
    """
    Force planning against a specific template id (used by UI template picker).

    Returns None only when the user_query is empty.
    """
    q = (user_query or "").strip()
    tid = (template_id or "").strip()
    if not q:
        return None
    if not tid:
        return plan_saved_report(q)

    tpl = get_template(tid)
    slots = extract_template_slots(q, tpl)
    # Forced-template UX: allow minimal "119 Q2 2021" style input.
    if slots.get("buyer_id") is None:
        slots["buyer_id"] = _loose_buyer_id_for_forced_template(q)
    missing = missing_required_slots(tpl, slots)
    return {
        "kind": "saved_report_plan_v1",
        "contract_version": SAVED_REPORT_PLAN_CONTRACT_VERSION,
        "template_id": tid,
        "display_name": tpl.display_name,
        "purpose": tpl.purpose,
        "slots": slots,
        "missing_required_slots": missing,
        "section_order": list(tpl.section_order),
        "phrasing_rules": list(tpl.phrasing_rules),
        "prompt_modules": list(tpl.prompt_modules),
        "data_blocks": _blocks_to_run(tid, user_query=q),
        "ready_to_execute": len(missing) == 0,
        "forced_template": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a saved-report plan JSON for a user query (v1, planning only)."
    )
    parser.add_argument("--query", type=str, required=True)
    args = parser.parse_args()
    plan = plan_saved_report(args.query.strip())
    if plan is None:
        print(json.dumps({"kind": "no_saved_template_match", "query": args.query.strip()}))
        return
    print(json.dumps(plan, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
