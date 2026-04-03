"""
Execute a saved-report plan by running existing answer_renderer → retrieval per data block,
then merge into one ``final_response`` (mode ``saved_report``) for phrasing and UI.
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Any, Dict, List, Optional

from ava_phraser_v1 import get_combined_payload, run_phrasing_for_final_response
from structured_report_v1 import build_structured_report

_DEFAULT_NEXT = (
    "Would you like a Postgres row listing for the same buyer and period? "
    "Reply yes to list upsheets, or ask to list opportunities for the same quarter."
)


def period_label_from_slots(slots: Dict[str, Any]) -> str:
    tf = slots.get("timeframe") or {}
    rt = (tf.get("raw_text") or "").strip()
    if rt:
        return rt
    start = str(tf.get("start") or "").strip()
    if start and re.fullmatch(r"\d{4}-\d{2}-\d{2}", start):
        y = int(start[0:4])
        m = int(start[5:7])
        q = (m - 1) // 3 + 1
        return f"Q{q} {y}"
    return "the requested period"


def _block_queries(buyer_id: int, period_label: str) -> Dict[str, str]:
    return {
        "semantic_quarterly_narrative": (
            f"How did Buyer {buyer_id} perform in {period_label}?"
        ),
        "kpi_snapshot_quarter": (
            f"What were the KPI metrics for Buyer {buyer_id} in {period_label}?"
        ),
        "row_listing_upsheets": (
            f"List all upsheets for Buyer {buyer_id} in {period_label}?"
        ),
    }


def _merge_highlights(*frs: Dict[str, Any]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for fr in frs:
        for h in fr.get("highlights") or []:
            s = str(h).strip()
            if s and s not in seen:
                seen.add(s)
                out.append(s)
    return out


def _merge_notes(*frs: Dict[str, Any]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for fr in frs:
        for key in ("retrieval_status", "confidence_note"):
            s = str(fr.get(key) or "").strip()
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        for n in fr.get("data_coverage_notes") or []:
            s = str(n).strip()
            if s and s not in seen:
                seen.add(s)
                out.append(s)
    return out


def _executive_from_narrative(fr: Dict[str, Any]) -> str:
    parts: List[str] = []
    es = str(fr.get("executive_summary") or "").strip()
    if es:
        parts.append(es)
    tn = str(fr.get("trend_narrative") or "").strip()
    if tn and tn != es:
        parts.append(tn)
    return "\n\n".join(parts).strip()


def execute_saved_report_plan(
    user_query: str,
    plan: Dict[str, Any],
    *,
    use_ava: bool = False,
    strict_validation: bool = False,
    thread_id: str = "",
    app_user_id: str = "",
    force_precise: bool = False,
) -> Dict[str, Any]:
    """
    Run selected template blocks via existing retrieval, merge, phrase. Caller must pass
    a plan with ``ready_to_execute`` true.
    """
    slots = plan.get("slots") or {}
    buyer_id = slots.get("buyer_id")
    if buyer_id is None:
        raise ValueError("saved report plan missing buyer_id")

    period_label = period_label_from_slots(slots)
    queries = _block_queries(int(buyer_id), period_label)

    narrative_fr: Dict[str, Any] = {}
    kpi_fr: Dict[str, Any] = {}
    listing_fr: Optional[Dict[str, Any]] = None

    block_runs: List[Dict[str, Any]] = []
    base_ep: Dict[str, Any] = {}

    for row in plan.get("data_blocks") or []:
        if row.get("status") != "selected":
            continue
        bid = row.get("block_id")
        if bid not in queries:
            continue
        sub_q = queries[bid]
        use_fp = bool(force_precise and bid == "row_listing_upsheets")
        rendered = get_combined_payload("", sub_q, force_precise=use_fp)
        fr = rendered.get("final_response") or {}
        ep = rendered.get("execution_plan") or {}
        if not base_ep and ep:
            base_ep = ep

        block_runs.append(
            {
                "block_id": bid,
                "sub_query": sub_q,
                "force_precise": use_fp,
                "selected_handler": rendered.get("selected_handler"),
                "execution_plan": ep,
            }
        )

        if bid == "semantic_quarterly_narrative":
            narrative_fr = fr
        elif bid == "kpi_snapshot_quarter":
            kpi_fr = fr
        elif bid == "row_listing_upsheets":
            listing_fr = fr

    if not narrative_fr:
        raise RuntimeError("Template executor produced no narrative block output.")

    kpi_snapshot: Dict[str, Any] = {}
    if kpi_fr:
        summary = str(kpi_fr.get("executive_summary") or "").strip()
        if summary:
            kpi_snapshot["kpi_summary"] = summary
    if listing_fr and listing_fr.get("mode") == "precise":
        snap = listing_fr.get("kpi_snapshot") or {}
        if isinstance(snap, dict):
            for k, v in snap.items():
                kpi_snapshot[f"listing_{k}"] = v

    merged_plan: Dict[str, Any] = {
        **base_ep,
        "intent": "saved_report_template",
        "report_template_id": plan.get("template_id"),
        "saved_report_plan": plan,
    }

    final_response: Dict[str, Any] = {
        "mode": "saved_report",
        "template_id": str(plan.get("template_id") or ""),
        "request_summary": (
            f"{plan.get('display_name', 'Saved report')}: Buyer {buyer_id}, {period_label}. "
            f"Original request: {user_query.strip()}"
        ),
        "executive_summary": _executive_from_narrative(narrative_fr),
        "kpi_snapshot": kpi_snapshot,
        "highlights": _merge_highlights(narrative_fr, kpi_fr),
        "notes": _merge_notes(narrative_fr, kpi_fr, listing_fr or {}),
        "suggested_next_question": str(
            narrative_fr.get("suggested_next_question") or ""
        ).strip()
        or _DEFAULT_NEXT,
        "semantic_quality": narrative_fr.get("semantic_quality"),
    }

    ph = run_phrasing_for_final_response(
        final_response,
        use_ava=use_ava,
        strict_validation=strict_validation,
        thread_id=thread_id,
        app_user_id=app_user_id,
    )

    return {
        "execution_plan": merged_plan,
        "selected_handler": f"saved_report_{plan.get('template_id')}",
        "final_response": final_response,
        "structured_report": build_structured_report(final_response),
        "template_block_runs": block_runs,
        "phrasing": {
            "mode": ph["mode"],
            "text": ph["text"],
            "validation": ph["validation"],
            "error": ph["error"],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Execute a saved-report plan JSON (stdin or --plan-json) for debugging."
    )
    parser.add_argument("--query", type=str, required=True, help="Original user query.")
    parser.add_argument(
        "--plan-json",
        type=str,
        default="",
        help="Plan object JSON; default reads from template_report_orchestrator.",
    )
    args = parser.parse_args()
    from template_report_orchestrator_v1 import plan_saved_report

    if args.plan_json.strip():
        plan = json.loads(args.plan_json)
    else:
        plan = plan_saved_report(args.query.strip())
        if not plan:
            print(json.dumps({"error": "no template match"}, indent=2))
            return
    out = execute_saved_report_plan(
        args.query.strip(),
        plan,
        use_ava=False,
        strict_validation=False,
    )
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
