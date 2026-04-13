"""
Saved-report normalization (v2): convert typed block outputs into a single final_response
payload with mode="saved_report".

The goal is to make the executor + normalizer the single source of truth for section
content. The UI renderer should be format-only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional


BlockType = Literal["executive_summary", "kpi_table", "row_listing", "notes", "highlights"]


@dataclass(frozen=True)
class BlockOutput:
    block_id: str
    block_type: BlockType
    output_key: str
    source: str  # semantic | precise | composition | ui
    payload: Dict[str, Any]


def normalize_saved_report_v2(
    *,
    user_query: str,
    template_id: str,
    display_name: str,
    buyer_id: int,
    period_label: str,
    section_order: List[str],
    block_outputs: List[BlockOutput],
    semantic_quality: Optional[Dict[str, Any]] = None,
    suggested_next_question: str = "",
) -> Dict[str, Any]:
    # Collect section content from typed outputs.
    exec_summary_parts: List[str] = []
    highlights: List[str] = []
    notes: List[str] = []
    kpi_snapshot: Dict[str, Any] = {}

    for bo in block_outputs:
        if bo.block_type == "executive_summary":
            txt = str(bo.payload.get("text") or "").strip()
            if txt:
                exec_summary_parts.append(txt)
        elif bo.block_type == "highlights":
            for h in (bo.payload.get("items") or []):
                s = str(h).strip()
                if s:
                    highlights.append(s)
        elif bo.block_type == "notes":
            for n in (bo.payload.get("items") or []):
                s = str(n).strip()
                if s:
                    notes.append(s)
        elif bo.block_type == "kpi_table":
            snap = bo.payload.get("snapshot") or {}
            if isinstance(snap, dict):
                for k, v in snap.items():
                    kpi_snapshot[str(k)] = v
            # Optional summary line for KPI section (kept in snapshot for current UI mapping)
            ksum = str(bo.payload.get("kpi_summary") or "").strip()
            if ksum:
                kpi_snapshot.setdefault("kpi_summary", ksum)
        elif bo.block_type == "row_listing":
            # Row listings can contribute KPI-ish counters (e.g., row_count).
            snap = bo.payload.get("kpi_snapshot") or {}
            if isinstance(snap, dict):
                for k, v in snap.items():
                    kpi_snapshot[str(k)] = v
            for n in (bo.payload.get("notes") or []):
                s = str(n).strip()
                if s:
                    notes.append(s)

    # De-dupe while preserving order.
    def _dedupe(items: List[str]) -> List[str]:
        seen: set[str] = set()
        out: List[str] = []
        for s in items:
            if s not in seen:
                seen.add(s)
                out.append(s)
        return out

    highlights = _dedupe(highlights)
    notes = _dedupe(notes)

    # Help users interpret an all-zero precise snapshot (common in seeded data).
    # This is not "no result"—it's a precise result that indicates no activity in-window.
    def _as_num(x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            if isinstance(x, str):
                xs = x.strip()
                if xs == "":
                    return None
                return float(xs)
            return float(x)
        except Exception:
            return None

    if kpi_snapshot:
        lead_n = _as_num(kpi_snapshot.get("total_leads"))
        up_n = _as_num(kpi_snapshot.get("total_upsheets"))
        opp_n = _as_num(kpi_snapshot.get("total_opportunities"))
        exp_amt = _as_num(kpi_snapshot.get("total_expected_amount"))
        sale_val = _as_num(kpi_snapshot.get("total_sale_value"))
        if (
            lead_n == 0
            and up_n == 0
            and opp_n == 0
            and (exp_amt == 0 or exp_amt is None)
            and (sale_val == 0 or sale_val is None)
        ):
            notes.append(
                "Precise KPIs show no recorded leads/upsheets/opportunities for this buyer in the requested date window."
            )
            notes = _dedupe(notes)

    final_response: Dict[str, Any] = {
        "mode": "saved_report",
        "template_id": template_id,
        "request_summary": (
            f"{display_name}: Buyer {buyer_id}, {period_label}. "
            f"Original request: {user_query.strip()}"
        ),
        "executive_summary": "\n\n".join(exec_summary_parts).strip(),
        "kpi_snapshot": kpi_snapshot,
        "highlights": highlights,
        "notes": notes,
        "suggested_next_question": suggested_next_question.strip(),
        "semantic_quality": semantic_quality,
        # Keep the template’s declared section order visible for dev/debug.
        "section_order": list(section_order),
    }
    return final_response

