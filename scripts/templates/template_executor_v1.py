"""
Execute a saved-report plan by running existing answer_renderer → retrieval per data block,
then merge into one ``final_response`` (mode ``saved_report``) for phrasing and UI.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ava_phraser_v1 import get_combined_payload, run_phrasing_for_final_response
from prompt_assembler_v1 import (
    ASSEMBLER_VERSION,
    SAVED_REPORT_MODULE_SELECTION,
    build_saved_report_prompt_payload,
)
from prompt_modules_v1 import PROMPT_MODULES, module_summary
from report_normalizer_v2 import BlockOutput, normalize_saved_report_v2
from scripts.reporting.structured_report_v1 import build_structured_report

SAVED_REPORT_RUNTIME_VERSION = "v2_typed_blocks_2026-04-09"

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
        "row_listing_upsheets": (
            f"List all upsheets for Buyer {buyer_id} in {period_label}?"
        ),
        "row_listing_opportunities": (
            f"List all opportunities for Buyer {buyer_id} in {period_label}?"
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


def _run_precise_buyer_quarter_kpis(
    *,
    buyer_id: int,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    KPI-source rule: KPIs must come from a declared source, never narrative text.
    Here we satisfy KPI snapshot via the precise KPI script (SQL aggregates).
    """
    scripts_dir = Path(__file__).resolve().parents[1]
    script_path = scripts_dir / "precise_get_buyer_quarter_kpis.py"
    if not script_path.exists():
        raise RuntimeError(f"Missing KPI script: {script_path}")

    # The script requires --query even when overriding with explicit args.
    cmd = [
        sys.executable,
        str(script_path),
        "--query",
        f"Buyer {buyer_id} KPIs between {start_date} and {end_date}",
        "--buyer-id",
        str(int(buyer_id)),
        "--start-date",
        str(start_date),
        "--end-date",
        str(end_date),
    ]

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "precise_get_buyer_quarter_kpis.py failed "
            f"(exit_code={proc.returncode}). stderr={proc.stderr.strip()}"
        )

    try:
        return json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as e:
        raise RuntimeError(
            "precise_get_buyer_quarter_kpis.py returned non-JSON output. "
            f"error={e} stdout={(proc.stdout or '').strip()[:500]}"
        ) from e


def _kpi_snapshot_from_precise_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract a stable KPI snapshot dict from the precise KPI script output.
    We intentionally keep it as a dict because the UI contract already maps
    snapshot→rows deterministically.
    """
    # Current precise KPI script returns:
    # { "result": { ... }, ... }  (not a renderer-shaped final_response)
    res = payload.get("result")
    if isinstance(res, dict) and res:
        return {str(k): v for k, v in res.items()}

    # Backward-compatible shapes (if we ever wrap it as a final_response later).
    fr = payload.get("final_response") or {}
    snap = fr.get("kpi_snapshot") or {}
    if isinstance(snap, dict) and snap:
        return {str(k): v for k, v in snap.items()}

    # Fallback: best-effort extraction from other common payload keys.
    out: Dict[str, Any] = {}
    k = payload.get("kpis") or payload.get("metrics") or {}
    if isinstance(k, dict):
        out.update({str(kk): vv for kk, vv in k.items()})
    return out


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
    tf = slots.get("timeframe") or {}
    start_date = str(tf.get("start") or "").strip()
    end_date = str(tf.get("end") or "").strip()
    if not start_date or not end_date:
        raise ValueError("saved report plan missing timeframe start/end for KPI sourcing")

    narrative_fr: Dict[str, Any] = {}
    listing_fr: Optional[Dict[str, Any]] = None

    block_runs: List[Dict[str, Any]] = []
    base_ep: Dict[str, Any] = {}
    block_outputs: List[BlockOutput] = []

    for row in plan.get("data_blocks") or []:
        if row.get("status") != "selected":
            continue
        bid = str(row.get("block_id") or "").strip()
        btype = str(row.get("block_type") or "").strip() or "unknown"
        out_key = str(row.get("output_key") or "").strip() or "unknown"
        source_mode = str(row.get("source_mode") or row.get("retrieval_mode") or "").strip()
        qfam = str(row.get("query_family_hint") or "").strip()

        # Spec-driven KPI block (no hidden block_id branching).
        if btype == "kpi_table" and source_mode == "precise" and qfam == "buyer_quarter_kpis":
            kpi_payload = _run_precise_buyer_quarter_kpis(
                buyer_id=int(buyer_id),
                start_date=start_date,
                end_date=end_date,
            )
            ks = _kpi_snapshot_from_precise_payload(kpi_payload)
            block_outputs.append(
                BlockOutput(
                    block_id=bid or "kpi_table",
                    block_type="kpi_table",
                    output_key=out_key or "kpi_snapshot",
                    source="precise",
                    payload={"snapshot": ks},
                )
            )
            block_runs.append(
                {
                    "block_id": bid or "kpi_table",
                    "sub_query": f"Precise KPIs for Buyer {buyer_id} {period_label}",
                    "force_precise": True,
                    "selected_handler": "precise_get_buyer_quarter_kpis",
                    "execution_plan": {"mode": "precise", "intent": "buyer_quarter_kpis"},
                }
            )
            continue

        if bid not in queries:
            continue
        sub_q = queries[bid]
        # Listing blocks must be deterministic and precise when the template declares it.
        # Do not depend on router heuristics for listings.
        use_fp = bool(force_precise) or (btype == "row_listing" and source_mode == "precise")
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

        if btype == "executive_summary":
            narrative_fr = fr
            block_outputs.append(
                BlockOutput(
                    block_id=bid,
                    block_type="executive_summary",
                    output_key=out_key or "executive_summary",
                    source="semantic",
                    payload={"text": _executive_from_narrative(fr)},
                )
            )
            # Normalize common narrative-derived sections as explicit typed outputs.
            hs = _merge_highlights(fr)
            if hs:
                block_outputs.append(
                    BlockOutput(
                        block_id=f"{bid}__highlights",
                        block_type="highlights",
                        output_key="highlights",
                        source="semantic",
                        payload={"items": hs},
                    )
                )
            ns = _merge_notes(fr)
            if ns:
                block_outputs.append(
                    BlockOutput(
                        block_id=f"{bid}__notes",
                        block_type="notes",
                        output_key="notes",
                        source="semantic",
                        payload={"items": ns},
                    )
                )
            # Carry over semantic quality (trust contract).
            sq = fr.get("semantic_quality")
            if isinstance(sq, dict) and sq:
                pass
        elif btype == "row_listing":
            listing_fr = fr
            snap = fr.get("kpi_snapshot") if isinstance(fr, dict) else {}
            supporting = fr.get("supporting_details") if isinstance(fr, dict) else {}
            first_preview = []
            if isinstance(supporting, dict):
                cand = supporting.get("first_rows_preview") or []
                if isinstance(cand, list):
                    # Keep preview small and deterministic.
                    first_preview = cand[:25]
            block_outputs.append(
                BlockOutput(
                    block_id=bid,
                    block_type="row_listing",
                    output_key=out_key,
                    source="precise" if (fr.get("mode") == "precise") else "unknown",
                    payload={
                        "kpi_snapshot": snap if isinstance(snap, dict) else {},
                        "rows_preview": first_preview,
                        "notes": _merge_notes(fr),
                    },
                )
            )

    # Some templates are listing-only (no semantic narrative). Do not fail those runs.
    # In that case, semantic_quality remains None and next-step copy falls back.

    merged_plan: Dict[str, Any] = {
        **base_ep,
        "intent": "saved_report_template",
        "report_template_id": plan.get("template_id"),
        "saved_report_plan": plan,
        "saved_report_runtime_version": SAVED_REPORT_RUNTIME_VERSION,
    }

    sq_obj = (
        narrative_fr.get("semantic_quality")
        if isinstance(narrative_fr.get("semantic_quality"), dict)
        else None
    )
    suggested_next = str(narrative_fr.get("suggested_next_question") or "").strip()
    if not suggested_next and isinstance(listing_fr, dict):
        suggested_next = str(listing_fr.get("suggested_next_question") or "").strip()

    final_response = normalize_saved_report_v2(
        user_query=user_query,
        template_id=str(plan.get("template_id") or ""),
        display_name=str(plan.get("display_name") or "Saved report"),
        buyer_id=int(buyer_id),
        period_label=period_label,
        section_order=list(plan.get("section_order") or []),
        block_outputs=block_outputs,
        semantic_quality=sq_obj,
        suggested_next_question=suggested_next or _DEFAULT_NEXT,
    )

    block_outputs_serialized = [
        {
            "block_id": bo.block_id,
            "block_type": bo.block_type,
            "output_key": bo.output_key,
            "source": bo.source,
            "payload": bo.payload,
        }
        for bo in block_outputs
    ]

    buyer_label = f"Buyer {buyer_id}"

    template_module_ids = tuple(
        mid for mid in (plan.get("prompt_modules") or []) if isinstance(mid, str) and mid.strip()
    )
    unknown_modules = [mid for mid in template_module_ids if mid not in PROMPT_MODULES]
    resolved_module_ids: Tuple[str, ...]
    if template_module_ids and not unknown_modules:
        resolved_module_ids = template_module_ids
        module_selection_source = "template"
    else:
        resolved_module_ids = SAVED_REPORT_MODULE_SELECTION
        module_selection_source = "default_fallback" if template_module_ids else "default"

    assembled_prompt_payload = build_saved_report_prompt_payload(
        final_response=final_response,
        block_outputs=block_outputs_serialized,
        buyer_label=buyer_label,
        period_label=period_label,
        module_ids=resolved_module_ids,
    )

    final_response["_assembly_inputs"] = {
        "buyer_label": buyer_label,
        "period_label": period_label,
        "block_outputs": block_outputs_serialized,
        "module_ids": list(resolved_module_ids),
    }

    try:
        ph = run_phrasing_for_final_response(
            final_response,
            use_ava=use_ava,
            strict_validation=strict_validation,
            thread_id=thread_id,
            app_user_id=app_user_id,
        )
    finally:
        final_response.pop("_assembly_inputs", None)

    return {
        "saved_report_runtime_version": SAVED_REPORT_RUNTIME_VERSION,
        "execution_plan": merged_plan,
        "selected_handler": f"saved_report_{plan.get('template_id')}",
        "final_response": final_response,
        "structured_report": build_structured_report(final_response),
        "template_block_runs": block_runs,
        "template_block_outputs_v2": block_outputs_serialized,
        "prompt_modules": {
            "assembler_version": ASSEMBLER_VERSION,
            "modules_used": list(resolved_module_ids),
            "modules_referenced_by_template": list(template_module_ids),
            "module_selection_source": module_selection_source,
            "unknown_modules_in_template": unknown_modules,
            "module_versions": module_summary(),
            "assembled_payload": assembled_prompt_payload,
        },
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
