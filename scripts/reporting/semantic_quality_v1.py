"""
Gate between Pinecone retrieval and answer rendering: classify semantic strength so we do not
emit full narrative templates when there is no usable evidence.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


def _env_float(name: str, default: str) -> float:
    raw = os.environ.get(name, default).strip()
    try:
        return float(raw)
    except ValueError:
        return float(default)


# Cosine-style scores from Pinecone; tune with SEMANTIC_SCORE_HIGH_MIN / MEDIUM_MIN env vars.
SCORE_HIGH_MIN = _env_float("SEMANTIC_SCORE_HIGH_MIN", "0.55")
SCORE_MEDIUM_MIN = _env_float("SEMANTIC_SCORE_MEDIUM_MIN", "0.35")


@dataclass
class SemanticQuality:
    confidence_level: str  # high | medium | low
    render_mode: str  # full_semantic | weak_semantic | no_semantic_summary
    reasons: List[str] = field(default_factory=list)
    metadata_aligned: bool = True


def _to_int_id(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if s.isdigit():
        try:
            return int(s)
        except ValueError:
            return None
    return None


def _parse_quarter_year_from_label(label: str) -> Tuple[Optional[int], Optional[int]]:
    if not label or not isinstance(label, str):
        return None, None
    m = re.search(r"\bQ([1-4])\s+(\d{4})\b", label, re.IGNORECASE)
    if not m:
        return None, None
    try:
        return int(m.group(2)), int(m.group(1))
    except ValueError:
        return None, None


def _metadata_matches_request(
    best: Dict[str, Any],
    req_buyer_id: Optional[int],
    req_year: Optional[int],
    req_quarter: Optional[int],
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    ok = True
    meta_bid = _to_int_id(best.get("buyer_id"))
    if req_buyer_id is not None and meta_bid is not None and meta_bid != req_buyer_id:
        ok = False
        reasons.append(f"top_match_buyer_{meta_bid}_requested_{req_buyer_id}")

    plabel = (best.get("period_label") or "").strip()
    py, pq = _parse_quarter_year_from_label(plabel)
    if req_year is not None and req_quarter is not None:
        if py is not None and pq is not None:
            if py != req_year or pq != req_quarter:
                ok = False
                reasons.append(
                    f"top_match_period_Q{pq}_{py}_requested_Q{req_quarter}_{req_year}"
                )
        elif py is not None and py != req_year:
            ok = False
            reasons.append(f"top_match_year_{py}_requested_{req_year}")
    elif req_year is not None and py is not None and py != req_year:
        ok = False
        reasons.append(f"top_match_year_{py}_requested_{req_year}")
    return ok, reasons


def _snippet_usable(snippet: Optional[str]) -> bool:
    if not snippet or not str(snippet).strip():
        return False
    s = str(snippet).strip().lower()
    if s == "no semantic summary found.":
        return False
    return True


def evaluate_semantic_quality(
    plan: Dict[str, Any],
    handler_output: Dict[str, Any],
) -> SemanticQuality:
    params = handler_output.get("params", {}) or {}
    result = handler_output.get("result", {}) or {}
    matches = result.get("matches") or []
    reasons: List[str] = []

    req_buyer = _to_int_id(params.get("buyer_id"))
    req_year = _to_int_id(params.get("period_year"))
    req_quarter = _to_int_id(params.get("period_quarter"))

    if not matches:
        reasons.append("no_matches")
        return SemanticQuality(
            confidence_level="low",
            render_mode="no_semantic_summary",
            reasons=reasons,
            metadata_aligned=False,
        )

    best = matches[0] if isinstance(matches[0], dict) else {}
    raw_score = best.get("score")
    score_val: Optional[float] = None
    if raw_score is not None:
        try:
            score_val = float(raw_score)
        except (TypeError, ValueError):
            score_val = None

    snippet = (best.get("summary_snippet") or "").strip()
    aligned, align_reasons = _metadata_matches_request(
        best, req_buyer, req_year, req_quarter
    )
    reasons.extend(align_reasons)

    has_text = _snippet_usable(snippet)

    if not has_text:
        reasons.append("empty_or_missing_snippet")
        if score_val is not None and score_val >= SCORE_MEDIUM_MIN and aligned:
            reasons.append("no_summary_text_but_medium_score")
            return SemanticQuality(
                confidence_level="medium",
                render_mode="weak_semantic",
                reasons=reasons,
                metadata_aligned=aligned,
            )
        return SemanticQuality(
            confidence_level="low",
            render_mode="no_semantic_summary",
            reasons=reasons,
            metadata_aligned=aligned,
        )

    # Has usable snippet
    if score_val is None:
        reasons.append("missing_similarity_score")
        if aligned:
            return SemanticQuality(
                confidence_level="medium",
                render_mode="weak_semantic",
                reasons=reasons,
                metadata_aligned=True,
            )
        return SemanticQuality(
            confidence_level="low",
            render_mode="no_semantic_summary",
            reasons=reasons,
            metadata_aligned=False,
        )

    if not aligned:
        # Fail-closed on metadata mismatch for structured requests (buyer+quarter/year).
        # For template-driven reporting, showing the "wrong quarter" narrative inside a
        # fixed report is worse than returning no summary.
        strict_mismatch = False
        if req_buyer is not None and any(r.startswith("top_match_buyer_") for r in reasons):
            strict_mismatch = True
        if (
            req_year is not None
            and req_quarter is not None
            and any(r.startswith("top_match_period_") for r in reasons)
        ):
            strict_mismatch = True
        if strict_mismatch:
            return SemanticQuality(
                confidence_level="low",
                render_mode="no_semantic_summary",
                reasons=reasons + ["metadata_mismatch_fail_closed"],
                metadata_aligned=False,
            )

        if score_val is not None and score_val >= SCORE_HIGH_MIN:
            return SemanticQuality(
                confidence_level="medium",
                render_mode="weak_semantic",
                reasons=reasons + ["metadata_mismatch_downgrade"],
                metadata_aligned=False,
            )
        if score_val is not None and score_val >= SCORE_MEDIUM_MIN:
            return SemanticQuality(
                confidence_level="medium",
                render_mode="weak_semantic",
                reasons=reasons + ["metadata_mismatch_downgrade"],
                metadata_aligned=False,
            )
        return SemanticQuality(
            confidence_level="low",
            render_mode="no_semantic_summary",
            reasons=reasons + ["metadata_mismatch_downgrade"],
            metadata_aligned=False,
        )

    if score_val >= SCORE_HIGH_MIN:
        reasons.append(f"score_high_{score_val:.4f}")
        return SemanticQuality(
            confidence_level="high",
            render_mode="full_semantic",
            reasons=reasons,
            metadata_aligned=True,
        )
    if score_val >= SCORE_MEDIUM_MIN:
        reasons.append(f"score_medium_{score_val:.4f}")
        return SemanticQuality(
            confidence_level="medium",
            render_mode="weak_semantic",
            reasons=reasons,
            metadata_aligned=True,
        )

    reasons.append(f"score_low_{score_val:.4f}")
    return SemanticQuality(
        confidence_level="low",
        render_mode="no_semantic_summary",
        reasons=reasons,
        metadata_aligned=aligned,
    )
