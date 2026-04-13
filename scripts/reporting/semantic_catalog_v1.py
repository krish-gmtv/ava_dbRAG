"""
Semantic catalog (v1): enumerate which buyer/quarters exist in the precomputed semantic
summary corpus (the same data that gets embedded into Pinecone).

Purpose: when semantic retrieval fails-closed due to metadata mismatch or missing docs,
we can tell the user which quarters are actually indexed for that buyer, instead of
repeating a generic failure message.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class BuyerQuarter:
    buyer_id: int
    period_year: int
    period_quarter: int
    period_label: str


def _default_catalog_path() -> Path:
    # Default to the repo’s seed output used for semantic chunks.
    return (
        Path(__file__).resolve().parents[2]
        / "KPIs"
        / "large_seed_res"
        / "buyer_quarterly_chunks_v2_final.csv"
    )


@lru_cache(maxsize=1)
def _load_catalog_rows() -> List[BuyerQuarter]:
    raw = os.environ.get("SEMANTIC_CATALOG_CSV", "").strip()
    path = Path(raw) if raw else _default_catalog_path()
    if not path.exists():
        return []

    out: List[BuyerQuarter] = []
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                buyer_id = int(str(row.get("assigned_user_id") or "").strip())
                year = int(str(row.get("period_year") or "").strip())
                quarter = int(str(row.get("period_quarter") or "").strip())
                label = str(row.get("period_label") or "").strip() or f"Q{quarter} {year}"
            except Exception:
                continue
            out.append(
                BuyerQuarter(
                    buyer_id=buyer_id,
                    period_year=year,
                    period_quarter=quarter,
                    period_label=label,
                )
            )
    return out


@lru_cache(maxsize=2048)
def available_period_labels_for_buyer(buyer_id: int) -> List[str]:
    # De-dupe and sort consistently (year asc, quarter asc).
    pairs: Dict[Tuple[int, int], str] = {}
    for row in _load_catalog_rows():
        if row.buyer_id != int(buyer_id):
            continue
        pairs[(row.period_year, row.period_quarter)] = row.period_label
    return [pairs[k] for k in sorted(pairs.keys())]


def format_available_periods_line(buyer_id: int, *, max_items: int = 8) -> Optional[str]:
    labels = available_period_labels_for_buyer(int(buyer_id))
    if not labels:
        return None
    shown = labels[: max_items if max_items > 0 else 8]
    suffix = "" if len(labels) <= len(shown) else f" (+{len(labels) - len(shown)} more)"
    return f"Indexed quarters for this buyer: {', '.join(shown)}{suffix}."

