import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)


# -----------------------------
# Parsing helpers
# -----------------------------


def parse_buyer_id(query: str) -> Optional[int]:
    """
    Parse buyer id from patterns like 'Buyer 2'.
    """
    m = re.search(r"\bBuyer\s+(\d+)\b", query, re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def parse_quarter_year(query: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse 'Q1 2018' into (year, quarter).
    """
    m = re.search(r"\bQ([1-4])\s+(\d{4})\b", query, re.IGNORECASE)
    if not m:
        return None, None
    try:
        quarter = int(m.group(1))
        year = int(m.group(2))
        return year, quarter
    except ValueError:
        return None, None


def try_parse_date(s: str) -> Optional[date]:
    """
    Parse a simple date string into a date object.
    Supports a few common formats.
    """
    s = s.strip()
    if not s:
        return None

    s_norm = re.sub(r"[./]", "-", s)
    fmts = ["%Y-%m-%d", "%m-%d-%Y", "%d-%m-%Y"]
    for fmt in fmts:
        try:
            return datetime.strptime(s_norm, fmt).date()
        except ValueError:
            continue
    return None


def parse_between_dates(query: str) -> Tuple[Optional[date], Optional[date], Optional[str]]:
    """
    Parse patterns like:
    - between 12-03-2018 to 12-30-2018
    - between 2018-03-12 and 2018-12-30
    - from 2018-03-12 to 2018-12-30
    Returns (start_date, end_date, raw_text).
    """
    q = query.strip()
    m = re.search(
        r"\b(between|from)\s+([0-9]{1,4}[-/.][0-9]{1,2}[-/.][0-9]{1,4})\s+(to|and)\s+([0-9]{1,4}[-/.][0-9]{1,2}[-/.][0-9]{1,4})\b",
        q,
        re.IGNORECASE,
    )
    if not m:
        return None, None, None
    start_raw = m.group(2)
    end_raw = m.group(4)
    start = try_parse_date(start_raw)
    end = try_parse_date(end_raw)
    # Recover common typo: 01-01-208 when the other bound is clearly 20xx.
    if start and end and start.year < 1000 <= end.year:
        try:
            start = date(end.year, start.month, start.day)
        except ValueError:
            pass
    raw = m.group(0)
    return start, end, raw


def quarter_date_range(year: int, quarter: int) -> Tuple[date, date]:
    if quarter == 1:
        return date(year, 1, 1), date(year, 3, 31)
    if quarter == 2:
        return date(year, 4, 1), date(year, 6, 30)
    if quarter == 3:
        return date(year, 7, 1), date(year, 9, 30)
    if quarter == 4:
        return date(year, 10, 1), date(year, 12, 31)
    raise ValueError(f"Invalid quarter: {quarter}")


def parse_timeframe(query: str) -> Dict[str, Any]:
    """
    Parse timeframe into a normalized structure.
    For v1 we support:
    - quarter/year: 'Q1 2018'
    - between/from .. to .. date range
    - bare year (semantic hint only)
    """
    year, quarter = parse_quarter_year(query)
    if year is not None and quarter is not None:
        start, end = quarter_date_range(year, quarter)
        return {
            "raw_text": f"Q{quarter} {year}",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "granularity": "quarter",
        }

    start_d, end_d, raw = parse_between_dates(query)
    if start_d and end_d:
        return {
            "raw_text": raw,
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
            "granularity": "range",
        }

    # Bare year as a semantic hint
    m = re.search(r"\b(19\d{2}|20\d{2})\b", query)
    if m:
        try:
            year_only = int(m.group(1))
            return {
                "raw_text": str(year_only),
                "start": None,
                "end": None,
                "granularity": "year",
            }
        except ValueError:
            pass

    return {
        "raw_text": None,
        "start": None,
        "end": None,
        "granularity": None,
    }


def infer_quarter_from_date_range(start: date, end: date) -> Tuple[Optional[int], Optional[int]]:
    """
    Map an inclusive date range to (period_year, period_quarter), or year-only when
    the range is not a single calendar quarter (or is a full Jan 1 .. Dec 31 year).
    """
    if start > end:
        return None, None
    for y in range(start.year, end.year + 1):
        for q in range(1, 5):
            qs, qe = quarter_date_range(y, q)
            if start >= qs and end <= qe:
                return y, q
    if start == date(start.year, 1, 1) and end == date(start.year, 12, 31):
        return start.year, None
    if start.year == end.year:
        return start.year, None
    return None, None


def period_from_execution_plan(plan: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    """
    Derive (period_year, period_quarter) from the router's normalized timeframe.
    Used to align semantic search + answer headlines with explicit dates (ranges, years, quarters).
    """
    tf = plan.get("timeframe") or {}
    gran = str(tf.get("granularity") or "").strip().lower()
    if gran == "quarter":
        raw = str(tf.get("raw_text") or "")
        y, q = parse_quarter_year(raw)
        if y is not None and q is not None:
            return y, q
        st = tf.get("start")
        if isinstance(st, str) and len(st) >= 10:
            try:
                d = date.fromisoformat(st[:10])
                qn = (d.month - 1) // 3 + 1
                return d.year, qn
            except ValueError:
                pass
    if gran == "range":
        st = tf.get("start")
        en = tf.get("end")
        if isinstance(st, str) and isinstance(en, str) and len(st) >= 10 and len(en) >= 10:
            try:
                sd = date.fromisoformat(st[:10])
                ed = date.fromisoformat(en[:10])
                return infer_quarter_from_date_range(sd, ed)
            except ValueError:
                pass
    if gran == "year":
        raw = str(tf.get("raw_text") or "")
        m = re.search(r"\b(19\d{2}|20\d{2})\b", raw)
        if m:
            return int(m.group(1)), None
    return None, None


# -----------------------------
# Signal detection
# -----------------------------


PRECISE_KEYWORDS = [
    "list",
    "show",
    "records",
    "exact",
    "total",
    "count",
    "how many",
    "verify",
    "accurate",
    "completed",
    "between",
    "from ",
]

SEMANTIC_KEYWORDS = [
    "performance",
    "perform",
    "summary",
    "recap",
    "overall",
    "trend",
    "insights",
    "patterns",
    "why",
    "what changed",
    "drivers",
    "compare",
]


def detect_precise_signals(query: str) -> List[str]:
    q = query.lower()
    reasons: List[str] = []
    for kw in PRECISE_KEYWORDS:
        if kw in q:
            reasons.append(f"precise_signal:{kw}")
    return reasons


def detect_semantic_signals(query: str) -> List[str]:
    q = query.lower()
    reasons: List[str] = []
    for kw in SEMANTIC_KEYWORDS:
        if kw in q:
            reasons.append(f"semantic_signal:{kw}")
    return reasons


# -----------------------------
# Family classification
# -----------------------------


def classify_query_family(query: str) -> str:
    """
    Very small v1 family classifier.
    """
    q = query.lower()
    # Object-first: decide which business object the query is about.
    if "upsheet" in q:
        return "list_buyer_upsheets"
    if "opportunit" in q:
        return "list_buyer_opportunities"
    # KPI-ish words
    if any(
        kw in q
        for kw in [
            "kpi",
            "metric",
            "metrics",
            "close rate",
            "conversion rate",
            "conversions",
            "total leads",
            "lead count",
            "opportunity count",
            "sold upsheets",
            "sold upsheet",
        ]
    ):
        return "buyer_quarter_kpis"
    # default semantic family
    return "buyer_performance_summary"


def choose_mode(
    precise_reasons: List[str],
    semantic_reasons: List[str],
    family: str,
) -> Tuple[str, List[str]]:
    """
    Decide between 'precise' and 'semantic' and return mode + reason_codes.
    Rules:
    - list_* families          -> always precise (v1)
    - buyer_performance_*      -> usually semantic
    - buyer_quarter_kpis       -> precise if exact/count/list language, semantic if performance/summary language
    - otherwise:
        - strong precise signal present  -> precise
        - else strong semantic signal   -> semantic
        - else fallback -> precise
    """
    # list_* families are always precise in v1
    if family.startswith("list_"):
        return "precise", ["decision:precise_family_list"]

    # Performance summary families lean semantic
    if family == "buyer_performance_summary":
        if semantic_reasons:
            return "semantic", ["decision:semantic_due_to_summary_signal"]
        if precise_reasons:
            # Only if there is clear precise language and no semantic signal
            return "precise", ["decision:precise_due_to_exact_signal"]
        return "semantic", ["decision:semantic_family_default"]

    # KPI family: mixed behavior
    if family == "buyer_quarter_kpis":
        if precise_reasons and not semantic_reasons:
            return "precise", ["decision:precise_kpi_due_to_exact_signal"]
        if semantic_reasons and not precise_reasons:
            return "semantic", ["decision:semantic_kpi_due_to_summary_signal"]
        if precise_reasons and semantic_reasons:
            # tie-break: exact/count/list wording wins for KPIs
            return "precise", ["decision:precise_kpi_tiebreak"]
        # no strong signals, default to precise for safety
        return "precise", ["decision:precise_kpi_default"]

    # Fallback: use signals only
    if precise_reasons and not semantic_reasons:
        return "precise", ["decision:precise_due_to_signal"]
    if semantic_reasons and not precise_reasons:
        return "semantic", ["decision:semantic_due_to_signal"]

    # If still unclear, default to precise for safety
    return "precise", ["decision:fallback_precise"]


# -----------------------------
# Execution plan builder
# -----------------------------


def build_execution_plan(query: str) -> Dict[str, Any]:
    buyer_id = parse_buyer_id(query)
    timeframe = parse_timeframe(query)

    precise_reasons = detect_precise_signals(query)
    semantic_reasons = detect_semantic_signals(query)
    family = classify_query_family(query)
    mode, mode_reasons = choose_mode(precise_reasons, semantic_reasons, family)

    if family == "list_buyer_upsheets":
        # list_* families are always precise in v1
        handler = "precise_list_buyer_upsheets"
        mode = "precise"
        intent = "buyer_upsheet_listing"
    elif family == "list_buyer_opportunities":
        # No precise_list_buyer_opportunities handler in v1; semantic retrieval avoids a hard pipeline failure.
        handler = "semantic_buyer_performance_summary"
        mode = "semantic"
        intent = "buyer_opportunity_listing"
    elif family == "buyer_quarter_kpis":
        # KPI / metric questions use semantic retrieval (precomputed quarterly summaries in Pinecone).
        # Precise SQL in this project is reserved for direct table row lookups (upsheets, opportunities).
        handler = "semantic_buyer_performance_summary"
        mode = "semantic"
        intent = "buyer_quarter_kpis"
        mode_reasons = ["decision:semantic_kpi_only"]
    else:
        handler = "semantic_buyer_performance_summary"
        intent = "buyer_performance"

    reason_codes = sorted(set(precise_reasons + semantic_reasons + mode_reasons))

    plan: Dict[str, Any] = {
        "intent": intent,
        "entity": {
            "type": "buyer",
            "raw_text": None if buyer_id is None else f"Buyer {buyer_id}",
            "resolved_id": buyer_id,
        },
        "timeframe": timeframe,
        "mode": mode,
        "reason_codes": reason_codes,
        "retrieval_plan": {
            "handler": handler,
            "query_family": family,
        },
        "report_template_id": (
            "buyer_detail_v1_precise"
            if mode == "precise"
            else "buyer_performance_v1_semantic"
        ),
    }

    return plan


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(
        description=(
            "Deterministic v1 intent router. "
            "Takes a natural language query and returns a JSON execution plan "
            "indicating mode (semantic/precise), entity/timeframe, and handler."
        )
    )
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="Natural language query to route.",
    )

    args = parser.parse_args()
    q = args.query.strip()
    if not q:
        raise SystemExit("Query must not be empty.")

    plan = build_execution_plan(q)
    print(json.dumps(plan, indent=2))


if __name__ == "__main__":
    main()

