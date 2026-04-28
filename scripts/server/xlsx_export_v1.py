from __future__ import annotations

import io
import re
from datetime import date as _date
from http import HTTPStatus
from typing import Any, Dict, List, Tuple

try:
    from openpyxl import Workbook  # type: ignore
except Exception:  # pragma: no cover
    Workbook = None  # type: ignore


def _safe_sheet_name(name: str, used: set[str]) -> str:
    base = re.sub(r"[\[\]\*\?/\\:]", "_", name).strip() or "Sheet"
    base = base[:31]
    out = base
    i = 2
    while out in used:
        suffix = f"_{i}"
        out = (base[: (31 - len(suffix))] + suffix)[:31]
        i += 1
    used.add(out)
    return out


def _parse_bucket_key(val: Any, *, split_mode: str) -> str:
    s = str(val or "").strip()
    # Common format: "2021-04-06 04:00:00-04:00"
    if len(s) >= 10 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", s[:10]):
        yyyy, mm, dd = s[:10].split("-")
        if split_mode == "year":
            return yyyy
        if split_mode == "month":
            return f"{yyyy}-{mm}"
        if split_mode == "week":
            try:
                d = _date(int(yyyy), int(mm), int(dd))
                iso_year, iso_week, _ = d.isocalendar()
                return f"{iso_year}-W{iso_week:02d}"
            except Exception:
                return "unknown_date"
    return "unknown_date"


def build_xlsx_bytes(payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any] | None, bytes | None, str | None]:
    """
    Return (http_status, json_error_payload_or_none, body_bytes_or_none, filename_or_none)
    """
    if Workbook is None:
        return (
            int(HTTPStatus.INTERNAL_SERVER_ERROR),
            {
                "error": "XLSX export unavailable",
                "details": "Missing dependency: openpyxl. Install it with: pip install openpyxl",
            },
            None,
            None,
        )

    columns = payload.get("columns")
    rows = payload.get("rows")
    split_mode = str(payload.get("split_mode") or "none").strip().lower()
    date_col_idx = payload.get("date_col_idx")
    preserve_types = bool(payload.get("preserve_types", False))
    filename = str(payload.get("filename") or "report.xlsx").strip() or "report.xlsx"
    if not filename.lower().endswith(".xlsx"):
        filename += ".xlsx"

    if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
        return (
            int(HTTPStatus.BAD_REQUEST),
            {"error": "columns must be a list of strings"},
            None,
            None,
        )
    if not isinstance(rows, list) or not all(isinstance(r, list) for r in rows):
        return (
            int(HTTPStatus.BAD_REQUEST),
            {"error": "rows must be a list of arrays"},
            None,
            None,
        )

    try:
        date_col = int(date_col_idx) if date_col_idx is not None else -1
    except Exception:
        date_col = -1

    buckets: Dict[str, List[List[Any]]] = {}
    if split_mode in {"week", "month", "year"} and date_col >= 0:
        for r in rows:
            key = _parse_bucket_key(r[date_col] if date_col < len(r) else None, split_mode=split_mode)
            buckets.setdefault(key, []).append(r)
    else:
        buckets = {"Data": rows}

    wb = Workbook()
    default_ws = wb.active
    wb.remove(default_ws)

    used_names: set[str] = set()
    for key in sorted(buckets.keys()):
        ws = wb.create_sheet(title=_safe_sheet_name(key, used_names))
        ws.append(columns)
        for r in buckets[key]:
            if preserve_types:
                ws.append([("" if v is None else v) for v in r])
            else:
                ws.append([("" if v is None else str(v)) for v in r])

    bio = io.BytesIO()
    wb.save(bio)
    return (int(HTTPStatus.OK), None, bio.getvalue(), filename)

