import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class PgConfig:
    host: str = os.environ.get("PRECISE_PG_HOST", "localhost")
    port: int = int(os.environ.get("PRECISE_PG_PORT", "5432"))
    dbname: str = os.environ.get("PRECISE_PG_DB", "ava_sandboxV2")
    user: str = os.environ.get("PRECISE_PG_USER", "postgres")
    # No real password is hardcoded; value should come from PRECISE_PG_PASSWORD in .env
    password: str = os.environ.get("PRECISE_PG_PASSWORD", "")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def parse_buyer_id(query: str) -> Optional[int]:
    m = re.search(r"\bBuyer\s+(\d+)\b", query, re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def try_parse_date(s: str) -> Optional[date]:
    """
    Parse common date strings into a date.
    Supported examples:
    - 2018-03-12
    - 03-12-2018
    - 12-03-2018
    - 2018/03/12
    - 03/12/2018
    """
    s = s.strip()
    if not s:
        return None

    # normalize separators
    s_norm = re.sub(r"[./]", "-", s)

    formats = [
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%d-%m-%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s_norm, fmt).date()
        except ValueError:
            continue
    return None


def parse_between_dates(query: str) -> Tuple[Optional[date], Optional[date]]:
    """
    Parse patterns like:
    - between 12-03-2018 to 12-30-2018
    - between 2018-03-12 and 2018-12-30
    - from 2018-03-12 to 2018-12-30
    Returns (start_date, end_date) as date objects.
    """
    q = query.strip()
    m = re.search(
        r"\b(between|from)\s+([0-9]{1,4}[-/.][0-9]{1,2}[-/.][0-9]{1,4})\s+(to|and)\s+([0-9]{1,4}[-/.][0-9]{1,2}[-/.][0-9]{1,4})\b",
        q,
        re.IGNORECASE,
    )
    if not m:
        return None, None

    start_raw = m.group(2)
    end_raw = m.group(4)
    start = try_parse_date(start_raw)
    end = try_parse_date(end_raw)
    return start, end


def parse_quarter(query: str) -> Tuple[Optional[int], Optional[int]]:
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


def connect_pg(cfg: PgConfig):
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    conn.autocommit = True
    return conn


def list_upsheets(
    conn,
    buyer_id: int,
    start_date: date,
    end_date: date,
) -> List[Dict[str, Any]]:
    """
    Return upsheets for a buyer where insert_date falls within [start_date, end_date].
    """
    sql = """
    SELECT
        u.upsheet_id,
        u.assigned_user_id AS buyer_id,
        b.buyer_fname,
        b.buyer_lname,
        (b.buyer_fname || ' ' || b.buyer_lname) AS buyer_name,
        u.insert_date,
        u.status,
        u.vin,
        u.year,
        u.make,
        u.model,
        u.current_mileage,
        u.sold_date,
        u.delivered_date,
        u.sale_price
    FROM upsheets u
    JOIN buyers b
      ON b.assigned_user_id = u.assigned_user_id
    WHERE
        u.assigned_user_id = %s
        AND u.insert_date >= %s::date
        AND u.insert_date < (%s::date + INTERVAL '1 day')
    ORDER BY u.insert_date ASC, u.upsheet_id ASC;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (buyer_id, start_date.isoformat(), end_date.isoformat()))
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def build_payload(
    query: str,
    buyer_id: int,
    start_date: date,
    end_date: date,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "source_mode": "precise",
        "query_type": "list_buyer_upsheets",
        "params": {
            "buyer_id": buyer_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "input_query": query,
        "result": {
            "row_count": len(rows),
            "rows": rows,
        },
        "provenance": {
            "source_tables": ["upsheets", "buyers"],
            "execution_mode": "direct_sql",
            "database": os.environ.get("PRECISE_PG_DB", "ava_sandboxV2"),
        },
        "notes": {
            "filter_basis": "upsheets.insert_date within [start_date, end_date]",
            "safety": "This payload is produced by direct SQL against ava_sandboxV2; do not invent numbers.",
        },
    }


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Precise retrieval: list upsheets for a buyer over a quarter or date range, "
            "by querying Postgres tables directly (ava_sandboxV2)."
        )
    )
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help=(
            "Natural language query. Examples: "
            "'List all upsheets for Buyer 2 in Q1 2018' or "
            "'All upsheets for Buyer 2 between 12-03-2018 to 12-30-2018'"
        ),
    )
    parser.add_argument("--buyer-id", type=int, default=None, help="Override buyer_id.")
    parser.add_argument("--start-date", type=str, default=None, help="Override start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", type=str, default=None, help="Override end date (YYYY-MM-DD).")
    parser.add_argument("--year", type=int, default=None, help="Override year (with --quarter).")
    parser.add_argument("--quarter", type=int, default=None, help="Override quarter 1-4 (with --year).")

    args = parser.parse_args()
    q = args.query.strip()

    buyer_id = args.buyer_id if args.buyer_id is not None else parse_buyer_id(q)
    if buyer_id is None:
        raise SystemExit(
            "Could not parse buyer_id. Please include 'Buyer <number>' in the query or pass --buyer-id."
        )

    # Determine date window in priority order:
    # 1) explicit CLI override dates
    # 2) explicit CLI override year+quarter
    # 3) parse between/from date range in query
    # 4) parse Qx YYYY in query
    start_d: Optional[date] = try_parse_date(args.start_date) if args.start_date else None
    end_d: Optional[date] = try_parse_date(args.end_date) if args.end_date else None

    if start_d and end_d:
        start_date, end_date = start_d, end_d
    elif args.year is not None and args.quarter is not None:
        start_date, end_date = quarter_date_range(args.year, args.quarter)
    else:
        s_between, e_between = parse_between_dates(q)
        if s_between and e_between:
            start_date, end_date = s_between, e_between
        else:
            year, quarter = parse_quarter(q)
            if year is not None and quarter is not None:
                start_date, end_date = quarter_date_range(year, quarter)
            else:
                raise SystemExit(
                    "Could not determine date window. Provide 'between <date> to <date>' or 'Q<1-4> <year>', "
                    "or pass --start-date/--end-date or --year/--quarter."
                )

    if start_date > end_date:
        raise SystemExit("start_date must be <= end_date.")

    cfg = PgConfig()
    logger.info(
        "Running precise SQL on %s:%d/%s for buyer_id=%d, start=%s, end=%s",
        cfg.host,
        cfg.port,
        cfg.dbname,
        buyer_id,
        start_date.isoformat(),
        end_date.isoformat(),
    )

    conn = connect_pg(cfg)
    try:
        rows = list_upsheets(conn, buyer_id, start_date, end_date)
    finally:
        conn.close()

    payload = build_payload(q, buyer_id, start_date, end_date, rows)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()

