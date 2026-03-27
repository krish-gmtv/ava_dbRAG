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
    password: str = os.environ.get("PRECISE_PG_PASSWORD", "")


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def parse_buyer_id(query: str) -> Optional[int]:
    m = re.search(r"\bBuyer\s+(\d+)\b", query, re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def try_parse_date(s: str) -> Optional[date]:
    s = s.strip()
    if not s:
        return None
    s_norm = re.sub(r"[./]", "-", s)
    for fmt in ("%Y-%m-%d", "%m-%d-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s_norm, fmt).date()
        except ValueError:
            continue
    return None


def parse_between_dates(query: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(
        r"\b(between|from)\s+([0-9]{1,4}[-/.][0-9]{1,2}[-/.][0-9]{1,4})\s+(to|and)\s+([0-9]{1,4}[-/.][0-9]{1,2}[-/.][0-9]{1,4})\b",
        query.strip(),
        re.IGNORECASE,
    )
    if not m:
        return None, None
    return try_parse_date(m.group(2)), try_parse_date(m.group(4))


def parse_quarter(query: str) -> Tuple[Optional[int], Optional[int]]:
    m = re.search(r"\bQ([1-4])\s+(\d{4})\b", query, re.IGNORECASE)
    if not m:
        return None, None
    try:
        return int(m.group(2)), int(m.group(1))
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


def list_opportunities(conn, buyer_id: int, start_date: date, end_date: date) -> List[Dict[str, Any]]:
    sql = """
    SELECT
        o.opportunity_id,
        o.upsheet_id,
        o.created_at,
        o.expected_amount,
        u.assigned_user_id AS buyer_id,
        b.buyer_fname,
        b.buyer_lname,
        (b.buyer_fname || ' ' || b.buyer_lname) AS buyer_name,
        u.status AS upsheet_status,
        u.insert_date AS upsheet_insert_date,
        u.delivered_date,
        u.sold_date,
        u.sale_price
    FROM opportunities o
    JOIN upsheets u ON u.upsheet_id = o.upsheet_id
    JOIN buyers b ON b.assigned_user_id = u.assigned_user_id
    WHERE
        u.assigned_user_id = %s
        AND o.created_at >= %s::date
        AND o.created_at < (%s::date + INTERVAL '1 day')
    ORDER BY o.created_at ASC, o.opportunity_id ASC;
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
        "query_type": "list_buyer_opportunities",
        "params": {
            "buyer_id": buyer_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "input_query": query,
        "result": {"row_count": len(rows), "rows": rows},
        "provenance": {
            "source_tables": ["opportunities", "upsheets", "buyers"],
            "execution_mode": "direct_sql",
            "database": os.environ.get("PRECISE_PG_DB", "ava_sandboxV2"),
        },
        "notes": {
            "filter_basis": "opportunities.created_at within [start_date, end_date]",
            "safety": "This payload is produced by direct SQL against ava_sandboxV2; do not invent numbers.",
        },
    }


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Precise retrieval: list opportunities for a buyer over quarter/date range."
    )
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--buyer-id", type=int, default=None)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--quarter", type=int, default=None)
    args = parser.parse_args()

    q = args.query.strip()
    buyer_id = args.buyer_id if args.buyer_id is not None else parse_buyer_id(q)
    if buyer_id is None:
        raise SystemExit("Could not parse buyer_id. Include 'Buyer <number>' or pass --buyer-id.")

    start_d = try_parse_date(args.start_date) if args.start_date else None
    end_d = try_parse_date(args.end_date) if args.end_date else None
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
            if year is None or quarter is None:
                raise SystemExit(
                    "Could not determine date window. Provide Q<1-4> <year>, between <date> to <date>, "
                    "or pass --start-date/--end-date or --year/--quarter."
                )
            start_date, end_date = quarter_date_range(year, quarter)

    if start_date > end_date:
        raise SystemExit("start_date must be <= end_date.")

    cfg = PgConfig()
    conn = connect_pg(cfg)
    try:
        rows = list_opportunities(conn, buyer_id, start_date, end_date)
    finally:
        conn.close()

    payload = build_payload(q, buyer_id, start_date, end_date, rows)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
