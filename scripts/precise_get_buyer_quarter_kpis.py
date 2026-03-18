import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple

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


def parse_quarter(query: str) -> Tuple[Optional[int], Optional[int]]:
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


def fetch_buyer_quarter_kpis(
    conn,
    buyer_id: int,
    period_start: date,
    period_end: date,
) -> Dict[str, Any]:
    """
    Compute buyer-quarter KPIs directly from base tables, mirroring buyer_kpi_quarterly.sql logic.

    Time filtering:
    - leads by leads.date_entered
    - upsheets by upsheets.insert_date
    - opportunities by opportunities.created_at (joined via upsheets)
    - delivered by upsheets.delivered_date
    - sold/value/realization by upsheets.sold_date
    """
    sql = """
    WITH
    params AS (
        SELECT
            %s::int AS buyer_id,
            %s::date AS start_date,
            %s::date AS end_date
    ),
    leads_cte AS (
        SELECT COUNT(DISTINCT l.lead_id) AS total_leads
        FROM leads l, params p
        WHERE l.assigned_user_id = p.buyer_id
          AND l.date_entered >= p.start_date
          AND l.date_entered < (p.end_date + INTERVAL '1 day')
    ),
    upsheets_cte AS (
        SELECT COUNT(DISTINCT u.upsheet_id) AS total_upsheets
        FROM upsheets u, params p
        WHERE u.assigned_user_id = p.buyer_id
          AND u.insert_date >= p.start_date
          AND u.insert_date < (p.end_date + INTERVAL '1 day')
    ),
    leads_with_opportunity_cte AS (
        SELECT COUNT(DISTINCT l.lead_id) AS leads_with_opportunity
        FROM leads l
        JOIN upsheets u ON u.lead_id = l.lead_id
        JOIN opportunities o ON o.upsheet_id = u.upsheet_id,
        params p
        WHERE l.assigned_user_id = p.buyer_id
          AND l.date_entered >= p.start_date
          AND l.date_entered < (p.end_date + INTERVAL '1 day')
    ),
    opportunities_cte AS (
        SELECT COUNT(o.opportunity_id) AS total_opportunities
        FROM opportunities o
        JOIN upsheets u ON u.upsheet_id = o.upsheet_id,
        params p
        WHERE u.assigned_user_id = p.buyer_id
          AND o.created_at >= p.start_date
          AND o.created_at < (p.end_date + INTERVAL '1 day')
    ),
    opportunity_upsheets_cte AS (
        SELECT COUNT(DISTINCT u.upsheet_id) AS opportunity_upsheets
        FROM opportunities o
        JOIN upsheets u ON u.upsheet_id = o.upsheet_id,
        params p
        WHERE u.assigned_user_id = p.buyer_id
          AND o.created_at >= p.start_date
          AND o.created_at < (p.end_date + INTERVAL '1 day')
    ),
    delivered_upsheets_cte AS (
        SELECT COUNT(DISTINCT u.upsheet_id) AS delivered_upsheets
        FROM upsheets u, params p
        WHERE u.assigned_user_id = p.buyer_id
          AND u.delivered_date IS NOT NULL
          AND u.delivered_date >= p.start_date
          AND u.delivered_date < (p.end_date + INTERVAL '1 day')
    ),
    sold_upsheets_cte AS (
        SELECT COUNT(DISTINCT u.upsheet_id) AS sold_upsheets
        FROM upsheets u, params p
        WHERE u.assigned_user_id = p.buyer_id
          AND u.sold_date IS NOT NULL
          AND u.sold_date >= p.start_date
          AND u.sold_date < (p.end_date + INTERVAL '1 day')
    ),
    delivered_opportunity_upsheets_cte AS (
        SELECT COUNT(DISTINCT u.upsheet_id) AS delivered_opportunity_upsheets
        FROM upsheets u
        JOIN opportunities o ON o.upsheet_id = u.upsheet_id,
        params p
        WHERE u.assigned_user_id = p.buyer_id
          AND u.delivered_date IS NOT NULL
          -- Cohort is defined by opportunity creation time in the reporting period.
          -- Delivery can happen outside the period.
          AND o.created_at >= p.start_date
          AND o.created_at < (p.end_date + INTERVAL '1 day')
    ),
    sale_value_cte AS (
        SELECT
            COALESCE(SUM(u.sale_price), 0)::numeric AS total_sale_value,
            ROUND(AVG(u.sale_price), 2) AS avg_sale_value
        FROM upsheets u, params p
        WHERE u.assigned_user_id = p.buyer_id
          AND u.sold_date IS NOT NULL
          AND u.sold_date >= p.start_date
          AND u.sold_date < (p.end_date + INTERVAL '1 day')
    ),
    total_expected_amount_cte AS (
        SELECT COALESCE(SUM(o.expected_amount), 0)::numeric AS total_expected_amount
        FROM opportunities o
        JOIN upsheets u ON u.upsheet_id = o.upsheet_id,
        params p
        WHERE u.assigned_user_id = p.buyer_id
          AND o.created_at >= p.start_date
          AND o.created_at < (p.end_date + INTERVAL '1 day')
    ),
    latest_expected_amount_cte AS (
        SELECT COALESCE(SUM(x.expected_amount), 0)::numeric AS latest_expected_amount
        FROM (
            SELECT
                u.upsheet_id,
                o.expected_amount,
                ROW_NUMBER() OVER (
                    PARTITION BY u.upsheet_id
                    ORDER BY o.created_at DESC, o.opportunity_id DESC
                ) AS rn
            FROM opportunities o
            JOIN upsheets u ON u.upsheet_id = o.upsheet_id,
            params p
            WHERE u.assigned_user_id = p.buyer_id
              AND o.created_at >= p.start_date
              AND o.created_at < (p.end_date + INTERVAL '1 day')
        ) x
        WHERE x.rn = 1
    ),
    realization_cte AS (
        SELECT
            ROUND(SUM(u.sale_price - lo.expected_amount), 2) AS realization_amount,
            ROUND(
                AVG(((u.sale_price - lo.expected_amount) / NULLIF(lo.expected_amount, 0)) * 100),
                2
            ) AS realization_percent
        FROM upsheets u
        LEFT JOIN (
            SELECT
                o.upsheet_id,
                o.expected_amount,
                ROW_NUMBER() OVER (
                    PARTITION BY o.upsheet_id
                    ORDER BY o.created_at DESC, o.opportunity_id DESC
                ) AS rn
            FROM opportunities o
        ) lo
          ON lo.upsheet_id = u.upsheet_id
         AND lo.rn = 1,
        params p
        WHERE u.assigned_user_id = p.buyer_id
          AND u.sold_date IS NOT NULL
          AND u.sold_date >= p.start_date
          AND u.sold_date < (p.end_date + INTERVAL '1 day')
    )
    SELECT
        (SELECT total_leads FROM leads_cte) AS total_leads,
        (SELECT total_upsheets FROM upsheets_cte) AS total_upsheets,
        (SELECT leads_with_opportunity FROM leads_with_opportunity_cte) AS leads_with_opportunity,
        ROUND(
            (COALESCE((SELECT leads_with_opportunity FROM leads_with_opportunity_cte), 0)::numeric
            / NULLIF((SELECT total_leads FROM leads_cte), 0)) * 100,
            2
        ) AS lead_to_opportunity_conversion_rate,
        (SELECT total_opportunities FROM opportunities_cte) AS total_opportunities,
        (SELECT opportunity_upsheets FROM opportunity_upsheets_cte) AS opportunity_upsheets,
        (SELECT delivered_upsheets FROM delivered_upsheets_cte) AS delivered_upsheets,
        (SELECT sold_upsheets FROM sold_upsheets_cte) AS sold_upsheets,
        (SELECT delivered_opportunity_upsheets FROM delivered_opportunity_upsheets_cte) AS delivered_opportunity_upsheets,
        ROUND(
            (COALESCE((SELECT delivered_opportunity_upsheets FROM delivered_opportunity_upsheets_cte), 0)::numeric
            / NULLIF((SELECT opportunity_upsheets FROM opportunity_upsheets_cte), 0)) * 100,
            2
        ) AS close_rate,
        (SELECT total_expected_amount FROM total_expected_amount_cte) AS total_expected_amount,
        (SELECT latest_expected_amount FROM latest_expected_amount_cte) AS latest_expected_amount,
        (SELECT total_sale_value FROM sale_value_cte) AS total_sale_value,
        (SELECT avg_sale_value FROM sale_value_cte) AS avg_sale_value,
        (SELECT realization_amount FROM realization_cte) AS realization_amount,
        (SELECT realization_percent FROM realization_cte) AS realization_percent;
    """

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (buyer_id, period_start.isoformat(), period_end.isoformat()))
        row = cur.fetchone()
    return dict(row or {})


def build_payload(
    query: str,
    buyer_id: int,
    year: int,
    quarter: int,
    period_start: date,
    period_end: date,
    kpis: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "source_mode": "precise",
        "query_type": "buyer_quarter_kpis",
        "params": {
            "buyer_id": buyer_id,
            "period_year": year,
            "period_quarter": quarter,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
        },
        "input_query": query,
        "result": kpis,
        "provenance": {
            "source_tables": ["buyers", "leads", "upsheets", "opportunities"],
            "execution_mode": "direct_sql",
            "database": os.environ.get("PRECISE_PG_DB", "ava_sandboxV2"),
        },
        "notes": {
            "safety": "This payload is produced by direct SQL against ava_sandboxV2; do not invent numbers.",
        },
    }


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Precise retrieval: compute buyer-quarter KPIs directly from base tables "
            "(ava_sandboxV2)."
        )
    )
    parser.add_argument("--query", type=str, required=True, help="Natural language query.")
    parser.add_argument("--buyer-id", type=int, default=None, help="Override buyer_id.")
    parser.add_argument("--year", type=int, default=None, help="Override year.")
    parser.add_argument("--quarter", type=int, default=None, help="Override quarter 1-4.")
    parser.add_argument("--start-date", type=str, default=None, help="Override start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", type=str, default=None, help="Override end date (YYYY-MM-DD).")

    args = parser.parse_args()
    q = args.query.strip()

    buyer_id = args.buyer_id if args.buyer_id is not None else parse_buyer_id(q)
    if buyer_id is None:
        raise SystemExit("Could not parse buyer_id. Use 'Buyer <number>' or pass --buyer-id.")

    # Determine window
    if args.start_date and args.end_date:
        start = try_parse_date(args.start_date)
        end = try_parse_date(args.end_date)
        if not start or not end:
            raise SystemExit("Could not parse --start-date/--end-date.")
        period_start, period_end = start, end
        # Derive year/quarter labels from start date for reporting
        year = period_start.year
        quarter = ((period_start.month - 1) // 3) + 1
    else:
        year = args.year
        quarter = args.quarter
        if year is None or quarter is None:
            py, pq = parse_quarter(q)
            year = year or py
            quarter = quarter or pq
        if year is None or quarter is None:
            raise SystemExit("Could not determine year/quarter. Provide 'Q<1-4> <year>' or pass --year/--quarter.")
        period_start, period_end = quarter_date_range(year, quarter)

    cfg = PgConfig()
    logger.info(
        "Running precise KPI SQL on %s:%d/%s for buyer_id=%d, period=%s..%s",
        cfg.host,
        cfg.port,
        cfg.dbname,
        buyer_id,
        period_start.isoformat(),
        period_end.isoformat(),
    )

    conn = connect_pg(cfg)
    try:
        kpis = fetch_buyer_quarter_kpis(conn, buyer_id, period_start, period_end)
    finally:
        conn.close()

    payload = build_payload(q, buyer_id, year, quarter, period_start, period_end, kpis)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()

