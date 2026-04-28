from __future__ import annotations

import os
from collections import defaultdict

import psycopg2
from dotenv import load_dotenv


def main() -> None:
    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv("PRECISE_PG_HOST", "localhost"),
        port=int(os.getenv("PRECISE_PG_PORT", "5432")),
        dbname=os.getenv("PRECISE_PG_DB", "ava_sandboxV2"),
        user=os.getenv("PRECISE_PG_USER", "postgres"),
        password=os.getenv("PRECISE_PG_PASSWORD", ""),
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        select assigned_user_id as buyer_id, count(*) as upsheets
        from upsheets
        group by 1
        order by upsheets desc, buyer_id asc
        limit 15;
        """
    )
    print("Top buyers by upsheets (overall):")
    top_buyers = cur.fetchall()
    for buyer_id, cnt in top_buyers:
        print(f"- Buyer {buyer_id}: {cnt}")

    # For the top few buyers, show quarterly distribution to pick a demo quarter that splits well.
    buyers = [int(b) for b, _ in top_buyers[:5]]
    if buyers:
        cur.execute(
            """
            select
              assigned_user_id as buyer_id,
              extract(year from insert_date)::int as y,
              extract(quarter from insert_date)::int as q,
              count(*) as c
            from upsheets
            where assigned_user_id = any(%s)
            group by 1,2,3
            order by c desc, buyer_id asc, y asc, q asc
            limit 25;
            """,
            (buyers,),
        )
        print("\nTop quarters for top buyers:")
        for buyer_id, y, q, c in cur.fetchall():
            print(f"- Buyer {buyer_id} Q{q} {y}: {c}")

    cur.execute(
        """
        select assigned_user_id as buyer_id, count(*) as opportunities
        from opportunities o
        join upsheets u on u.upsheet_id = o.upsheet_id
        group by 1
        order by opportunities desc, buyer_id asc
        limit 15;
        """
    )
    print("\nTop buyers by opportunities (overall):")
    top_opps = cur.fetchall()
    for buyer_id, cnt in top_opps:
        print(f"- Buyer {buyer_id}: {cnt}")

    opp_buyers = [int(b) for b, _ in top_opps[:5]]
    if opp_buyers:
        cur.execute(
            """
            select
              u.assigned_user_id as buyer_id,
              extract(year from o.created_at)::int as y,
              extract(quarter from o.created_at)::int as q,
              count(*) as c
            from opportunities o
            join upsheets u on u.upsheet_id = o.upsheet_id
            where u.assigned_user_id = any(%s)
            group by 1,2,3
            order by c desc, buyer_id asc, y asc, q asc
            limit 25;
            """,
            (opp_buyers,),
        )
        print("\nTop quarters for top opportunity buyers:")
        for buyer_id, y, q, c in cur.fetchall():
            print(f"- Buyer {buyer_id} Q{q} {y}: {c}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

