import argparse
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)

AVA_EMBEDDINGS_URL = "https://ava.andrew-chat.com/api/v1/embeddings"
EMBEDDING_DIMENSION = 3072


@dataclass
class PgConfig:
    host: str = os.environ.get("PGVECTOR_HOST", "localhost")
    port: int = int(os.environ.get("PGVECTOR_PORT", "5433"))
    dbname: str = os.environ.get("PGVECTOR_DB", "ava_vectors")
    user: str = os.environ.get("PGVECTOR_USER", "postgres")
    password: str = os.environ.get("PGVECTOR_PASSWORD", "postgres")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def get_ava_token_from_env() -> str:
    token = os.environ.get("AVA_TOKEN")
    if not token:
        raise RuntimeError(
            "AVA_TOKEN environment variable is not set. "
            "Export your Ava auth token as AVA_TOKEN or add it to .env."
        )
    return token


def to_vector_literal(values: Sequence[float]) -> str:
    return "[" + ",".join(str(float(v)) for v in values) + "]"


def embed_query_with_ava(query: str, token: str) -> List[float]:
    """Call Ava embeddings endpoint for a single natural-language query."""
    resp = requests.post(
        AVA_EMBEDDINGS_URL,
        headers={
            "Authorization": token,  # raw token, no "Bearer "
            "Content-Type": "application/json",
        },
        json={
            "texts": [query],
            "type": "RETRIEVAL_QUERY",
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    matrix = data.get("embeddings_matrix")
    if not isinstance(matrix, list) or not matrix:
        raise RuntimeError("Unexpected embeddings_matrix format from Ava.")
    emb = matrix[0]
    if len(emb) != EMBEDDING_DIMENSION:
        raise RuntimeError(
            f"Query embedding dimension {len(emb)} != expected {EMBEDDING_DIMENSION}"
        )
    return emb


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


def search_buyers(
    conn,
    query_embedding: Sequence[float],
    top_k: int,
) -> List[Dict[str, Any]]:
    vector_literal = to_vector_literal(query_embedding)

    sql = """
    WITH q AS (
        SELECT %s::vector AS embedding
    )
    SELECT
        v.doc_id,
        v.buyer_id,
        v.buyer_name,
        v.period_year,
        v.period_quarter,
        v.period_label,
        1 - (v.embedding <=> q.embedding) AS similarity,
        v.payload
    FROM buyer_quarter_vectors v, q
    ORDER BY v.embedding <=> q.embedding
    LIMIT %s;
    """

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (vector_literal, top_k))
        rows = cur.fetchall()

    results: List[Dict[str, Any]] = []
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        interpretation = (payload or {}).get("interpretation", {}) or {}
        exec_summary = interpretation.get("executive_summary") or ""
        embedding_text = (payload or {}).get("embedding_text") or ""

        results.append(
            {
                "doc_id": row["doc_id"],
                "buyer_id": row["buyer_id"],
                "buyer_name": row["buyer_name"],
                "period_label": row["period_label"],
                "similarity": float(row["similarity"]) if row["similarity"] is not None else None,
                "executive_summary": exec_summary,
                "embedding_text": embedding_text,
            }
        )

    return results


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Run a semantic search over buyer_quarter_vectors using Ava embeddings "
            "and pgvector."
        )
    )
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="Natural language query to search for.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of top similar buyer-quarter docs to return.",
    )

    args = parser.parse_args()

    query_text = args.query.strip()
    if not query_text:
        raise SystemExit("Query text must not be empty.")

    token = get_ava_token_from_env()
    logger.info("Embedding query with Ava...")
    query_embedding = embed_query_with_ava(query_text, token)
    logger.info("Received query embedding with dimension %d.", len(query_embedding))

    pg_cfg = PgConfig()
    conn = connect_pg(pg_cfg)
    try:
        logger.info(
            "Searching buyer_quarter_vectors for top %d similar docs...", args.top_k
        )
        results = search_buyers(conn, query_embedding, args.top_k)
    finally:
        conn.close()

    if not results:
        print("No matching buyer-quarter documents found.")
        return

    print(f"\nTop {len(results)} matches for query: {query_text!r}\n")
    for i, r in enumerate(results, start=1):
        sim_str = "N/A" if r["similarity"] is None else f"{r['similarity']:.4f}"
        print(f"{i}. doc_id={r['doc_id']}")
        print(
            f"   buyer_id={r['buyer_id']}, buyer_name={r['buyer_name']}, "
            f"period={r['period_label']}, similarity={sim_str}"
        )
        if r["executive_summary"]:
            snippet = r["executive_summary"].splitlines()[0]
        elif r["embedding_text"]:
            snippet = r["embedding_text"][:160]
        else:
            snippet = ""
        if snippet:
            print(f"   summary: {snippet}")
        print()


if __name__ == "__main__":
    main()

