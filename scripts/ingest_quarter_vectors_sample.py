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


logger = logging.getLogger(__name__)
from dotenv import load_dotenv
load_dotenv()

AVA_EMBEDDINGS_URL = "https://ava.andrew-chat.com/api/v1/embeddings"
EMBEDDING_DIMENSION = 3072


@dataclass
class PgConfig:
    host: str = "localhost"
    port: int = 5433
    dbname: str = "ava_vectors"
    user: str = "postgres"
    password: str = "postgres"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def load_docs(path: Path, limit: int) -> List[Dict[str, Any]]:
    """Load up to `limit` JSON lines from the vector docs file."""
    docs: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as exc:
                logger.warning("Skipping malformed JSON line: %s", exc)
                continue
            docs.append(doc)
            if len(docs) >= limit:
                break
    return docs


def get_ava_token_from_env() -> str:
    """Read Ava token from environment."""
    token = os.environ.get("AVA_TOKEN")
    if not token:
        raise RuntimeError(
            "AVA_TOKEN environment variable is not set. "
            "Export your Ava auth token as AVA_TOKEN before running."
        )
    return token


def fetch_embeddings(texts: Sequence[str], token: str) -> List[List[float]]:
    """Call Ava embeddings endpoint for a small batch of texts."""
    if not texts:
        return []

    resp = requests.post(
        AVA_EMBEDDINGS_URL,
        headers={
            "Authorization": token,  # raw token, no 'Bearer ' prefix
            "Content-Type": "application/json",
        },
        json={
            "texts": list(texts),
            "type": "RETRIEVAL_DOCUMENT",
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    matrix = data.get("embeddings_matrix")
    if not isinstance(matrix, list):
        raise RuntimeError("Unexpected embeddings_matrix format from Ava.")
    return matrix


def to_vector_literal(values: Sequence[float]) -> str:
    """
    Convert a numeric sequence into a pgvector literal string.

    Example: [0.1, 0.2] -> "[0.1,0.2]"
    """
    return "[" + ",".join(str(float(v)) for v in values) + "]"


def connect_pg(cfg: PgConfig):
    """Create a psycopg2 connection."""
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    conn.autocommit = False
    return conn


def upsert_vectors(
    conn,
    docs: Sequence[Dict[str, Any]],
    embeddings: Sequence[Sequence[float]],
) -> None:
    """Insert or update rows in buyer_quarter_vectors with embeddings."""
    if len(docs) != len(embeddings):
        raise ValueError("docs and embeddings lengths do not match.")

    sql = """
    INSERT INTO buyer_quarter_vectors (
        doc_id,
        summary_level,
        buyer_id,
        buyer_name,
        period_year,
        period_quarter,
        period_start,
        period_end,
        period_label,
        payload,
        embedding
    ) VALUES (
        %(doc_id)s,
        %(summary_level)s,
        %(buyer_id)s,
        %(buyer_name)s,
        %(period_year)s,
        %(period_quarter)s,
        %(period_start)s,
        %(period_end)s,
        %(period_label)s,
        %(payload)s,
        %(embedding)s::vector
    )
    ON CONFLICT (doc_id) DO UPDATE SET
        summary_level   = EXCLUDED.summary_level,
        buyer_id        = EXCLUDED.buyer_id,
        buyer_name      = EXCLUDED.buyer_name,
        period_year     = EXCLUDED.period_year,
        period_quarter  = EXCLUDED.period_quarter,
        period_start    = EXCLUDED.period_start,
        period_end      = EXCLUDED.period_end,
        period_label    = EXCLUDED.period_label,
        payload         = EXCLUDED.payload,
        embedding       = EXCLUDED.embedding;
    """

    with conn.cursor() as cur:
        for doc, emb in zip(docs, embeddings):
            buyer = doc.get("buyer", {}) or {}
            period = doc.get("period", {}) or {}

            params = {
                "doc_id": doc.get("doc_id"),
                "summary_level": doc.get("summary_level", "quarter"),
                "buyer_id": buyer.get("assigned_user_id"),
                "buyer_name": buyer.get("buyer_full_name") or "",
                "period_year": period.get("period_year"),
                "period_quarter": period.get("period_quarter"),
                "period_start": period.get("period_start"),
                "period_end": period.get("period_end"),
                "period_label": period.get("period_label"),
                "payload": psycopg2.extras.Json(doc),
                "embedding": to_vector_literal(emb),
            }

            cur.execute(sql, params)

    conn.commit()


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Embed a small batch of buyer-quarter docs with Ava and "
            "load them into buyer_quarter_vectors (pgvector)."
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default="KPIs/large_seed_res/buyer_quarter_vector_docs.jsonl",
        help="Path to the JSONL file of buyer-quarter vector docs.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Maximum number of docs to embed and load (e.g., 1, 5, 20).",
    )
    parser.add_argument(
        "--pg-host",
        type=str,
        default="localhost",
        help="Postgres host for ava_vectors.",
    )
    parser.add_argument(
        "--pg-port",
        type=int,
        default=5433,
        help="Postgres port for ava_vectors.",
    )
    parser.add_argument(
        "--pg-db",
        type=str,
        default="ava_vectors",
        help="Postgres database name.",
    )
    parser.add_argument(
        "--pg-user",
        type=str,
        default="postgres",
        help="Postgres user.",
    )
    parser.add_argument(
        "--pg-password",
        type=str,
        default="postgres",
        help="Postgres password.",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error("Input JSONL file not found: %s", input_path)
        raise SystemExit(1)

    docs = load_docs(input_path, args.limit)
    if not docs:
        logger.warning("No docs loaded from %s", input_path)
        return

    logger.info("Loaded %d docs from %s", len(docs), input_path)

    token = get_ava_token_from_env()

    valid_docs: List[Dict[str, Any]] = []
    texts: List[str] = []
    for d in docs:
        text = (d.get("embedding_text") or "").strip()
        if not text:
            logger.warning("Skipping doc with empty embedding_text: %s", d.get("doc_id"))
            continue
        valid_docs.append(d)
        texts.append(text)

    docs = valid_docs
    if not docs:
        logger.warning("All loaded docs had empty embedding_text; nothing to embed/load.")
        return

    logger.info("Requesting embeddings for %d texts from Ava...", len(texts))
    embeddings = fetch_embeddings(texts, token)
    logger.info("Received embeddings for %d texts.", len(embeddings))

    if len(embeddings) != len(docs):
        raise RuntimeError(
            f"Embedding count mismatch: got {len(embeddings)} for {len(docs)} docs"
        )
    for i, emb in enumerate(embeddings):
        if len(emb) != EMBEDDING_DIMENSION:
            raise RuntimeError(
                f"Embedding at index {i} has dimension {len(emb)} instead of {EMBEDDING_DIMENSION}"
            )

    pg_cfg = PgConfig(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_password,
    )
    conn = connect_pg(pg_cfg)
    try:
        upsert_vectors(conn, docs, embeddings)
    finally:
        conn.close()

    logger.info("Successfully upserted %d docs into buyer_quarter_vectors.", len(docs))


if __name__ == "__main__":
    main()

