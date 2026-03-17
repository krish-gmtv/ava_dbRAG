import argparse
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

try:
    from pinecone import Pinecone
except ImportError:  # pragma: no cover
    Pinecone = None  # type: ignore


load_dotenv()
logger = logging.getLogger(__name__)

AVA_EMBEDDINGS_URL = "https://ava.andrew-chat.com/api/v1/embeddings"
EMBEDDING_DIMENSION = 3072


@dataclass
class PineconeConfig:
    api_key: str
    index_name: str
    environment: str


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
            "Add it to .env or export it before running."
        )
    return token


def load_pinecone_config() -> PineconeConfig:
    api_key = os.environ.get("PINECONE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "PINECONE_API_KEY environment variable is not set. "
            "Add it to .env or export it before running."
        )
    env = os.environ.get("PINECONE_ENVIRONMENT", "us-east-1-aws")
    index_name = os.environ.get("PINECONE_INDEX_NAME", "buyer-quarter-vectors")
    return PineconeConfig(api_key=api_key, index_name=index_name, environment=env)


def ensure_index(cfg: PineconeConfig):
    if Pinecone is None:
        raise RuntimeError(
            "pinecone client library is not installed. Install it with `pip install pinecone`."
        )
    pc = Pinecone(api_key=cfg.api_key)
    return pc.Index(cfg.index_name)


def embed_query(query: str, token: str) -> List[float]:
    resp = requests.post(
        AVA_EMBEDDINGS_URL,
        headers={
            "Authorization": token,  # raw token, no Bearer
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


def parse_buyer_and_period(query: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Very simple parser for:
    - 'Buyer 1'  -> buyer_id = 1
    - 'Q1 2018'  -> period_quarter = 1, period_year = 2018
    Returns (buyer_id, period_year, period_quarter).
    """
    buyer_id: Optional[int] = None
    period_year: Optional[int] = None
    period_quarter: Optional[int] = None

    # Buyer N
    m = re.search(r"\bBuyer\s+(\d+)\b", query, re.IGNORECASE)
    if m:
        try:
            buyer_id = int(m.group(1))
        except ValueError:
            buyer_id = None

    # QN YYYY (quarter and year)
    m = re.search(r"\bQ([1-4])\s+(\d{4})\b", query, re.IGNORECASE)
    if m:
        try:
            period_quarter = int(m.group(1))
            period_year = int(m.group(2))
        except ValueError:
            period_quarter = None
            period_year = None

    return buyer_id, period_year, period_quarter


def build_filter(
    buyer_id: Optional[int],
    period_year: Optional[int],
    period_quarter: Optional[int],
) -> Dict[str, Any]:
    flt: Dict[str, Any] = {}
    if buyer_id is not None:
        flt["buyer_id"] = buyer_id
    if period_year is not None:
        flt["period_year"] = period_year
    if period_quarter is not None:
        flt["period_quarter"] = period_quarter
    return flt


def short_snippet(metadata: Dict[str, Any], max_len: int = 200) -> str:
    txt = (metadata.get("executive_summary") or "").strip()
    if not txt:
        txt = (metadata.get("embedding_text") or "").strip()
    if not txt:
        return ""
    first_line = txt.splitlines()[0]
    if len(first_line) <= max_len:
        return first_line
    return first_line[: max_len - 3] + "..."


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Metadata-filtered semantic search over Pinecone buyer-quarter KPI vectors.\n"
            "Parses 'Buyer N' and 'Qx YYYY' from the query to build a Pinecone filter."
        )
    )
    parser.add_argument("--query", type=str, required=True, help="Natural language query.")
    parser.add_argument("--top-k", type=int, default=5, help="Top K matches to return.")
    parser.add_argument(
        "--namespace",
        type=str,
        default="",
        help="Optional Pinecone namespace (leave blank for default).",
    )

    args = parser.parse_args()
    q = args.query.strip()
    if not q:
        raise SystemExit("Query must not be empty.")

    ava_token = get_ava_token_from_env()
    pc_cfg = load_pinecone_config()
    index = ensure_index(pc_cfg)

    buyer_id, period_year, period_quarter = parse_buyer_and_period(q)
    flt = build_filter(buyer_id, period_year, period_quarter)

    logger.info(
        "Embedding query with Ava (type=RETRIEVAL_QUERY), Pinecone index=%s, env=%s",
        pc_cfg.index_name,
        pc_cfg.environment,
    )
    if flt:
        logger.info("Using Pinecone metadata filter: %s", flt)
    else:
        logger.info("No metadata filter inferred from query; running global semantic search.")

    query_vec = embed_query(q, ava_token)

    logger.info("Querying Pinecone top_k=%d...", args.top_k)
    res = index.query(
        vector=query_vec,
        top_k=args.top_k,
        include_metadata=True,
        include_values=False,
        filter=flt if flt else None,
        namespace=args.namespace or None,
    )

    matches = getattr(res, "matches", None) or []
    if not matches:
        print("No matches returned.")
        return

    print(f"\nTop {len(matches)} matches for query: {q!r}\n")
    for i, m in enumerate(matches, start=1):
        meta = getattr(m, "metadata", None) or {}
        score = getattr(m, "score", None)
        mid = getattr(m, "id", None)
        buyer_name = meta.get("buyer_name", "")
        period_label = meta.get("period_label", "")
        buyer_id_meta = meta.get("buyer_id", "")

        score_str = "N/A" if score is None else f"{float(score):.4f}"
        print(f"{i}. id={mid} score={score_str}")
        print(
            f"   buyer_id={buyer_id_meta} buyer_name={buyer_name} period={period_label}"
        )
        snippet = short_snippet(meta)
        if snippet:
            print(f"   summary: {snippet}")
        print()


if __name__ == "__main__":
    main()

