import argparse
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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
        description="Semantic search over Pinecone buyer-quarter KPI vectors."
    )
    parser.add_argument("--query", type=str, required=True, help="Natural language query.")
    parser.add_argument("--top-k", type=int, default=5, help="Top K matches to return.")
    parser.add_argument(
        "--namespace",
        type=str,
        default="",
        help="Optional Pinecone namespace (leave blank for default).",
    )
    parser.add_argument(
        "--include-values",
        action="store_true",
        help="Include raw vector values in the response (not recommended).",
    )

    args = parser.parse_args()
    q = args.query.strip()
    if not q:
        raise SystemExit("Query must not be empty.")

    ava_token = get_ava_token_from_env()
    pc_cfg = load_pinecone_config()
    index = ensure_index(pc_cfg)

    logger.info(
        "Embedding query with Ava (type=RETRIEVAL_QUERY), Pinecone index=%s, env=%s",
        pc_cfg.index_name,
        pc_cfg.environment,
    )
    query_vec = embed_query(q, ava_token)

    logger.info("Querying Pinecone top_k=%d...", args.top_k)
    res = index.query(
        vector=query_vec,
        top_k=args.top_k,
        include_metadata=True,
        include_values=bool(args.include_values),
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
        buyer_id = meta.get("buyer_id", "")

        score_str = "N/A" if score is None else f"{float(score):.4f}"
        print(f"{i}. id={mid} score={score_str}")
        print(f"   buyer_id={buyer_id} buyer_name={buyer_name} period={period_label}")
        snippet = short_snippet(meta)
        if snippet:
            print(f"   summary: {snippet}")
        print()


if __name__ == "__main__":
    main()

