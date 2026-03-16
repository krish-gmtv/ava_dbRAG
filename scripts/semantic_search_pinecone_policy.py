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
    Very simple parser:
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

    # Bare year, e.g. "in 2018" or "2019" (only if year not already set)
    if period_year is None:
        m = re.search(r"\b(19\d{2}|20\d{2})\b", query)
        if m:
            try:
                period_year = int(m.group(1))
            except ValueError:
                period_year = None

    return buyer_id, period_year, period_quarter


def classify_case(
    buyer_id: Optional[int],
    period_year: Optional[int],
    period_quarter: Optional[int],
) -> str:
    """
    Return one of:
    - 'buyer_quarter_year'
    - 'buyer_year'
    - 'buyer_only'
    - 'none'
    """
    if buyer_id is not None and period_year is not None and period_quarter is not None:
        return "buyer_quarter_year"
    if buyer_id is not None and period_year is not None:
        return "buyer_year"
    if buyer_id is not None:
        return "buyer_only"
    return "none"


def build_policy_sequence(
    case: str,
    buyer_id: Optional[int],
    period_year: Optional[int],
    period_quarter: Optional[int],
    cli_top_k: int,
) -> List[Dict[str, Any]]:
    """
    Build a sequence of {filter, top_k} according to your v1 policy.
    """
    policies: List[Dict[str, Any]] = []

    if case == "buyer_quarter_year":
        # Primary: buyer + year + quarter
        primary_filter: Dict[str, Any] = {
            "buyer_id": buyer_id,
            "period_year": period_year,
            "period_quarter": period_quarter,
        }
        policies.append({"filter": primary_filter, "top_k": 1})

        # Fallback 1: buyer + year
        fallback1 = {"buyer_id": buyer_id, "period_year": period_year}
        policies.append({"filter": fallback1, "top_k": 4})

        # Fallback 2: buyer only
        fallback2 = {"buyer_id": buyer_id}
        policies.append({"filter": fallback2, "top_k": 4})

    elif case == "buyer_year":
        # Primary: buyer + year
        primary_filter = {"buyer_id": buyer_id, "period_year": period_year}
        policies.append({"filter": primary_filter, "top_k": 4})

        # Fallback: buyer only
        fallback = {"buyer_id": buyer_id}
        policies.append({"filter": fallback, "top_k": 4})

    elif case == "buyer_only":
        # Just buyer across all quarters/years
        primary_filter = {"buyer_id": buyer_id}
        policies.append({"filter": primary_filter, "top_k": 4})

    else:
        # No structured info -> pure semantic
        policies.append({"filter": None, "top_k": cli_top_k})

    return policies


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
            "Policy-based semantic search over Pinecone buyer-quarter KPI vectors.\n"
            "Implements Buyer+Quarter, Buyer+Year, Buyer-only, and global search policies."
        )
    )
    parser.add_argument("--query", type=str, required=True, help="Natural language query.")
    parser.add_argument("--top-k", type=int, default=5, help="Top K matches to return (for global case).")
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
    case = classify_case(buyer_id, period_year, period_quarter)
    policy_seq = build_policy_sequence(case, buyer_id, period_year, period_quarter, args.top_k)

    logger.info(
        "Embedding query with Ava (type=RETRIEVAL_QUERY), Pinecone index=%s, env=%s",
        pc_cfg.index_name,
        pc_cfg.environment,
    )
    logger.info("Parsed case=%s, buyer_id=%r, period_year=%r, period_quarter=%r", case, buyer_id, period_year, period_quarter)
    query_vec = embed_query(q, ava_token)

    all_matches: List[Any] = []
    used_policy: Optional[Dict[str, Any]] = None

    for policy in policy_seq:
        flt = policy["filter"]
        top_k = policy["top_k"]
        if flt:
            logger.info("Trying policy filter=%s, top_k=%d", flt, top_k)
        else:
            logger.info("Trying global semantic search, top_k=%d", top_k)

        res = index.query(
            vector=query_vec,
            top_k=top_k,
            include_metadata=True,
            include_values=False,
            filter=flt or None,
            namespace=args.namespace or None,
        )

        matches = getattr(res, "matches", None) or []
        if matches:
            all_matches = matches
            used_policy = policy
            break

    if not all_matches:
        print("No matches returned from any policy.")
        return

    print(f"\nTop {len(all_matches)} matches for query: {q!r}\n")
    if used_policy and used_policy.get("filter"):
        print(f"Policy filter used: {used_policy['filter']}, top_k={used_policy['top_k']}\n")
    else:
        print("Policy used: global semantic search\n")

    for i, m in enumerate(all_matches, start=1):
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