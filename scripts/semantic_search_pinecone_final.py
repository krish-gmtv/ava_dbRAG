import argparse
import itertools
import json
import logging
import numbers
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from intent_router_v1 import (
    infer_quarter_from_date_range,
    parse_between_dates,
)

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
    last_exc: Optional[Exception] = None
    for attempt in range(1, 3):  # one retry for transient timeout/network issues
        try:
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
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            logger.warning(
                "Ava embeddings request failed (attempt %d/2): %s",
                attempt,
                exc,
            )
            if attempt == 2:
                raise RuntimeError(
                    "Ava embeddings request timed out after retry. "
                    "Please retry the query or use a precise query."
                ) from exc
            continue
    else:  # pragma: no cover
        raise RuntimeError(f"Ava embeddings request failed: {last_exc}")

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
    - '... 2018' -> bare year
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

    # Date range -> quarter when contained in a single quarter (e.g. Jan–Mar 2018 -> Q1 2018)
    if period_quarter is None:
        sd, ed, _ = parse_between_dates(query)
        if sd and ed:
            py, pq = infer_quarter_from_date_range(sd, ed)
            if py is not None:
                period_year = py
            if pq is not None:
                period_quarter = pq

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
    *,
    allow_buyer_only_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    Build a sequence of {filter, top_k} according to v1 policy.
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

        if allow_buyer_only_fallback and buyer_id is not None:
            policies.append(
                {
                    "filter": {"buyer_id": buyer_id},
                    "top_k": 8,
                    "_fallback": "buyer_only",
                }
            )

    elif case == "buyer_year":
        # Primary: buyer + year
        primary_filter = {"buyer_id": buyer_id, "period_year": period_year}
        policies.append({"filter": primary_filter, "top_k": 8})

        if allow_buyer_only_fallback and buyer_id is not None:
            policies.append(
                {
                    "filter": {"buyer_id": buyer_id},
                    "top_k": 8,
                    "_fallback": "buyer_only",
                }
            )

    elif case == "buyer_only":
        # Just buyer across all quarters/years
        primary_filter = {"buyer_id": buyer_id}
        policies.append({"filter": primary_filter, "top_k": 4})

    else:
        # No structured info -> pure semantic
        policies.append({"filter": None, "top_k": cli_top_k})

    return policies


def env_flag(name: str, default: bool = True) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def metadata_filter_variants(flt: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Pinecone metadata often stores buyer_id / period_* as strings while the router
    passes ints. Try both so typed filters are not silently empty.
    """
    typed_keys = ("buyer_id", "period_year", "period_quarter")
    variant_dims: List[List[Tuple[str, Any]]] = []
    base: Dict[str, Any] = {}
    for k, v in flt.items():
        if (
            k in typed_keys
            and isinstance(v, numbers.Integral)
            and not isinstance(v, bool)
        ):
            variant_dims.append([(k, int(v)), (k, str(int(v)))])
        else:
            base[k] = v
    if not variant_dims:
        return [{**flt}]
    branches: List[Dict[str, Any]] = []
    for combo in itertools.product(*variant_dims):
        d = dict(base)
        for key, val in combo:
            d[key] = val
        branches.append(d)
    seen: set = set()
    uniq: List[Dict[str, Any]] = []
    for d in branches:
        # repr preserves int vs str (str("2") must not collapse with int 2).
        sig = tuple(sorted((k, repr(v)) for k, v in d.items()))
        if sig not in seen:
            seen.add(sig)
            uniq.append(d)
    return uniq


def short_snippet(metadata: Dict[str, Any], max_len: int = 200) -> str:
    txt = (metadata.get("executive_summary") or "").strip()
    if not txt:
        txt = (metadata.get("embedding_text") or "").strip()
    if not txt:
        return ""
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    if not lines:
        return ""

    primary = lines[0]
    secondary = ""
    # Prefer a second line that gives more discriminative business signal.
    for ln in lines[1:]:
        lnl = ln.lower()
        if any(
            kw in lnl
            for kw in (
                "generated",
                "value generation",
                "conversion",
                "execution",
                "realized sale value",
                "pricing performance",
            )
        ):
            secondary = ln
            break

    snippet = f"{primary} {secondary}".strip() if secondary else primary
    if len(snippet) <= max_len:
        return snippet
    return snippet[: max_len - 3] + "..."


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Final semantic retrieval: policy-based Pinecone search that returns normalized JSON, "
            "suitable for Ava to synthesize answers without inventing numbers."
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
    parser.add_argument(
        "--period-year",
        type=int,
        default=None,
        help="Override parsed period year (from router execution plan).",
    )
    parser.add_argument(
        "--period-quarter",
        type=int,
        default=None,
        help="Override parsed period quarter 1-4 (from router execution plan).",
    )

    args = parser.parse_args()
    q = args.query.strip()
    if not q:
        raise SystemExit("Query must not be empty.")

    ava_token = get_ava_token_from_env()
    pc_cfg = load_pinecone_config()
    index = ensure_index(pc_cfg)

    buyer_id, period_year, period_quarter = parse_buyer_and_period(q)
    if args.period_year is not None:
        period_year = args.period_year
    if args.period_quarter is not None:
        period_quarter = args.period_quarter
    case = classify_case(buyer_id, period_year, period_quarter)
    allow_buyer_fb = env_flag("SEMANTIC_BUYER_ONLY_FALLBACK", default=True)
    policy_seq = build_policy_sequence(
        case,
        buyer_id,
        period_year,
        period_quarter,
        args.top_k,
        allow_buyer_only_fallback=allow_buyer_fb,
    )

    logger.info(
        "Embedding query with Ava (type=RETRIEVAL_QUERY), Pinecone index=%s, env=%s",
        pc_cfg.index_name,
        pc_cfg.environment,
    )
    logger.info(
        "Parsed case=%s, buyer_id=%r, period_year=%r, period_quarter=%r",
        case,
        buyer_id,
        period_year,
        period_quarter,
    )
    query_vec = embed_query(q, ava_token)

    all_matches: List[Any] = []
    used_policy: Optional[Dict[str, Any]] = None

    for policy in policy_seq:
        flt = policy["filter"]
        top_k = policy["top_k"]

        filter_attempts: List[Optional[Dict[str, Any]]] = (
            [None] if flt is None else metadata_filter_variants(flt)
        )
        for concrete_flt in filter_attempts:
            res = index.query(
                vector=query_vec,
                top_k=top_k,
                include_metadata=True,
                include_values=False,
                filter=concrete_flt,
                namespace=args.namespace or None,
            )
            matches = getattr(res, "matches", None) or []
            if matches:
                all_matches = matches
                used_policy = policy
                break
        if all_matches:
            break

    payload: Dict[str, Any] = {
        "source_mode": "semantic",
        "query_type": "buyer_performance_summary",
        "input_query": q,
        "params": {
            "buyer_id": buyer_id,
            "period_year": period_year,
            "period_quarter": period_quarter,
            "case": case,
        },
        "result": {
            "match_count": len(all_matches),
            "matches": [],
        },
        "provenance": {
            "source_system": "pinecone",
            "index_name": pc_cfg.index_name,
            "policy_filter_used": (used_policy or {}).get("filter"),
            "top_k_used": (used_policy or {}).get("top_k", args.top_k),
            "retrieval_fallback": (used_policy or {}).get("_fallback"),
        },
    }

    for m in all_matches:
        meta = getattr(m, "metadata", None) or {}
        score = getattr(m, "score", None)
        mid = getattr(m, "id", None)
        buyer_name = meta.get("buyer_name", "")
        period_label = meta.get("period_label", "")
        buyer_id_meta = meta.get("buyer_id", "")

        snippet = short_snippet(meta)
        payload["result"]["matches"].append(
            {
                "id": mid,
                "score": float(score) if score is not None else None,
                "buyer_id": buyer_id_meta,
                "buyer_name": buyer_name,
                "period_label": period_label,
                "summary_snippet": snippet,
            }
        )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

