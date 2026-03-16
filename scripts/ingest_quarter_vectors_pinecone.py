import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence

import requests
from dotenv import load_dotenv

try:
    from pinecone import Pinecone
except ImportError:  # pragma: no cover - import hint
    Pinecone = None  # type: ignore


load_dotenv()
logger = logging.getLogger(__name__)

AVA_EMBEDDINGS_URL = "https://ava.andrew-chat.com/api/v1/embeddings"
EMBEDDING_DIMENSION = 3072


@dataclass
class AvaConfig:
    token: str


@dataclass
class PineconeConfig:
    api_key: str
    environment: str
    index_name: str


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def load_ava_config() -> AvaConfig:
    token = os.environ.get("AVA_TOKEN")
    if not token:
        raise RuntimeError(
            "AVA_TOKEN environment variable is not set. "
            "Add it to your .env or export it before running."
        )
    return AvaConfig(token=token)


def load_pinecone_config() -> PineconeConfig:
    api_key = os.environ.get("PINECONE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "PINECONE_API_KEY environment variable is not set. "
            "Add it to your .env or export it before running."
        )
    environment = os.environ.get("PINECONE_ENVIRONMENT", "us-east-1-aws")
    index_name = os.environ.get("PINECONE_INDEX_NAME", "buyer-quarter-vectors")
    return PineconeConfig(api_key=api_key, environment=environment, index_name=index_name)


def ensure_pinecone_client(cfg: PineconeConfig):
    if Pinecone is None:
        raise RuntimeError(
            "pinecone client library is not installed. "
            "Install it with `pip install pinecone`."
        )
    # New Pinecone client automatically uses the correct environment/host
    # based on your account; environment is informational here.
    pc = Pinecone(api_key=cfg.api_key)
    index = pc.Index(cfg.index_name)
    return index


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


def fetch_document_embeddings(
    texts: Sequence[str],
    ava_cfg: AvaConfig,
) -> List[List[float]]:
    """Call Ava embeddings endpoint for a batch of document texts."""
    if not texts:
        return []

    resp = requests.post(
        AVA_EMBEDDINGS_URL,
        headers={
            "Authorization": ava_cfg.token,  # raw token, no "Bearer "
            "Content-Type": "application/json",
        },
        json={
            "texts": list(texts),
            "type": "RETRIEVAL_DOCUMENT",
        },
        timeout=90,
    )
    resp.raise_for_status()
    data = resp.json()
    matrix = data.get("embeddings_matrix")
    if not isinstance(matrix, list):
        raise RuntimeError("Unexpected embeddings_matrix format from Ava.")
    return matrix


def build_pinecone_vectors(
    docs: Sequence[Dict[str, Any]],
    embeddings: Sequence[Sequence[float]],
) -> List[Dict[str, Any]]:
    if len(docs) != len(embeddings):
        raise RuntimeError(
            f"Embedding count mismatch: got {len(embeddings)} for {len(docs)} docs"
        )

    vectors: List[Dict[str, Any]] = []
    for i, (doc, emb) in enumerate(zip(docs, embeddings)):
        if len(emb) != EMBEDDING_DIMENSION:
            raise RuntimeError(
                f"Embedding at index {i} has dimension {len(emb)} "
                f"instead of {EMBEDDING_DIMENSION}"
            )

        buyer = doc.get("buyer", {}) or {}
        period = doc.get("period", {}) or {}

        metadata: Dict[str, Any] = {
            "summary_level": doc.get("summary_level"),
            "buyer_id": buyer.get("assigned_user_id"),
            "buyer_name": buyer.get("buyer_full_name"),
            "period_year": period.get("period_year"),
            "period_quarter": period.get("period_quarter"),
            "period_label": period.get("period_label"),
            "period_start": period.get("period_start"),
            "period_end": period.get("period_end"),
        }

        # Optional short text fields for debugging / retrieval context
        embedding_text = (doc.get("embedding_text") or "").strip()
        if embedding_text:
            metadata["embedding_text"] = embedding_text

        interpretation = (doc.get("interpretation") or {}) or {}
        exec_summary = (interpretation.get("executive_summary") or "").strip()
        if exec_summary:
            metadata["executive_summary"] = exec_summary

        vectors.append(
            {
                "id": doc.get("doc_id"),
                "values": list(emb),
                "metadata": metadata,
            }
        )

    return vectors


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Ingest buyer-quarter KPI documents into Pinecone using Ava embeddings."
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
        help="Maximum number of docs to embed and upsert (e.g., 1, 20, 1000).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Number of docs per embeddings/upsert batch.",
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

    ava_cfg = load_ava_config()
    pinecone_cfg = load_pinecone_config()
    index = ensure_pinecone_client(pinecone_cfg)
    logger.info(
        "Using Pinecone index '%s' in environment '%s'.",
        pinecone_cfg.index_name,
        pinecone_cfg.environment,
    )

    # Filter out docs with empty embedding_text up front
    filtered_docs: List[Dict[str, Any]] = []
    for d in docs:
        text = (d.get("embedding_text") or "").strip()
        if not text:
            logger.warning(
                "Skipping doc with empty embedding_text: %s", d.get("doc_id")
            )
            continue
        filtered_docs.append(d)

    if not filtered_docs:
        logger.warning(
            "All loaded docs had empty embedding_text; nothing to embed/upsert."
        )
        return

    batch_size = max(1, args.batch_size)
    total = len(filtered_docs)
    total_batches = (total + batch_size - 1) // batch_size

    logger.info(
        "Embedding and upserting %d docs into Pinecone in batches of %d...",
        total,
        batch_size,
    )

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch_docs = filtered_docs[start:end]
        batch_texts = [
            (d.get("embedding_text") or "").strip() for d in batch_docs
        ]

        batch_id = (start // batch_size) + 1
        logger.info(
            "Processing batch %d/%d: docs %d-%d, doc_ids %s -> %s",
            batch_id,
            total_batches,
            start + 1,
            end,
            batch_docs[0].get("doc_id"),
            batch_docs[-1].get("doc_id"),
        )

        try:
            embeddings = fetch_document_embeddings(batch_texts, ava_cfg)
        except Exception:
            logger.exception(
                "Failed while fetching embeddings for batch %d/%d",
                batch_id,
                total_batches,
            )
            raise

        logger.info("Received %d embeddings for current batch.", len(embeddings))

        vectors = build_pinecone_vectors(batch_docs, embeddings)
        try:
            index.upsert(vectors=vectors)
        except Exception:
            logger.exception(
                "Failed while upserting batch %d/%d into Pinecone",
                batch_id,
                total_batches,
            )
            raise
        logger.info(
            "Upserted %d vectors into Pinecone for batch %d.",
            len(vectors),
            batch_id,
        )

        if end < total:
            time.sleep(2.0)

    logger.info("Finished upserting %d docs into Pinecone.", total)


if __name__ == "__main__":
    main()

