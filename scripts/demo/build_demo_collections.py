#!/usr/bin/env python3
"""
Build two Qdrant collections (baseline and candidate) from a JSONL docs file
using a deterministic local embedder with profile-based perturbations.

This script requires:
  pip install qdrant-client numpy

Example:
  python build_demo_collections.py \
    --docs traceowl_demo_docs.jsonl \
    --qdrant-url http://localhost:6333 \
    --baseline-profile baseline_profile.json \
    --candidate-profile candidate_profile.json \
    --baseline-collection demo_baseline \
    --candidate-collection demo_candidate \
    --vector-size 64
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

TOKEN_RE = re.compile(r"[a-z0-9_]+")

CATEGORY_HINTS = {
    "auth": ["password", "login", "account", "locked", "2fa", "email"],
    "billing": ["refund", "subscription", "invoice", "payment", "pricing", "billing"],
    "usage": ["project", "upload", "delete", "share", "export", "create"],
    "errors": ["error", "timeout", "connection", "api", "failed", "refused"],
    "misc": ["support", "company", "privacy", "terms", "status", "contact"],
}


def tokenize(text: str) -> List[str]:
    return TOKEN_RE.findall(text.lower())


def seeded_unit_vector(token: str, dim: int) -> np.ndarray:
    """Deterministic pseudo-random unit vector from token."""
    data = hashlib.sha256(token.encode("utf-8")).digest()
    seed = int.from_bytes(data[:8], "big", signed=False)
    rng = np.random.default_rng(seed)
    vec = rng.normal(0.0, 1.0, size=dim).astype(np.float32)
    norm = np.linalg.norm(vec)
    if norm == 0:
        return vec
    return vec / norm


def category_of_text(tokens: Iterable[str]) -> str:
    token_set = set(tokens)
    scores = {}
    for cat, hints in CATEGORY_HINTS.items():
        scores[cat] = sum(1 for h in hints if h in token_set)
    return max(scores, key=scores.get)


def embed_text(text: str, profile: Dict, dim: int) -> np.ndarray:
    tokens = tokenize(text)
    if not tokens:
        return np.zeros(dim, dtype=np.float32)

    token_weight = float(profile.get("token_weight", 1.0))
    title_boost = float(profile.get("title_boost", 1.0))
    category_weight = float(profile.get("category_weight", 0.8))
    term_bias_weight = float(profile.get("term_bias_weight", 1.5))
    noise = float(profile.get("noise", 0.0))

    vec = np.zeros(dim, dtype=np.float32)

    for tok in tokens:
        vec += token_weight * seeded_unit_vector(f"tok:{tok}", dim)

    # Simple phrase-ish feature: adjacent token bigrams
    for a, b in zip(tokens, tokens[1:]):
        vec += 0.35 * token_weight * seeded_unit_vector(f"bigram:{a}_{b}", dim)

    detected_category = category_of_text(tokens)
    vec += category_weight * seeded_unit_vector(f"cat:{detected_category}", dim)

    # Configurable biases to intentionally skew certain queries/documents
    # Example:
    # "term_biases": {
    #   "refund": ["usage", "misc"],
    #   "invoice": ["usage"]
    # }
    for tok in tokens:
        for target_cat in profile.get("term_biases", {}).get(tok, []):
            vec += term_bias_weight * seeded_unit_vector(f"cat:{target_cat}", dim)

    # Optional penalties: subtract category signals for certain terms
    for tok in tokens:
        for target_cat in profile.get("term_penalties", {}).get(tok, []):
            vec -= term_bias_weight * seeded_unit_vector(f"cat:{target_cat}", dim)

    # Optional small deterministic noise
    if noise > 0:
        noise_vec = seeded_unit_vector("noise:" + "|".join(tokens), dim)
        vec += noise * noise_vec

    norm = np.linalg.norm(vec)
    if norm == 0:
        return vec
    return (vec / norm).astype(np.float32)


def load_jsonl(path: Path) -> List[Dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"Malformed JSONL in {path} line {line_no}: {e}") from e
    return rows


def recreate_collection(client: QdrantClient, name: str, vector_size: int) -> None:
    exists = client.collection_exists(name)
    if exists:
        client.delete_collection(name)
    client.create_collection(
        collection_name=name,
        vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
    )


def upsert_docs(client: QdrantClient, collection: str, docs: List[Dict], profile: Dict, dim: int) -> None:
    points: List[PointStruct] = []
    for i, doc in enumerate(docs, 1):
        text = f"{doc.get('title', '')}. {doc.get('content', '')}"
        vector = embed_text(text, profile, dim).tolist()
        points.append(
            PointStruct(
                id=i,
                vector=vector,
                payload={
                    "doc_id": doc["doc_id"],
                    "category": doc.get("category"),
                    "title": doc.get("title"),
                    "content": doc.get("content"),
                },
            )
        )
    client.upsert(collection_name=collection, points=points)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", required=True, type=Path)
    ap.add_argument("--qdrant-url", required=True)
    ap.add_argument("--baseline-profile", required=True, type=Path)
    ap.add_argument("--candidate-profile", required=True, type=Path)
    ap.add_argument("--baseline-collection", default="demo_baseline")
    ap.add_argument("--candidate-collection", default="demo_candidate")
    ap.add_argument("--vector-size", type=int, default=64)
    args = ap.parse_args()

    docs = load_jsonl(args.docs)
    baseline_profile = json.loads(args.baseline_profile.read_text(encoding="utf-8"))
    candidate_profile = json.loads(args.candidate_profile.read_text(encoding="utf-8"))

    client = QdrantClient(url=args.qdrant_url)

    recreate_collection(client, args.baseline_collection, args.vector_size)
    recreate_collection(client, args.candidate_collection, args.vector_size)

    upsert_docs(client, args.baseline_collection, docs, baseline_profile, args.vector_size)
    upsert_docs(client, args.candidate_collection, docs, candidate_profile, args.vector_size)

    print(f"Loaded {len(docs)} docs into:")
    print(f"  baseline:  {args.baseline_collection}")
    print(f"  candidate: {args.candidate_collection}")


if __name__ == "__main__":
    main()
