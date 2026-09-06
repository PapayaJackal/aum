#!/usr/bin/env python3
"""Validate the checked-in aum-docs-v1 hybrid-search fixture.

The fixture is deliberately dependency-free so it can be checked in CI before
starting Tika, an embedding service, or a search backend.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


DATASET = Path(__file__).resolve().parents[1] / "datasets" / "aum-docs-v1"


def read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{line_no}: invalid JSON: {exc.msg}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_no}: expected a JSON object")
        records.append(value)
    if not records:
        raise ValueError(f"{label} is empty: {path}")
    return records


def unique_ids(records: list[dict[str, Any]], label: str) -> set[str]:
    ids: list[str] = []
    for position, record in enumerate(records, 1):
        record_id = record.get("_id")
        text = record.get("text")
        if not isinstance(record_id, str) or not record_id:
            raise ValueError(f"{label} record {position} has no non-empty _id")
        if not isinstance(text, str) or not text.strip():
            raise ValueError(f"{label} record {record_id!r} has no non-empty text")
        ids.append(record_id)
    duplicates = sorted(record_id for record_id, count in Counter(ids).items() if count > 1)
    if duplicates:
        raise ValueError(f"duplicate {label} ids: {', '.join(duplicates)}")
    return set(ids)


def read_qrels(path: Path, corpus_ids: set[str], query_ids: set[str]) -> dict[str, int]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "query-id\tcorpus-id\tscore":
        raise ValueError(f"{path}: expected BEIR qrels header")

    counts: Counter[str] = Counter()
    pairs: set[tuple[str, str]] = set()
    for line_no, raw in enumerate(lines[1:], 2):
        fields = raw.split("\t")
        if len(fields) != 3:
            raise ValueError(f"{path}:{line_no}: expected three tab-separated fields")
        query_id, corpus_id, score_text = fields
        if query_id not in query_ids:
            raise ValueError(f"{path}:{line_no}: unknown query id {query_id!r}")
        if corpus_id not in corpus_ids:
            raise ValueError(f"{path}:{line_no}: unknown corpus id {corpus_id!r}")
        try:
            score = int(score_text)
        except ValueError as exc:
            raise ValueError(f"{path}:{line_no}: relevance score must be an integer") from exc
        if score not in {1, 2, 3}:
            raise ValueError(f"{path}:{line_no}: relevance score must be 1, 2, or 3")
        pair = (query_id, corpus_id)
        if pair in pairs:
            raise ValueError(f"{path}:{line_no}: duplicate qrel {pair!r}")
        pairs.add(pair)
        counts[query_id] += 1

    missing = sorted(query_ids - set(counts))
    if missing:
        raise ValueError(f"qrels missing query ids: {', '.join(missing)}")
    return dict(counts)


def check_document_map(corpus_ids: set[str]) -> int:
    path = DATASET / "document-map.tsv"
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "corpus-id\tdisplay-path":
        raise ValueError(f"{path}: expected corpus-id/display-path header")

    mapped: dict[str, str] = {}
    for line_no, raw in enumerate(lines[1:], 2):
        fields = raw.split("\t")
        if len(fields) != 2 or not all(fields):
            raise ValueError(f"{path}:{line_no}: expected two non-empty tab-separated fields")
        corpus_id, display_path = fields
        if corpus_id in mapped:
            raise ValueError(f"{path}:{line_no}: duplicate corpus id {corpus_id!r}")
        if corpus_id != display_path:
            raise ValueError(
                f"{path}:{line_no}: corpus id must match AUM display path for this fixture"
            )
        document = DATASET / "documents" / display_path
        if not document.is_file() or not document.read_text(encoding="utf-8").strip():
            raise ValueError(f"{path}:{line_no}: missing or empty source document {document}")
        mapped[corpus_id] = display_path

    if set(mapped) != corpus_ids:
        missing = sorted(corpus_ids - set(mapped))
        extra = sorted(set(mapped) - corpus_ids)
        raise ValueError(f"document map and corpus differ; missing={missing}, extra={extra}")
    return len(mapped)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="print the fixture counts as JSON")
    args = parser.parse_args()

    try:
        corpus = read_jsonl(DATASET / "corpus.jsonl", "corpus")
        queries = read_jsonl(DATASET / "queries.jsonl", "queries")
        corpus_ids = unique_ids(corpus, "corpus")
        query_ids = unique_ids(queries, "query")
        for query in queries:
            if not isinstance(query.get("category"), str) or not isinstance(query.get("purpose"), str):
                raise ValueError(f"query {query['_id']!r} needs category and purpose strings")
        qrel_counts = read_qrels(DATASET / "qrels" / "test.tsv", corpus_ids, query_ids)
        document_count = check_document_map(corpus_ids)
    except (OSError, ValueError) as exc:
        print(f"fixture check failed: {exc}", file=sys.stderr)
        return 1

    summary = {
        "dataset": "aum-docs-v1",
        "documents": document_count,
        "queries": len(query_ids),
        "judgments": sum(qrel_counts.values()),
        "minimum_judgments_per_query": min(qrel_counts.values()),
        "maximum_judgments_per_query": max(qrel_counts.values()),
    }
    if args.json:
        print(json.dumps(summary, sort_keys=True))
    else:
        print(
            "fixture OK: {documents} documents, {queries} queries, {judgments} graded judgments".format(
                **summary
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
