#!/usr/bin/env python3
"""Collect a scorer-ready lexical run directly from an AUM OpenSearch index.

This is an evaluation-only ablation tool.  ``--operator and`` mirrors AUM's
current OpenSearch text branch; ``--operator or`` changes only the content and
display-path match operators, keeping their existing filename boost.
"""
import argparse
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--queries", type=Path, required=True)
    p.add_argument("--index", required=True)
    p.add_argument("--url", default="http://127.0.0.1:9200")
    p.add_argument("--operator", choices=("and", "or"), required=True)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--qrels", type=Path, help="limit collection to judged BEIR query ids")
    p.add_argument("--id-map", type=Path)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    id_map = {}
    judged = None
    if a.qrels:
        judged = {r.split("\t")[0] for r in a.qrels.read_text().splitlines()[1:] if r}
    if a.id_map:
        rows = a.id_map.read_text().splitlines()
        header = rows.pop(0).split("\t")
        display, corpus = header.index("display-path"), header.index("corpus-id")
        id_map = {r.split("\t")[display]: r.split("\t")[corpus] for r in rows}
    rows = []
    for raw in a.queries.read_text().splitlines():
        q = json.loads(raw)
        if judged is not None and q["_id"] not in judged:
            continue
        body = {"size": a.limit, "_source": ["display_path"], "query": {"bool": {"should": [
            {"match": {"content": {"query": q["text"], "operator": a.operator}}},
            {"match": {"display_path": {"query": q["text"], "operator": a.operator, "boost": 2}}},
        ], "minimum_should_match": 1}}}
        request = urllib.request.Request(
            f"{a.url.rstrip('/')}/{urllib.parse.quote(a.index)}/_search",
            data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}, method="POST")
        started = time.perf_counter()
        with urllib.request.urlopen(request, timeout=30) as res:
            hits = json.load(res)["hits"]["hits"]
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        results = []
        for rank, hit in enumerate(hits, 1):
            display_path = hit["_source"]["display_path"]
            results.append({"corpus_id": id_map.get(display_path, display_path), "rank": rank, "score": hit["_score"]})
        rows.append({"query_id": q["_id"], "query": q["text"], "results": results, "elapsed_ms": elapsed_ms,
                     "search": {"endpoint": f"{a.url.rstrip('/')}/{a.index}/_search", "type": "opensearch-text-ablation", "operator": a.operator, "limit": a.limit}})
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text("\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n")
    print(f"wrote {len(rows)} query results to {a.out}")


if __name__ == "__main__":
    raise SystemExit(main())
