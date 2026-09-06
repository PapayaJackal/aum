#!/usr/bin/env python3
"""Collect an OpenSearch weighted-RRF run with cached real Ollama query vectors.

Evaluation-only: mirrors AUM's OR lexical branch and request-local weighted
RRF body while allowing ``pagination_depth`` to vary without repeating model
inference.  It is deliberately not an API latency measurement.
"""
import argparse
import hashlib
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path


def request(url, body):
    r = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(r, timeout=60) as response:
        return json.load(response)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--queries", type=Path, required=True)
    p.add_argument("--qrels", type=Path, required=True)
    p.add_argument("--max-queries", type=int)
    p.add_argument("--index", required=True)
    p.add_argument("--id-map", type=Path, required=True)
    p.add_argument("--ollama-url", default="http://127.0.0.1:11434")
    p.add_argument("--model", required=True)
    p.add_argument("--query-prefix", default="")
    p.add_argument("--context-length", type=int, required=True)
    p.add_argument("--opensearch-url", default="http://127.0.0.1:9200")
    p.add_argument("--ratio", type=float, default=.5)
    p.add_argument("--depth", type=int, required=True)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--vector-cache", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    if not 0.0 < a.ratio < 1.0:
        p.error("--ratio must be strictly between 0 and 1 for this hybrid ablation")
    if a.depth < a.limit:
        p.error("--depth must be at least --limit")
    if a.context_length < 1:
        p.error("--context-length must be positive")
    qrel_ids = {x.split("\t")[0] for x in a.qrels.read_text().splitlines()[1:] if x}
    queries = [json.loads(x) for x in a.queries.read_text().splitlines() if x]
    queries = [x for x in queries if x["_id"] in qrel_ids]
    if a.max_queries: queries = queries[:a.max_queries]
    mapping_rows = a.id_map.read_text().splitlines()
    header = mapping_rows.pop(0).split("\t")
    di, ci = header.index("display-path"), header.index("corpus-id")
    mapping = {x.split("\t")[di]: x.split("\t")[ci] for x in mapping_rows}
    provenance = {"version": 1, "model": a.model, "query_prefix": a.query_prefix, "context_length": a.context_length}
    cache = {**provenance, "queries": {}}
    if a.vector_cache.exists():
        cache = json.loads(a.vector_cache.read_text())
        if not isinstance(cache, dict) or any(cache.get(k) != v for k, v in provenance.items()) or not isinstance(cache.get("queries"), dict):
            raise ValueError(f"incompatible or legacy vector cache: {a.vector_cache}; choose a new cache path")
    vectors = cache["queries"]
    for q in queries:
        text_hash = hashlib.sha256(q["text"].encode()).hexdigest()
        existing = vectors.get(q["_id"])
        if existing is not None and existing.get("text_sha256") != text_hash:
            raise ValueError(f"vector cache text mismatch for query {q['_id']!r}")
        if existing is None:
            vectors[q["_id"]] = {"text_sha256": text_hash, "vector": request(a.ollama_url.rstrip("/")+"/api/embed", {"model":a.model,"input":a.query_prefix + q["text"]})["embeddings"][0]}
    a.vector_cache.parent.mkdir(parents=True, exist_ok=True)
    a.vector_cache.write_text(json.dumps(cache, separators=(",",":")))
    rows=[]
    for q in queries:
        lexical={"bool":{"should":[{"match":{"content":{"query":q["text"],"operator":"or"}}},{"match":{"display_path":{"query":q["text"],"operator":"or","boost":2}}}],"minimum_should_match":1}}
        knn={"nested":{"path":"chunks","score_mode":"max","query":{"knn":{"chunks.embedding":{"vector":vectors[q["_id"]]["vector"],"k":a.depth}}}}}
        body={"size":a.limit,"_source":["display_path"],"query":{"hybrid":{"queries":[lexical,knn],"pagination_depth":a.depth}},"search_pipeline":{"phase_results_processors":[{"score-ranker-processor":{"combination":{"technique":"rrf","rank_constant":60,"parameters":{"weights":[1-a.ratio,a.ratio]}}}}]}}
        started=time.perf_counter(); data=request(a.opensearch_url.rstrip("/")+"/"+urllib.parse.quote(a.index)+"/_search",body); elapsed=round((time.perf_counter()-started)*1000,3)
        shards = data.get("_shards", {})
        if data.get("timed_out") or shards.get("failed", 0):
            raise RuntimeError(f"OpenSearch query {q['_id']!r} timed out or had failed shards: {shards}")
        results=[{"corpus_id":mapping[h["_source"]["display_path"]],"rank":i,"score":h["_score"]} for i,h in enumerate(data["hits"]["hits"],1)]
        rows.append({"query_id":q["_id"],"query":q["text"],"results":results,"elapsed_ms":elapsed,"search":{"type":"opensearch-direct-hybrid-depth-ablation","ratio":a.ratio,"pagination_depth":a.depth,"limit":a.limit}})
    a.out.write_text("\n".join(json.dumps(x,separators=(",",":")) for x in rows)+"\n")
    print(f"wrote {len(rows)} query results to {a.out}")


if __name__ == "__main__": main()
