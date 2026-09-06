#!/usr/bin/env python3
"""Compare lexical variants on SciFact train judgments, keeping raw runs.

Run from repository root against the already ingested evaluation index.
The default split is train; --split test is reserved for post-selection checks.
"""
import argparse
import json
import time
import urllib.request
from pathlib import Path
from collect_api_run import read_queries, read_qrel_ids, read_id_map
from score_run import read_qrels, score_query


def clauses(query, variant):
    content = {"query": query, "operator": "or"}
    if variant.startswith("msm"):
        content["minimum_should_match"] = "2<50%" if variant == "msm50" else "2<75%"
    result = [{"match": {"content": content}}, {"match": {"display_path": {
        "query": query, "operator": "or", "boost": 2}}}]
    if variant == "phrase":
        result.append({"match_phrase": {"content": {"query": query, "boost": 1}}})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--split', choices=['train', 'test'], default='train')
    parser.add_argument('--url', default='http://127.0.0.1:9200')
    parser.add_argument('--index', default='scifact-eval-20260905')
    args = parser.parse_args()
    root = Path('evaluation/external/scifact')
    qpath = root / 'qrels' / (args.split + '.tsv')
    queries = read_queries(root / 'queries.jsonl', read_qrel_ids(qpath))
    qrels = read_qrels(qpath)
    mapping = read_id_map(Path('evaluation/external/scifact-documents/document-map.tsv'))
    summary = {}
    for variant in ['or', 'msm50', 'msm75', 'phrase']:
        rows = []
        for qid, query in queries:
            body = {'size': 100, '_source': ['display_path'], 'query': {'bool': {
                'should': clauses(query, variant), 'minimum_should_match': 1}}}
            request = urllib.request.Request(args.url + '/' + args.index + '/_search',
                data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
            start = time.perf_counter()
            with urllib.request.urlopen(request, timeout=60) as response:
                data = json.load(response)
            if data.get('timed_out') or data.get('_shards', {}).get('failed', 0):
                raise RuntimeError('Incomplete search: ' + str(data))
            hits = data['hits']['hits']
            rows.append({'query_id': qid, 'query': query, 'elapsed_ms': (time.perf_counter()-start)*1000,
                'results': [{'corpus_id': mapping[h['_source']['display_path']], 'rank': rank,
                    'score': h['_score']} for rank, h in enumerate(hits, 1)], 'search': body})
        out = Path(f'evaluation/runs/scifact-{args.split}-lexical-{variant}-20260906.jsonl')
        out.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        metrics = {}
        for cutoff in [10, 100]:
            values = [score_query(qrels[row['query_id']], [r['corpus_id'] for r in row['results']], cutoff, 'linear') for row in rows]
            metrics[cutoff] = {metric: sum(v[metric] for v in values)/len(values) for metric in values[0]}
        summary[variant] = {'queries':len(rows), 'metrics':metrics}
        print(variant, summary[variant], flush=True)
    Path(f'evaluation/runs/scifact-{args.split}-lexical-variants-20260906.json').write_text(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
