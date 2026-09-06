# Hybrid-search benchmark — 2026-09-05

## Scope and environment

This run used the checked-out AUM implementation, the bundled `aum-docs-v1`
fixture, and the locally downloaded BEIR SciFact test set. OpenSearch was
`3.6.0` (`4ca747d8d47...`) from the repository's pinned Compose image. Tika
was the pinned `3.3.0.0-full` image. Results were collected serially, with a
limit of 100. Each query was issued once to a warm, local engine; latency is
therefore a single-pass diagnostic rather than a stable performance estimate.
All runs and per-query metric files are in the ignored
`evaluation/runs/` directory.

Two disposable local indices remain available for inspection:
`aum-docs-eval-20260905` (16 of 16 files indexed, no failures) and
`scifact-eval-20260905` (5,183 of 5,183 materialized SciFact files indexed,
no failures). The temporary AUM data directory is ignored at
`evaluation/bench-data/`. Existing services and data were left intact.
All 14 fixture qrel query IDs and all 300 SciFact test qrel query IDs were
submitted; there were no missing judged queries or collector request failures.

No fixed embedding service or cached local embedding model was available:
there was no Ollama server/binary and no configured OpenAI-compatible endpoint.
Consequently, a semantic vector run and hybrid ratios 0.25, 0.5, 0.75, and 1.0
were **not** produced. No mock or substitute vectors were used. The API
`hybrid` ratio 0 route was measured because it explicitly delegates to text
search without embedding.

## API baseline: aum-docs-v1

| Mode | nDCG@10 | MRR@10 | Recall@10 | queries with results | mean / p50 / p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| API text | 0.1180 | 0.1429 | 0.0714 | 2 / 14 | 7.928 / 7.232 / 16.535 |
| API hybrid, ratio 0 | 0.1180 | 0.1429 | 0.0714 | 2 / 14 | 6.605 / 6.059 / 13.740 |

The identical rankings confirm that the current ratio-zero endpoint takes the
keyword path. Twelve judged fixture queries returned no document, despite the
successful corpus ingest.

## Backend-only lexical ablation

This is not an API or hybrid result. It queries the same AUM-created
OpenSearch indices directly, changing only each `match` clause from the
current `operator: and` to `operator: or`; filename boost 2 and the outer
`minimum_should_match: 1` were kept. It requests only `_source.display_path`
and omits API highlighting, facets, totals, authentication, and JSON response
mapping, so its latency must not be compared with API latency. It isolates the
lexical candidate issue.

| Dataset and operator | nDCG@10 | MRR@10 | Recall@10 | queries with results | mean / p50 / p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| aum-docs-v1 AND | 0.1180 | 0.1429 | 0.0714 | 2 / 14 | 5.282 / 4.569 / 14.053 |
| aum-docs-v1 OR | 0.9314 | 1.0000 | 0.8929 | 14 / 14 | 5.150 / 4.831 / 11.605 |
| SciFact AND | 0.0180 | 0.0200 | 0.0175 | 6 / 300 | 3.232 / 3.074 / 4.281 |
| SciFact OR | 0.6502 | 0.6195 | 0.7665 | 300 / 300 | 5.970 / 5.624 / 8.222 |

The result is decisive evidence that strict all-term matching starves the
lexical branch for ordinary questions and scientific claims. OR has a modest
latency cost on SciFact (mean +2.738 ms). Follow-up experiments should compare
dynamic minimum-should-match rules, phrase and filename boosts, and query
classes before selecting a production default. These test qrels are being used
exploratorily to identify the failure mode; they are not a held-out tuning set.

On the fixture, strict AND retrieved the exact q01 policy result but returned
no result for exact technical q08 (`API 429 throttling`). OR put q01's primary
document first and q08's rate-limit document first. OR still missed secondary
judgments for q01 (only 1 of 2), q12 (1 of 2), and q13 (1 of 2), showing the
remaining ranking/recall work even after candidate starvation is removed.
At nDCG@10, OR improved 13 fixture queries, tied one, and lost none; on
SciFact it improved 231 test queries, tied 69, and lost none. SciFact OR
Recall@100 was 0.8842 (versus Recall@10 0.7665), indicating that much of the
remaining gap is ranking within the first page rather than candidate absence.

## Reproduction

```sh
docker compose up -d
cargo build -p aum-cli -p aum-api
AUM_DATA__DIR=evaluation/bench-data \
  AUM_TIKA__SERVER_URL=http://127.0.0.1:9998 \
  AUM_OPENSEARCH__URL=http://127.0.0.1:9200 ./target/debug/aum ingest \
  aum-docs-eval-20260905 evaluation/datasets/aum-docs-v1/documents
AUM_DATA__DIR=evaluation/bench-data \
  AUM_TIKA__SERVER_URL=http://127.0.0.1:9998 \
  AUM_OPENSEARCH__URL=http://127.0.0.1:9200 ./target/debug/aum ingest \
  scifact-eval-20260905 evaluation/external/scifact-documents/documents

# Separate terminal: local only, public for the evaluator, with embeddings off.
AUM_DATA__DIR=evaluation/bench-data AUM_AUTH__PUBLIC_MODE=true \
  AUM_SERVER__HOST=127.0.0.1 AUM_SERVER__PORT=18000 \
  AUM_OPENSEARCH__URL=http://127.0.0.1:9200 AUM_EMBEDDINGS__ENABLED=false \
  ./target/debug/aum-api serve

python3 evaluation/scripts/collect_api_run.py \
  --queries evaluation/datasets/aum-docs-v1/queries.jsonl \
  --index aum-docs-eval-20260905 --base-url http://127.0.0.1:18000 \
  --type text --limit 100 --out evaluation/runs/aum-docs-text-20260905.jsonl
python3 evaluation/scripts/collect_api_run.py \
  --queries evaluation/datasets/aum-docs-v1/queries.jsonl \
  --index aum-docs-eval-20260905 --base-url http://127.0.0.1:18000 \
  --type hybrid --semantic-ratio 0 --limit 100 \
  --out evaluation/runs/aum-docs-hybrid-000-20260905.jsonl

python3 evaluation/scripts/collect_opensearch_text_run.py \
  --queries evaluation/external/scifact/queries.jsonl \
  --qrels evaluation/external/scifact/qrels/test.tsv \
  --index scifact-eval-20260905 \
  --id-map evaluation/external/scifact-documents/document-map.tsv \
  --operator or --out evaluation/runs/scifact-opensearch-or-20260905.jsonl
python3 evaluation/scripts/score_run.py --qrels evaluation/external/scifact/qrels/test.tsv \
  --run evaluation/runs/scifact-opensearch-or-20260905.jsonl --cutoffs 1,3,10 --json
```

## Important semantic-indexing finding

The current embed pipeline receives `SearchResult` values from
`scroll_unembedded` / `scroll_documents`. OpenSearch `parse_hit` turns the
stored `content` into `snippet` by taking only its first 200 characters when
there is no highlight, and `EmbedPipeline::scroll_source` chunks that snippet.
Thus the production embedding path currently embeds a 200-character prefix,
not full document content. On SciFact every materialized document exceeds that
length (median title-plus-text length about 1,427 characters), so this is a
blocker for interpreting any future semantic benchmark. Fix and test full
content retrieval into the embed pipeline before comparing fusion ratios.
