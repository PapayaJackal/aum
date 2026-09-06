# Full-content hybrid benchmark — 2026-09-06

This report extends [the 5 September baseline](BENCHMARK-2026-09-05.md). It
uses the production full-content embedding path and the production lexical OR
branch. All generated query runs, per-query metrics, and vector caches remain
ignored under `evaluation/runs/`.

## Fixed environments

The final external-corpus configuration was local CPU Ollama at
`http://127.0.0.1:11434` and `:11435`, image
`docker.io/ollama/ollama:latest` (local image `2a5d04622211...`), with two
independent workers. The recorded repository digest is
`sha256:32931b46719f673c05fdbaa81ccb26da18ea4a1c57590a754874ab28ba269eb2`
(the local image also reports
`sha256:57a73f11f75b32b97b59b003f351445c9c2a8af4b9d586ecdc928dee6150ef26`). The fixed model was `all-minilm:22m`, digest
`1b226e2802dbb772b5fc32a58f103ca1804ef7501331012de126ab22f67475ef`, a 23M-parameter F16 BERT encoder with a 512-token context
and 384 output dimensions. AUM used a 512-token chunk context, its overlap
default, and an empty query prefix (appropriate for this model). No vector
mocking or model substitution occurred inside a comparison.

Fresh isolated indices were used: `aum-docs-minilm-full-20260906` (16/16
ingested and embedded) and `scifact-minilm-full-20260906` (5,183/5,183,
no failures). SciFact full-content embedding completed in 123 seconds. The
older 1,024-dimensional Qwen fixture experiment is retained separately and
is not compared as a before/after result.

## Fixture: fixed all-minilm model, live API

| semantic ratio | nDCG@10 | MRR@10 | Recall@10 | mean / p50 / p95 ms |
| ---: | ---: | ---: | ---: | ---: |
| 0.00 | 0.9314 | 1.0000 | 0.8929 | 6.540 / 5.594 / 13.132 |
| 0.25 | 0.9575 | 1.0000 | 0.9643 | 24.719 / 24.187 / 33.243 |
| 0.50 | 0.9654 | 1.0000 | 0.9643 | 23.496 / 23.419 / 25.561 |
| 0.75 | 0.9691 | 1.0000 | 0.9643 | 23.736 / 24.075 / 26.091 |
| 1.00 | 0.9865 | 1.0000 | 1.0000 | 20.776 / 20.324 / 25.420 |

All 14 judged queries returned results. This small purpose-built fixture has
many semantic wins, so it demonstrates retrieval coverage but does not select
the production ratio.

## SciFact development selection

The first 200 judged query IDs in the existing SciFact training-query order
were fixed before comparing ratios (225 judgments). This subset, rather than
the SciFact test set, selected the final ratio.

| semantic ratio | nDCG@10 | MRR@10 | Recall@10 | API mean / p50 / p95 ms |
| ---: | ---: | ---: | ---: | ---: |
| 0.00 | 0.6399 | 0.6017 | 0.7838 | 34.239 / 33.936 / 43.278 |
| 0.25 | 0.6947 | 0.6570 | 0.8463 | 538.434 / 573.176 / 732.847 |
| 0.50 | **0.7027** | **0.6662** | 0.8438 | 537.892 / 574.523 / 732.728 |

Ratio 0.50 was frozen for test evaluation because it had the best nDCG and
MRR; ratio 0.25 had 0.25 percentage points more Recall@10.

## Candidate-depth diagnostic

The same fixed training subset was queried directly against OpenSearch at
ratio 0.50 with cached, real all-minilm query vectors and RRF constant 60.
It requested only `display_path`, so these are retrieval-only timings rather
than API end-to-end timings.

| vector candidate depth | nDCG@10 | MRR@10 | Recall@10 | mean / p95 ms |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0.6963 | 0.6590 | 0.8388 | 12.661 / 15.176 |
| 300 | 0.6975 | 0.6607 | 0.8388 | 27.493 / 30.625 |
| 1,000 | **0.7027** | **0.6662** | **0.8438** | 73.387 / 80.075 |

Depth 1,000 remains the quality setting. The large API latency gap is not
explained by raw vector retrieval alone; a five-query highlighting diagnostic found no material gain from supplying
an explicit lexical highlight query, so that experiment was not applied. Do not compare the table's direct-backend latency with API timing.

## Frozen SciFact test

The frozen 300-query test run at the train-selected 0.50 ratio returned
results for all 300 judged queries: nDCG@10 **0.6899**, MRR@10 **0.6461**, and
Recall@10 **0.8347**. The exact-index API text baseline was nDCG@10 **0.6507**,
MRR@10 **0.6201**, and Recall@10 **0.7665**, so the selected hybrid setting
improved all three measures by 0.0392, 0.0260, and 0.0682 respectively.

The post-aggregation-cleanup rerun had identical rankings for every one of
the 300 query result lists and identical metrics. Its end-to-end latency was
mean 548.026 ms, p50 582.101 ms, p95 739.573 ms (text: 33.265 / 32.450 /
43.318 ms). The cleanup avoids computing unused aggregations, but this benchmark does
not claim a material latency win. Hybrid candidate
retrieval plus highlighting remains the dominant cost at this corpus size.

For a separate paired comparison of the final hybrid run against the exact
same-index OR lexical baseline, nDCG@10 changed by +0.03923 (67 query wins,
30 losses, 203 ties); a 5,000-resample paired bootstrap with seed `20260906`
gave a 95% percentile interval of [+0.01719, +0.06148].

## Evaluation-harness safeguards

Long API collections now stream each completed record to a `.partial` file
and atomically publish the final JSONL only on success. Interrupted runs keep
their completed-query evidence rather than silently losing it. The direct
cached-vector depth collector records and validates model name, query prefix,
context length, and a SHA-256 of each query text; it rejects legacy or
mismatched caches, endpoint ratios, invalid depths, timed-out searches, and
failed shards. The depth measurements above were generated with all-minilm,
empty prefix, and the 512-token configuration recorded above.

## Reproduction

The local configuration at `evaluation/bench-data/aum.toml` contains the
following model and worker settings (that generated directory is ignored by
Git). Keep the data directory fixed so the API reads the same model metadata
as the CLI; set `[data].dir` to its absolute path in a fresh checkout.

```toml
[opensearch]
url = "http://127.0.0.1:9200"

[embeddings]
enabled = true
backend = "ollama"
model = "all-minilm:22m"
dimension = 384
context_length = 512
query_prefix = ""
chunk_overlap = 200
batch_size = 50
instances = [
  { url = "http://127.0.0.1:11434", concurrency = 1 },
  { url = "http://127.0.0.1:11435", concurrency = 1 },
]
```

Both local Ollama workers must have the recorded model available. Verify its
`/api/tags` digest before reproducing; the mutable tag alone does not pin the
model. OpenSearch 3.6.0 and Tika use the repository Compose setup. The running
API is local at `http://127.0.0.1:18000`.

```sh
# Evaluation-only config lives in evaluation/bench-data/aum.toml.
cd evaluation/bench-data
../../target/debug/aum ingest scifact-minilm-full-20260906 \
  ../external/scifact-documents/documents
../../target/debug/aum embed scifact-minilm-full-20260906

# Start the API from this directory, then collect and score a frozen run.
AUM_AUTH__PUBLIC_MODE=true AUM_SERVER__HOST=127.0.0.1 AUM_SERVER__PORT=18000 \
  ../../target/debug/aum-api serve
python3 ../scripts/collect_api_run.py \
  --queries ../external/scifact/queries.jsonl \
  --qrels ../external/scifact/qrels/test.tsv \
  --index scifact-minilm-full-20260906 --base-url http://127.0.0.1:18000 \
  --type hybrid --semantic-ratio 0.5 --limit 100 \
  --id-map ../external/scifact-documents/document-map.tsv \
  --out ../runs/scifact-test-hybrid-050-all-minilm-full-20260906.jsonl
python3 ../scripts/score_run.py --qrels ../external/scifact/qrels/test.tsv \
  --run ../runs/scifact-test-hybrid-050-all-minilm-full-20260906.jsonl \
  --cutoffs 1,3,10 --json
```
