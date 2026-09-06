# Hybrid-search evaluation

This directory provides two complementary ways to measure AUM search quality:

| Dataset | What it measures | Size | Included in git |
| --- | --- | ---: | --- |
| `datasets/aum-docs-v1` | AUM-style file ingestion, filenames, email headers, exact terms, paraphrases, acronyms, and near-topic distractors | 16 documents / 14 queries / 27 graded judgments | Yes |
| BEIR SciFact | External scientific-claim-to-abstract retrieval with independent relevance labels | 5,183 documents / 300 judged test queries | Downloaded locally with a verified official archive |
| BEIR NFCorpus | Optional medical question-to-document retrieval | 3,633 documents / 323 BEIR test queries | Download on demand; terms restrict non-academic use |

`aum-docs-v1` is a fictional, self-authored corpus dedicated to CC0. Its
source files live in `documents/`, its standard retrieval representation is
`corpus.jsonl`, and `qrels/test.tsv` uses the BEIR TSV convention. The corpus
ids intentionally match the relative paths AUM exposes as `display_path` after
ingestion.

Run the dependency-free structural check and scorer first:

```sh
python3 evaluation/scripts/check_fixture.py
python3 evaluation/scripts/score_run.py \
  --qrels evaluation/datasets/aum-docs-v1/qrels/test.tsv \
  --run evaluation/examples/aum-docs-v1-perfect.trec \
  --cutoffs 1,3,10
```

The example run is deliberately perfect: it should report 1.0000 for
nDCG@10, MRR@10, and Recall@10. `empty-run.jsonl` is a regression example for
a legitimate zero-result API run; it scores zero rather than being rejected.
The scorer accepts either six-column TREC runs or the JSONL produced by the
collector. Its default linear nDCG gain matches BEIR/pytrec_eval conventions;
use `--gain exponential` only when comparing against a system that uses that
different definition.

To compare AUM modes on the local fixture, ingest the source directory into a
fresh index, generate embeddings with one fixed model, then collect one run per
mode and ratio:

```sh
aum ingest aum-eval evaluation/datasets/aum-docs-v1/documents
AUM_EMBEDDINGS__ENABLED=true aum embed aum-eval

python3 evaluation/scripts/collect_api_run.py \
  --queries evaluation/datasets/aum-docs-v1/queries.jsonl \
  --index aum-eval --type text --out evaluation/runs/aum-docs-text.jsonl
python3 evaluation/scripts/collect_api_run.py \
  --queries evaluation/datasets/aum-docs-v1/queries.jsonl \
  --index aum-eval --type hybrid --semantic-ratio 0.5 \
  --out evaluation/runs/aum-docs-hybrid-050.jsonl

python3 evaluation/scripts/score_run.py \
  --qrels evaluation/datasets/aum-docs-v1/qrels/test.tsv \
  --run evaluation/runs/aum-docs-hybrid-050.jsonl \
  --cutoffs 1,3,10 --per-query evaluation/runs/aum-docs-hybrid-050.metrics.json
```

The collector records `elapsed_ms`, index, mode, ratio, endpoint, limit, and
the results returned for each query. Keep the embedding model and the indexed
corpus fixed while comparing text, hybrid 0.0, hybrid 0.5, and hybrid 1.0.
Use the per-query output to investigate a metric change before treating it as a
quality improvement. The fixture deliberately mixes semantic wins with cases
where exact filename or policy wording should still rank first.

For an external run, acquire and materialize SciFact. Downloaded data and
generated runs are ignored by git; each download gets a `SOURCE.json` with the
publisher checksum and observed SHA-256.

```sh
python3 evaluation/scripts/fetch_beir.py scifact --accept-license
python3 evaluation/scripts/materialize_beir.py \
  --dataset-dir evaluation/external/scifact

aum ingest scifact-eval evaluation/external/scifact-documents/documents
AUM_EMBEDDINGS__ENABLED=true aum embed scifact-eval
python3 evaluation/scripts/collect_api_run.py \
  --queries evaluation/external/scifact/queries.jsonl \
  --qrels evaluation/external/scifact/qrels/test.tsv \
  --index scifact-eval --type hybrid --semantic-ratio 0.5 \
  --id-map evaluation/external/scifact-documents/document-map.tsv \
  --out evaluation/runs/scifact-hybrid-050.jsonl
python3 evaluation/scripts/score_run.py \
  --qrels evaluation/external/scifact/qrels/test.tsv \
  --run evaluation/runs/scifact-hybrid-050.jsonl --cutoffs 1,3,10
```

`materialize_beir.py` renders each BEIR title and text into a safe `.txt` file
and writes a display-path-to-corpus-id map. Passing that map to the collector
preserves the external qrels identifiers even though AUM derives its own file
document ids. The `--qrels` collector argument filters SciFact's shared query
file down to its judged test queries.

The controlled OpenSearch integration checks validate the mechanics of the
hybrid implementation. No broad benchmark score is claimed here: producing
one requires a running AUM API plus a fixed embedding service and model. The
commands above make that measurement reproducible once those services are
available.

Read [PROVENANCE.md](PROVENANCE.md) before using an external corpus. The BEIR
package format does not replace the license or attribution obligations of the
original data source.
