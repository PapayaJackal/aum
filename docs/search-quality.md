# Hybrid search quality assessment

The default OpenSearch path had two demonstrable problems: `semantic_ratio`
was discarded and vector `k` was set to page size. The former made the UI
slider ineffective; the latter restricted fusion to a shallow semantic pool
and did not cover the requested offset.

The implementation now uses request-local weighted RRF with rank constant 60,
true keyword/vector endpoints, and a 1,000-candidate minimum for vector
retrieval and hybrid pagination. Filters remain in both retrieval branches.
Invalid ratios are rejected; API searches across indices also require matching
embedding dimensions and query prefixes. Ratio zero does not need an embedding
service or embedded index, in either the API or CLI.

The September 6 follow-up fixes two further input-quality problems. Lexical
matching now uses OR across query terms in content/path, preserving the path
boost of 2. Embedding scrolls now return a dedicated full-content document
instead of the 200-character search snippet, for both initial and retry jobs.
Text and hybrid search responses exclude unused embedding vectors from `_source`.
The API also avoids redundant facet aggregations on the hit stream; its separate
count operation still supplies the response facets.
Existing embeddings produced through the old OpenSearch path need regeneration.

Live benchmark results and exact setup are recorded in
[the September 5 baseline](../evaluation/BENCHMARK-2026-09-05.md) and
[the September 6 evaluation](../evaluation/BENCHMARK-2026-09-06.md).
On 809 SciFact training queries, OR achieved nDCG@10 0.6594; tested stricter
term thresholds reduced it to 0.6268 and 0.3641. A phrase boost tied OR, so the
implemented lexical change retains the simpler OR query.

Regression tests exercise actual backend HTTP requests through a mock server,
including slider weights, endpoint behavior, filters, pagination, invalid
ratios, and candidate bounds. They do not emulate the OpenSearch ranker.
A live integration test passed against an isolated OpenSearch 3.6.0 container.
Its four-document corpus uses controlled two-dimensional vectors and verifies
that low/high semantic weights change the top result, ratio endpoints work,
PDF filtering selects the eligible semantic match, and the second vector page
returns the next document. This verifies engine compatibility and retrieval
behavior, not embedding-model quality or aggregate benchmark gains.

Run it against an expendable local engine with:

```sh
AUM_TEST_OPENSEARCH_URL=http://127.0.0.1:19200 \
  cargo test -p aum-core live_hybrid_ranking -- --ignored --nocapture
```

The test creates and deletes an index named `aum-quality-test-<process id>`.
Public-dataset nDCG and latency comparisons are recorded in `evaluation/`.

## Evaluation procedure

Use the datasets and tools in `evaluation/`. Measure keyword-only, vector-only,
and hybrid search on identical queries and corpora. Sweep ratios 0.25, 0.5,
and 0.75 on a development set; reserve held-out judgments for final reporting.
Compare nDCG@10, reciprocal rank, recall@10, and latency. Include file identifiers,
paraphrases, distractors, multi-index searches, filters, and later pages.
Keep corpus, embedding model/dimension/prefix, index settings, and engine
version fixed between runs. Public passage benchmarks do not substitute for
judgments on the user's own mixed-document corpus.

## Remaining limitations

- API totals and facets still come from keyword-only counting. Semantic-only
  hits can appear despite a zero total; pagination UI can consequently mislead.
  A follow-up should return counts/facets from the same retrieval operation,
  with explicit approximate-total semantics for bounded vector retrieval.
- Lexical matching is now deliberately broad. Keep exact identifiers and
  filename lookups in regression datasets when changing lexical boosts.
- Semantic snippets fall back to the first 200 content characters, rather than
  the matching chunk. Chunk text/offsets are not currently stored with vectors.
- Candidate depth is a starting value, not a tuned optimum. More candidates can
  increase latency; ranking can shift when depth expands beyond 1,000.
- Meilisearch retains its native hybrid scoring; ratios and scores are not
  numerically comparable across the two engines.

## Engine references

- [Weighted RRF](https://docs.opensearch.org/latest/search-plugins/search-pipelines/score-ranker-processor/)
- [Per-request pipelines](https://docs.opensearch.org/latest/search-plugins/search-pipelines/using-search-pipeline/)
- [Hybrid pagination depth](https://docs.opensearch.org/latest/vector-search/ai-search/hybrid-search/pagination/)
- [Nested vector filtering](https://docs.opensearch.org/latest/vector-search/specialized-operations/nested-search-knn/)

The repository pins OpenSearch 3.6.0. Older engine releases may lack weighted
RRF or pagination support; the old README's blanket “2.x+” is insufficient
for the hybrid feature set.
