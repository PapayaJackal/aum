# Search improvement experiments

This plan is based on the current working-tree implementation, reviewed on
2026-09-05 and implemented in part on 2026-09-06. Benchmark measurements are recorded separately in `evaluation/runs/`.
Proposals below are hypotheses unless accompanied by a measured comparison.
See [the benchmark report](../evaluation/BENCHMARK-2026-09-05.md) for the live
Terra runs and reproduction commands.

The lexical ablation measured nDCG@10 of 0.1180 → 0.9314 on the AUM fixture
and 0.0180 → 0.6502 on SciFact when both field-match operators changed from
AND to OR. All 14 and 300 judged queries were collected respectively. These
are exploratory lexical results, not measured hybrid improvements: there was
no configured embedding service for semantic runs on September 5. The follow-up
on September 6 installed a local embedding model and applied the fixes below.

## 1. Restore full document text to the embedding pipeline

Fixed on September 6 with a dedicated `EmbeddingDocument` type. Both initial
and retry scrolls now read full stored content; the embedding pipeline cannot
accidentally use a display snippet. Regression tests cover both scroll paths
and evidence beyond character 200.

The original correctness issue was upstream of ranking:

- `aum-core/src/search/opensearch/mod.rs::fetch_unembedded_cursor` calls
  `search_raw`, which parses documents through `parse_hit`.
- `aum-core/src/search/opensearch/parse.rs::parse_hit` puts the first 200
  content characters in `SearchResult.snippet` when there is no highlight.
- `aum-core/src/embeddings/pipeline.rs::scroll_source` chunks `doc.snippet`.
- The explicit-document retry path (`scroll_documents`) also uses `search_raw`.

Consequently, the old OpenSearch embedding path saw only the document prefix.
Existing affected indices need re-embedding after the fix; changing query
weights cannot restore the missing information.

All 5,183 downloaded SciFact records exceed 200 characters when represented as
title plus two newlines plus text. Median length is 1,427 characters; prefix-only
input retains about 13.3% of aggregate characters. These are corpus diagnostics,
not estimates of retrieval quality loss. Compare current prefix embeddings with
full-content chunks using the same model and query vectors.

## 2. Improve lexical candidate recall while protecting identifiers

`build_text_query` previously required every query term to match content, or
every term to match the path. The implemented query uses OR in both fields and
retains the filename boost of 2. Questions containing absent filler words and
terms distributed between those fields can therefore lose lexical candidates.

Potential further experiments include: relaxed content matching, a term-centric query
across compatible text fields, and relaxed matching with phrase/exact-path
boosts. Keep exact identifiers, error codes, and named entities as separate
evaluation slices. Different content/path analyzers mean a cross-field query
must be checked against actual tokenization rather than assumed equivalent.
Prefer the smallest change that improves recall and nDCG without regressing
exact-document queries. Test the lexical branch alone and in fusion.
On 809 SciFact training queries, OR achieved nDCG@10 of 0.6594 versus 0.6268
for `2<50%` minimum term matching and 0.3641 for `2<75%`. Adding a content
phrase boost of 1 tied OR. Keep simple OR: the additional tested restrictions
lost recall and the phrase boost added no measured value. Reproduce with
`python3 evaluation/scripts/compare_lexical_variants.py`.

The measured OR baseline has SciFact Recall@100 of 0.8842 versus Recall@10
of 0.7665, leaving both a candidate-recall gap and room for reranking. The
fixture's OR Recall@100 remains 0.8929, equal to Recall@10; some judged
documents still need a different retrieval signal even with relaxed matching.

## 3. Tune fusion after fixing its inputs

Hold corpus, model, chunks, and filters constant. Sweep semantic weights
0, 0.25, 0.5, 0.75, 1; then test candidate depths 100, 300, 1,000 and RRF rank
constants 10, 30, 60. Measure Recall@100 as a candidate-coverage diagnostic in
addition to nDCG@10, MRR@10, Recall@10, and latency. Change one factor at a time.
Ensure candidate depth covers the requested page and test ranking consistency
across pages. Keep settings fixed before reporting held-out results; a sweep on
test judgments is exploratory and does not establish a generalizable winner.

## 4. Return counts and snippets consistent with retrieval

The API performs a separate lexical count even for semantic retrieval, while
the OpenSearch search helpers discard their returned totals. Semantic results
can therefore coexist with zero reported matches. Return results and count
metadata through one backend response, retaining exact versus bounded/approximate
count semantics. Validate facet behavior against the same retrieval population.
Implemented: the API now disables facet aggregations on its hit stream,
because its separate count request already supplies response facets. The
search stream previously computed and discarded duplicate aggregations.

Store chunk text or offsets with embeddings and return the best matching chunk
as the semantic snippet. This also enables a later reranking experiment on
actual matching passages instead of document prefixes.

## 5. Consider reranking and caching after establishing a baseline

If relevant documents are present in the top 100 but absent from the top 10,
test a reranker on the top 50–100 candidates. If they are absent from the
candidate pool, fix retrieval first. Report reranking latency separately.

For repeated queries and slider changes, reuse query embeddings through a
bounded cache keyed by query text, backend/model identity, dimensions, prefix,
and model revision where available. Cache and reuse embedding clients as well:
the API currently constructs a new client for each query. Measure cold and warm
latency separately so cache effects do not masquerade as ranking improvements.
Implemented: exclude `chunks.embedding` from text/hybrid search response
`_source`, since the result parser never uses the vectors.
Measure transferred bytes and response parsing time before and after this
change, retaining full content on the dedicated embedding-source path.

## Acceptance and coverage

Use the 14-query AUM fixture as a smoke test, not sufficient evidence for a
new default. Use external judgments for broader retrieval evidence and grow a
held-out mixed-file evaluation set with filename lookups, long-document evidence,
filters, multiple indices, acronyms, and paraphrases. Report missing queries,
index/embedding coverage, per-query wins and losses, and paired uncertainty for
small aggregate changes. Preserve raw runs and exact model/index configuration.

## Implemented-change validation (September 6)

- `cargo test -p aum-core --all-features`: 332 tests and two doctests passed;
  the separately gated live engine test was run against local OpenSearch 3.6.0
  and passed (weights, endpoints, filters, and pagination).
- `cargo test -p aum-api -p aum-cli --quiet`: passed.
- `cargo build -p aum-cli -p aum-api`, formatting, and diff whitespace checks:
  passed.
- Strict core-library Clippy found three pre-existing warnings in extraction,
  rate limiting, and Meilisearch settings. It passed with only
  `clippy::map_unwrap_or` and `clippy::duration_suboptimal_units` allowed.

The fixed full-content MiniLM fixture run at semantic ratio 0.5 improved
nDCG@10 on six queries and tied the other eight versus the new OR lexical
baseline. Gains include the expense policy, broadband reimbursement, laptop
approval, object restoration, retention, and slow-dashboard queries. Every
query still has a relevant top result. These 14 queries are a regression
check, not sufficient evidence for broad retrieval claims.
