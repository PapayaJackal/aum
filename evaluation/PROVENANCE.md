# Dataset provenance and terms

## `aum-docs-v1`

The fixture's documents, query text, relevance judgments, and directory layout
were written for this repository. They use fictional people, organisations,
services, email addresses, and policies. The dataset is dedicated to the public
domain under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/); see
`datasets/aum-docs-v1/LICENSE.md`.

It is intentionally small and hand-judged. It is a regression suite, not a
claim that its score predicts retrieval quality for a user's real document
collection. Its strongest use is to catch ranking regressions in distinct
behaviours: exact title/path matching, policy-question paraphrases, acronyms,
named entities, technical tasks, email subjects, and plausible distractors.

## BEIR SciFact

The fetcher downloads the official BEIR-format archive from the URL documented
by the [BEIR dataset list](https://github.com/beir-cellar/beir/wiki/Datasets-available).
That list publishes the SciFact archive MD5
`5f7d1de60b170fc8027bb7898e2efca1`; `fetch_beir.py` refuses to extract an
archive with a different value and records an observed SHA-256 in `SOURCE.json`.

SciFact is the scientific fact-checking dataset released by
[AllenAI](https://github.com/allenai/scifact). Its authoritative license file
states that claims and evidence annotations are [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
while corpus abstracts from S2ORC are [ODC-By 1.0](https://opendatacommons.org/licenses/by/1-0/).
Its data schema is documented in the project's
[data documentation](https://github.com/allenai/scifact/blob/master/doc/data.md).

The checked-out BEIR archive observed on 2026-09-05 contains 5,183 documents,
1,109 queries in the shared query file, and 300 judged test queries (340 qrel
rows). SciFact is a useful independent relevance check for semantic claim-to-
abstract retrieval, but it is English-only, scientific, and mostly has binary
BEIR judgments. It does not test Tika extraction, email structure, metadata
facets, private-corpus vocabulary, or ordinary workplace document search.

Use the required attribution and comply with both source licenses when sharing
derived corpora or results. The BEIR project's own
[disclaimer](https://github.com/beir-cellar/beir#-disclaimer) explains that its
repackaging does not grant rights to every constituent dataset.

## BEIR NFCorpus (optional)

The fetcher also supports the BEIR NFCorpus archive and verifies the published
MD5 `a89dba18a62ef92f7d323ec890a0d38d`. NFCorpus provides a useful
non-technical-question to technical-medical-document test, which can reveal
semantic retrieval effects that SciFact misses.

Its original [NFCorpus site](https://webserver.cl.uni-heidelberg.de/statnlpgroup/nfcorpus/)
says it is free for academic purposes and requires users of included
NutritionFacts.org data to consult the terms of service and contact its author
for other uses. It is therefore not fetched by default and should not be used
for commercial or redistributed evaluation without resolving those terms.

## Measurement limits

All qrels are incomplete: an unjudged document is treated as non-relevant by
the supplied scorer, which is the ordinary offline IR convention but can
understate a system that retrieves sensible unjudged results. Compare systems
on the same corpus, qrels, query set, cutoff, embedding model, backend, and
index state. Report the mode and semantic ratio alongside metrics rather than
averaging scores across the local fixture and SciFact.
