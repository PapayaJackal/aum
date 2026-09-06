#!/usr/bin/env python3
"""Score a retrieval run against BEIR-style qrels without third-party packages.

Supported runs are standard six-column TREC files and JSON Lines records of
the form {"query_id": "...", "results": [{"corpus_id": "...", "rank": 1}]}.
Scores are reported over every query in the qrels; a missing or explicitly
empty run contributes zero, which makes incomplete runs visible instead of
silently inflating scores.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


Judgments = dict[str, dict[str, float]]
Runs = dict[str, list[str]]


def nonempty_lines(path: Path) -> Iterable[tuple[int, str]]:
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if line and not line.startswith("#"):
            yield number, line


def parse_float(value: str, path: Path, line_no: int, name: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{path}:{line_no}: {name} is not a number: {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{path}:{line_no}: {name} must be finite")
    return parsed


def read_qrels(path: Path) -> Judgments:
    qrels: defaultdict[str, dict[str, float]] = defaultdict(dict)
    for line_no, line in nonempty_lines(path):
        fields = line.split("\t") if "\t" in line else line.split()
        lowered = [field.lower() for field in fields]
        if lowered[:3] == ["query-id", "corpus-id", "score"]:
            continue
        if len(fields) == 3:
            query_id, corpus_id, score_text = fields
        elif len(fields) >= 4:
            # TREC qrels convention: query-id, iteration, corpus-id, score.
            query_id, corpus_id, score_text = fields[0], fields[2], fields[3]
        else:
            raise ValueError(f"{path}:{line_no}: expected BEIR or TREC qrels fields")
        score = parse_float(score_text, path, line_no, "qrel score")
        qrels[query_id][corpus_id] = max(score, qrels[query_id].get(corpus_id, score))
    if not qrels:
        raise ValueError(f"qrels are empty: {path}")
    return dict(qrels)


def deduplicate(entries: list[tuple[int, float, int, str]]) -> list[str]:
    """Sort entries by explicit rank, then score, while retaining stable input order."""
    seen: set[str] = set()
    output: list[str] = []
    for _, _, _, corpus_id in sorted(entries, key=lambda item: (item[0], -item[1], item[2])):
        if corpus_id not in seen:
            output.append(corpus_id)
            seen.add(corpus_id)
    return output


def read_trec_run(path: Path) -> Runs:
    rows: defaultdict[str, list[tuple[int, float, int, str]]] = defaultdict(list)
    for order, (line_no, line) in enumerate(nonempty_lines(path), 1):
        fields = line.split()
        if len(fields) < 6:
            raise ValueError(f"{path}:{line_no}: expected six-column TREC run format")
        query_id, corpus_id = fields[0], fields[2]
        try:
            rank = int(fields[3])
        except ValueError as exc:
            raise ValueError(f"{path}:{line_no}: rank is not an integer") from exc
        score = parse_float(fields[4], path, line_no, "run score")
        if rank < 1:
            raise ValueError(f"{path}:{line_no}: rank must be at least 1")
        rows[query_id].append((rank, score, order, corpus_id))
    return {query_id: deduplicate(entries) for query_id, entries in rows.items()}


def result_id(result: Any, path: Path, line_no: int) -> str:
    if isinstance(result, str) and result:
        return result
    if isinstance(result, dict):
        for field in ("corpus_id", "display_path", "doc_id", "id", "_id"):
            value = result.get(field)
            if isinstance(value, str) and value:
                return value
    raise ValueError(f"{path}:{line_no}: each JSONL result needs a corpus_id (or display_path/doc_id)")


def read_jsonl_run(path: Path) -> Runs:
    rows: defaultdict[str, list[tuple[int, float, int, str]]] = defaultdict(list)
    records = 0
    for input_order, (line_no, line) in enumerate(nonempty_lines(path), 1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{line_no}: invalid JSON: {exc.msg}") from exc
        if not isinstance(record, dict):
            raise ValueError(f"{path}:{line_no}: expected a JSON object")
        query_id = record.get("query_id", record.get("_id"))
        results = record.get("results")
        if not isinstance(query_id, str) or not query_id:
            raise ValueError(f"{path}:{line_no}: record needs query_id")
        if not isinstance(results, list):
            raise ValueError(f"{path}:{line_no}: record needs a results list")
        records += 1
        # Materialise the key before iterating so ``{"results": []}`` is a
        # valid submitted zero-result query rather than an invalid empty run.
        rows[query_id]
        for result_order, result in enumerate(results, 1):
            corpus_id = result_id(result, path, line_no)
            rank = result_order
            score = 0.0
            if isinstance(result, dict):
                rank_value = result.get("rank", result_order)
                score_value = result.get("score", 0.0)
                if not isinstance(rank_value, int) or rank_value < 1:
                    raise ValueError(f"{path}:{line_no}: result rank must be a positive integer")
                if not isinstance(score_value, (int, float)) or not math.isfinite(float(score_value)):
                    raise ValueError(f"{path}:{line_no}: result score must be finite")
                rank, score = rank_value, float(score_value)
            rows[query_id].append((rank, score, input_order * 1_000_000 + result_order, corpus_id))
    if not records:
        raise ValueError(f"run is empty: {path}")
    return {query_id: deduplicate(entries) for query_id, entries in rows.items()}


def detect_run_format(path: Path) -> str:
    for _, line in nonempty_lines(path):
        return "jsonl" if line.startswith("{") else "trec"
    raise ValueError(f"run is empty: {path}")


def dcg(relevances: Iterable[float], gain: str) -> float:
    if gain == "linear":
        gains = relevances
    else:
        gains = (2.0**relevance - 1.0 for relevance in relevances)
    return sum(value / math.log2(position + 1) for position, value in enumerate(gains, 1))


def score_query(
    judgments: dict[str, float], retrieved: list[str], cutoff: int, gain: str
) -> dict[str, float]:
    relevant = {corpus_id: score for corpus_id, score in judgments.items() if score > 0}
    top = retrieved[:cutoff]
    gains = [relevant.get(corpus_id, 0.0) for corpus_id in top]
    ideal = sorted(relevant.values(), reverse=True)[:cutoff]
    ideal_dcg = dcg(ideal, gain)
    ndcg = dcg(gains, gain) / ideal_dcg if ideal_dcg else 0.0
    reciprocal_rank = 0.0
    for position, corpus_id in enumerate(top, 1):
        if corpus_id in relevant:
            reciprocal_rank = 1.0 / position
            break
    recall = len(set(top) & set(relevant)) / len(relevant) if relevant else 0.0
    return {"ndcg": ndcg, "mrr": reciprocal_rank, "recall": recall}


def parse_cutoffs(value: str) -> list[int]:
    try:
        cutoffs = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cutoffs must be comma-separated positive integers") from exc
    if not cutoffs or any(cutoff < 1 for cutoff in cutoffs):
        raise argparse.ArgumentTypeError("cutoffs must contain positive integers")
    return sorted(set(cutoffs))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qrels", required=True, type=Path, help="BEIR TSV or TREC qrels file")
    parser.add_argument("--run", required=True, type=Path, help="TREC run or JSONL run file")
    parser.add_argument("--format", choices=("auto", "trec", "jsonl"), default="auto")
    parser.add_argument("--cutoffs", type=parse_cutoffs, default=[10], help="e.g. 1,3,10 (default: 10)")
    parser.add_argument(
        "--gain",
        choices=("linear", "exponential"),
        default="linear",
        help="nDCG gain function; linear matches BEIR/pytrec_eval (default: %(default)s)",
    )
    parser.add_argument("--json", action="store_true", help="write a machine-readable summary")
    parser.add_argument("--per-query", type=Path, help="write per-query metric values as JSON")
    args = parser.parse_args()

    try:
        qrels = read_qrels(args.qrels)
        run_format = detect_run_format(args.run) if args.format == "auto" else args.format
        runs = read_jsonl_run(args.run) if run_format == "jsonl" else read_trec_run(args.run)
    except (OSError, ValueError) as exc:
        print(f"scoring failed: {exc}", file=sys.stderr)
        return 2

    per_query: dict[str, dict[str, dict[str, float]]] = {}
    aggregate: dict[int, dict[str, float]] = {}
    for cutoff in args.cutoffs:
        totals = {"ndcg": 0.0, "mrr": 0.0, "recall": 0.0}
        for query_id, judgments in qrels.items():
            values = score_query(judgments, runs.get(query_id, []), cutoff, args.gain)
            per_query.setdefault(query_id, {})[str(cutoff)] = values
            for metric, value in values.items():
                totals[metric] += value
        aggregate[cutoff] = {metric: value / len(qrels) for metric, value in totals.items()}

    summary: dict[str, Any] = {
        "queries": len(qrels),
        "queries_with_results": sum(1 for query_id in qrels if runs.get(query_id)),
        "extra_run_queries": sorted(set(runs) - set(qrels)),
        "format": run_format,
        "ndcg_gain": args.gain,
        "metrics": {
            str(cutoff): {
                f"nDCG@{cutoff}": values["ndcg"],
                f"MRR@{cutoff}": values["mrr"],
                f"Recall@{cutoff}": values["recall"],
            }
            for cutoff, values in aggregate.items()
        },
    }

    if args.per_query:
        args.per_query.parent.mkdir(parents=True, exist_ok=True)
        args.per_query.write_text(json.dumps(per_query, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(
            f"Scored {summary['queries']} qrels queries; "
            f"{summary['queries_with_results']} have at least one submitted result."
        )
        for cutoff in args.cutoffs:
            values = summary["metrics"][str(cutoff)]
            print(
                f"nDCG@{cutoff}: {values[f'nDCG@{cutoff}']:.4f}  "
                f"MRR@{cutoff}: {values[f'MRR@{cutoff}']:.4f}  "
                f"Recall@{cutoff}: {values[f'Recall@{cutoff}']:.4f}"
            )
        if summary["extra_run_queries"]:
            print("Ignored run-only query ids: " + ", ".join(summary["extra_run_queries"]), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
