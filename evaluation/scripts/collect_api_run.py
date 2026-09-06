#!/usr/bin/env python3
"""Execute aum API searches for a JSONL query set and write a scorer-ready run.

For ``aum-docs-v1``, ingest its ``documents/`` directory and use the default
``display_path`` identifier: AUM stores that path relative to the ingest root,
which is exactly the corpus id used in its qrels.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def read_qrel_ids(path: Path) -> set[str]:
    query_ids: set[str] = set()
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t") if "\t" in line else line.split()
        if [field.lower() for field in fields[:3]] == ["query-id", "corpus-id", "score"]:
            continue
        if len(fields) == 3:
            query_ids.add(fields[0])
        elif len(fields) >= 4:
            query_ids.add(fields[0])
        else:
            raise ValueError(f"{path}:{line_no}: expected BEIR or TREC qrels fields")
    if not query_ids:
        raise ValueError(f"qrels are empty: {path}")
    return query_ids


def read_queries(path: Path, include: set[str] | None = None) -> list[tuple[str, str]]:
    queries: list[tuple[str, str]] = []
    seen: set[str] = set()
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{line_no}: invalid JSON: {exc.msg}") from exc
        if not isinstance(record, dict):
            raise ValueError(f"{path}:{line_no}: expected a JSON object")
        query_id, text = record.get("_id"), record.get("text")
        if not isinstance(query_id, str) or not query_id or not isinstance(text, str) or not text:
            raise ValueError(f"{path}:{line_no}: query needs non-empty _id and text")
        if query_id in seen:
            raise ValueError(f"{path}:{line_no}: duplicate query id {query_id!r}")
        seen.add(query_id)
        if include is None or query_id in include:
            queries.append((query_id, text))
    if not queries:
        raise ValueError(f"queries are empty: {path}")
    if include is not None:
        found = {query_id for query_id, _ in queries}
        missing = sorted(include - found)
        if missing:
            raise ValueError(f"qrels reference query ids missing from {path}: {', '.join(missing[:10])}")
    return queries


def parse_headers(values: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for value in values:
        name, separator, header_value = value.partition(":")
        if not separator or not name.strip() or not header_value.strip():
            raise ValueError("--header values must use 'Name: value' syntax")
        headers[name.strip()] = header_value.strip()
    return headers


def read_id_map(path: Path) -> dict[str, str]:
    """Read either corpus-id/display-path or display-path/corpus-id TSV maps."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ValueError(f"identifier map is empty: {path}")
    header = lines[0].split("\t")
    if header == ["corpus-id", "display-path"]:
        corpus_column, display_column = 0, 1
    elif header == ["display-path", "corpus-id"]:
        display_column, corpus_column = 0, 1
    else:
        raise ValueError(
            f"{path}: expected a corpus-id/display-path or display-path/corpus-id TSV header"
        )
    mapping: dict[str, str] = {}
    for line_no, line in enumerate(lines[1:], 2):
        fields = line.split("\t")
        if len(fields) != 2 or not all(fields):
            raise ValueError(f"{path}:{line_no}: expected two non-empty tab-separated fields")
        display_path, corpus_id = fields[display_column], fields[corpus_column]
        if display_path in mapping:
            raise ValueError(f"{path}:{line_no}: duplicate display path {display_path!r}")
        mapping[display_path.replace("\\", "/")] = corpus_id
    if not mapping:
        raise ValueError(f"identifier map has no entries: {path}")
    return mapping


def get_result_id(result: dict[str, Any], id_field: str, strip_prefix: str) -> str:
    value = result.get(id_field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"API result does not contain a non-empty {id_field!r}")
    normalised = value.replace("\\", "/")
    prefix = strip_prefix.replace("\\", "/").rstrip("/")
    if prefix:
        prefix_with_slash = prefix + "/"
        if normalised == prefix:
            return ""
        if normalised.startswith(prefix_with_slash):
            normalised = normalised[len(prefix_with_slash) :]
    return normalised


def call_search(
    endpoint: str,
    params: dict[str, str],
    headers: dict[str, str],
    timeout: float,
) -> dict[str, Any]:
    url = endpoint + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"Accept": "application/json", **headers})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"HTTP {exc.code} for query {params['q']!r}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"unable to reach {endpoint}: {exc.reason}") from exc
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"search API returned invalid JSON: {exc.msg}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("results"), list):
        raise RuntimeError("search API response does not contain a results array")
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries", required=True, type=Path, help="JSONL file with _id and text fields")
    parser.add_argument("--qrels", type=Path, help="only collect query ids that have judgments in this qrels file")
    parser.add_argument(
        "--max-queries",
        type=int,
        help="collect only the first N selected query records (for a documented development subset)",
    )
    parser.add_argument("--base-url", default="http://localhost:8000", help="aum API host (default: %(default)s)")
    parser.add_argument("--index", required=True, help="AUM index containing the evaluated corpus")
    parser.add_argument("--type", choices=("text", "hybrid"), default="hybrid", help="search mode")
    parser.add_argument("--semantic-ratio", type=float, help="hybrid semantic weight from 0 to 1")
    parser.add_argument("--limit", type=int, default=100, help="results requested per query (1–200)")
    parser.add_argument("--id-field", default="display_path", help="API result field used as corpus id")
    parser.add_argument("--strip-prefix", default="", help="remove this path prefix from result identifiers")
    parser.add_argument(
        "--id-map",
        type=Path,
        help="TSV mapping AUM display paths to qrels corpus ids (needed after BEIR materialisation)",
    )
    parser.add_argument("--header", action="append", default=[], help="extra HTTP header, e.g. 'Authorization: Bearer TOKEN'")
    parser.add_argument("--timeout", type=float, default=30.0, help="seconds per HTTP request")
    parser.add_argument("--out", type=Path, help="write JSONL to this file instead of stdout")
    args = parser.parse_args()

    if not 1 <= args.limit <= 200:
        parser.error("--limit must be between 1 and 200")
    if args.semantic_ratio is not None and not 0.0 <= args.semantic_ratio <= 1.0:
        parser.error("--semantic-ratio must be between 0 and 1")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.semantic_ratio is not None and args.type != "hybrid":
        parser.error("--semantic-ratio requires --type hybrid")

    try:
        judged_query_ids = read_qrel_ids(args.qrels) if args.qrels else None
        queries = read_queries(args.queries, judged_query_ids)
        if args.max_queries is not None:
            if args.max_queries < 1:
                raise ValueError("--max-queries must be positive")
            queries = queries[: args.max_queries]
        headers = parse_headers(args.header)
        id_map = read_id_map(args.id_map) if args.id_map else None
    except (OSError, ValueError) as exc:
        print(f"run collection failed: {exc}", file=sys.stderr)
        return 2

    endpoint = args.base_url.rstrip("/") + "/api/search"
    # Persist a restartable diagnostic trail for long external runs.  A complete
    # run is published atomically; an interrupted run remains as ``.partial``
    # with every query completed before the interruption.
    partial_path = args.out.with_suffix(args.out.suffix + ".partial") if args.out else None
    output_file = None
    if partial_path:
        partial_path.parent.mkdir(parents=True, exist_ok=True)
        output_file = partial_path.open("w", encoding="utf-8")
    lines: list[str] = []
    for number, (query_id, query) in enumerate(queries, 1):
        params = {"q": query, "index": args.index, "type": args.type, "limit": str(args.limit)}
        if args.semantic_ratio is not None:
            params["semantic_ratio"] = str(args.semantic_ratio)
        try:
            started = time.perf_counter()
            response = call_search(endpoint, params, headers, args.timeout)
            elapsed_ms = round((time.perf_counter() - started) * 1_000, 3)
            results: list[dict[str, Any]] = []
            for rank, raw_result in enumerate(response["results"], 1):
                if not isinstance(raw_result, dict):
                    raise ValueError("API result is not an object")
                corpus_id = get_result_id(raw_result, args.id_field, args.strip_prefix)
                if not corpus_id:
                    raise ValueError("normalising an API result produced an empty corpus id")
                if id_map is not None:
                    try:
                        corpus_id = id_map[corpus_id]
                    except KeyError as exc:
                        raise ValueError(
                            f"API result identifier {corpus_id!r} is absent from {args.id_map}"
                        ) from exc
                score = raw_result.get("score", 0.0)
                if not isinstance(score, (int, float)):
                    raise ValueError("API result score is not numeric")
                results.append({"corpus_id": corpus_id, "rank": rank, "score": score})
        except (RuntimeError, ValueError) as exc:
            print(f"run collection failed for {query_id} ({number}/{len(queries)}): {exc}", file=sys.stderr)
            return 1
        line = json.dumps(
            {
                "query_id": query_id,
                "query": query,
                "results": results,
                "elapsed_ms": elapsed_ms,
                "search": {
                    "endpoint": endpoint,
                    "index": args.index,
                    "type": args.type,
                    "semantic_ratio": args.semantic_ratio,
                    "limit": args.limit,
                    "id_map": str(args.id_map) if args.id_map else None,
                    "qrels": str(args.qrels) if args.qrels else None,
                },
            },
            separators=(",", ":"),
        )
        if output_file:
            output_file.write(line + "\n")
            output_file.flush()
        else:
            lines.append(line)

    if args.out:
        assert output_file is not None and partial_path is not None
        output_file.close()
        partial_path.replace(args.out)
        print(f"wrote {len(queries)} query results to {args.out}", file=sys.stderr)
    else:
        sys.stdout.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
