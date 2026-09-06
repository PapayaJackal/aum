#!/usr/bin/env python3
"""Turn a downloaded BEIR corpus.jsonl into safe, AUM-ingestible text files.

The original BEIR corpus and qrels remain authoritative. A generated TSV maps
the opaque source filenames back to BEIR corpus ids so ``collect_api_run.py``
can score AUM's relative display paths against the original qrels.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def read_corpus(path: Path) -> list[tuple[str, str, str]]:
    documents: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            record: Any = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{line_no}: invalid JSON: {exc.msg}") from exc
        if not isinstance(record, dict):
            raise ValueError(f"{path}:{line_no}: expected JSON object")
        corpus_id, title, text = record.get("_id"), record.get("title", ""), record.get("text", "")
        if not isinstance(corpus_id, str) or not corpus_id:
            raise ValueError(f"{path}:{line_no}: corpus item has no non-empty _id")
        if not isinstance(title, str) or not isinstance(text, str) or not (title.strip() or text.strip()):
            raise ValueError(f"{path}:{line_no}: corpus item {corpus_id!r} has no usable title or text")
        if corpus_id in seen:
            raise ValueError(f"{path}:{line_no}: duplicate corpus id {corpus_id!r}")
        seen.add(corpus_id)
        documents.append((corpus_id, title.strip(), text.strip()))
    if not documents:
        raise ValueError(f"corpus is empty: {path}")
    return documents


def document_name(position: int, corpus_id: str) -> str:
    """Keep source filenames portable and prevent corpus ids from becoming paths."""
    digest = hashlib.sha256(corpus_id.encode("utf-8")).hexdigest()[:16]
    return f"{position:06d}-{digest}.txt"


def write_materialization(destination: Path, source: Path, documents: list[tuple[str, str, str]]) -> None:
    docs_directory = destination / "documents"
    docs_directory.mkdir(parents=True)
    map_rows = ["display-path\tcorpus-id"]
    for position, (corpus_id, title, text) in enumerate(documents, 1):
        display_path = document_name(position, corpus_id)
        body = f"{title}\n\n{text}\n" if title else f"{text}\n"
        (docs_directory / display_path).write_text(body, encoding="utf-8")
        map_rows.append(f"{display_path}\t{corpus_id}")
    (destination / "document-map.tsv").write_text("\n".join(map_rows) + "\n", encoding="utf-8")
    (destination / "MATERIALIZED.json").write_text(
        json.dumps(
            {
                "source_corpus": str(source.resolve()),
                "documents": len(documents),
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "format": "UTF-8 text files plus display-path/corpus-id TSV map",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-dir", required=True, type=Path, help="extracted BEIR dataset directory")
    parser.add_argument("--output-dir", type=Path, help="generated AUM source directory")
    parser.add_argument("--force", action="store_true", help="replace an existing generated directory")
    args = parser.parse_args()

    source = args.dataset_dir.resolve() / "corpus.jsonl"
    destination = (
        args.output_dir.resolve()
        if args.output_dir
        else args.dataset_dir.resolve().with_name(args.dataset_dir.resolve().name + "-documents")
    )
    if destination == destination.parent:
        parser.error("--output-dir cannot be a filesystem root")
    try:
        documents = read_corpus(source)
    except (OSError, ValueError) as exc:
        print(f"materialisation failed: {exc}", file=sys.stderr)
        return 2
    if destination.exists() and not args.force:
        print(f"refusing to overwrite existing output: {destination} (pass --force to replace it)", file=sys.stderr)
        return 2

    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.build-", dir=destination.parent))
    try:
        write_materialization(staging, source, documents)
        if destination.exists():
            shutil.rmtree(destination)
        shutil.move(str(staging), str(destination))
    except OSError as exc:
        print(f"materialisation failed: {exc}", file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    print(f"materialised {len(documents)} documents at {destination}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
