#!/usr/bin/env python3
"""Fetch and verify selected official BEIR-format relevance datasets.

The archives are deliberately excluded from git. This command records the
source URL, published MD5, locally observed SHA-256, and retrieval time beside
the extracted files so an experiment remains auditable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any


BASE_URL = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets"
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "external"

DATASETS: dict[str, dict[str, str]] = {
    "scifact": {
        "md5": "5f7d1de60b170fc8027bb7898e2efca1",
        "license": (
            "SciFact claims and evidence annotations are CC BY 4.0; corpus abstracts are "
            "S2ORC material under ODC-By 1.0. See evaluation/PROVENANCE.md."
        ),
    },
    "nfcorpus": {
        "md5": "a89dba18a62ef92f7d323ec890a0d38d",
        "license": (
            "NFCorpus is free for academic use. Other use of included NutritionFacts.org "
            "data requires consulting its terms and contacting its author. See evaluation/PROVENANCE.md."
        ),
    },
}


def safe_extract(archive: zipfile.ZipFile, destination: Path) -> None:
    """Extract archive members only when their paths remain inside destination."""
    for member in archive.infolist():
        member_path = PurePosixPath(member.filename)
        if member_path.is_absolute() or ".." in member_path.parts:
            raise ValueError(f"archive contains unsafe path: {member.filename!r}")
        target = destination.joinpath(*member_path.parts)
        if target.resolve().parent != destination.resolve() and destination.resolve() not in target.resolve().parents:
            raise ValueError(f"archive member escapes destination: {member.filename!r}")
    archive.extractall(destination)


def download(url: str, target: Path) -> tuple[str, str]:
    md5 = hashlib.md5()  # noqa: S324 - compares the publisher's documented archive checksum.
    sha256 = hashlib.sha256()
    request = urllib.request.Request(url, headers={"User-Agent": "aum-evaluation-fetcher/1"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response, target.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                md5.update(chunk)
                sha256.update(chunk)
    except urllib.error.URLError as exc:
        raise RuntimeError(f"download failed for {url}: {exc.reason}") from exc
    return md5.hexdigest(), sha256.hexdigest()


def data_root(staging: Path, dataset: str) -> Path:
    expected = {"corpus.jsonl", "queries.jsonl", "qrels"}
    direct = {entry.name for entry in staging.iterdir()}
    if expected <= direct:
        return staging
    named = staging / dataset
    if named.is_dir() and expected <= {entry.name for entry in named.iterdir()}:
        return named
    directories = [entry for entry in staging.iterdir() if entry.is_dir()]
    if len(directories) == 1 and expected <= {entry.name for entry in directories[0].iterdir()}:
        return directories[0]
    raise ValueError("archive does not contain the expected BEIR corpus.jsonl, queries.jsonl, and qrels layout")


def write_manifest(destination: Path, values: dict[str, Any]) -> None:
    (destination / "SOURCE.json").write_text(
        json.dumps(values, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=sorted(DATASETS), help="dataset to download")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT, help="directory for extracted data")
    parser.add_argument("--accept-license", action="store_true", help="confirm that you reviewed the source terms")
    parser.add_argument("--force", action="store_true", help="replace an existing extracted dataset")
    args = parser.parse_args()

    if not args.accept_license:
        parser.error("--accept-license is required after reviewing evaluation/PROVENANCE.md")

    dataset = args.dataset
    details = DATASETS[dataset]
    url = f"{BASE_URL}/{dataset}.zip"
    output_dir = args.output_dir.resolve()
    destination = output_dir / dataset
    output_dir.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not args.force:
        print(f"refusing to overwrite existing dataset: {destination} (pass --force to replace it)", file=sys.stderr)
        return 2

    archive_path = output_dir / f".{dataset}.zip"
    staging = Path(tempfile.mkdtemp(prefix=f".{dataset}.extract-", dir=output_dir))
    try:
        print(f"downloading {url}", file=sys.stderr)
        observed_md5, observed_sha256 = download(url, archive_path)
        if observed_md5 != details["md5"]:
            raise ValueError(
                f"checksum mismatch: expected MD5 {details['md5']}, received {observed_md5}; archive was not extracted"
            )
        with zipfile.ZipFile(archive_path) as archive:
            safe_extract(archive, staging)
        root = data_root(staging, dataset)
        if destination.exists():
            shutil.rmtree(destination)
        shutil.move(str(root), str(destination))
        write_manifest(
            destination,
            {
                "dataset": dataset,
                "download_url": url,
                "published_md5": details["md5"],
                "observed_md5": observed_md5,
                "observed_sha256": observed_sha256,
                "retrieved_at_utc": datetime.now(UTC).isoformat(),
                "license_notice": details["license"],
                "layout": "BEIR: corpus.jsonl, queries.jsonl, qrels/<split>.tsv",
            },
        )
    except (OSError, RuntimeError, ValueError, zipfile.BadZipFile) as exc:
        print(f"dataset fetch failed: {exc}", file=sys.stderr)
        return 1
    finally:
        archive_path.unlink(missing_ok=True)
        shutil.rmtree(staging, ignore_errors=True)

    print(f"fetched and verified {dataset} at {destination}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
