from pathlib import Path
from unittest.mock import MagicMock

from aum.extraction.base import RecordErrorFn
from aum.ingest.pipeline import IngestPipeline
from aum.ingest.tracker import JobTracker
from aum.models import Document, IngestJob, JobStatus, JobType


class TestJobTracker:
    def test_create_and_get_job(self, tracker: JobTracker):
        job = tracker.create_job("test1", Path("/data"), total_files=100)
        assert job.job_id == "test1"
        assert job.status == JobStatus.RUNNING

        fetched = tracker.get_job("test1")
        assert fetched is not None
        assert fetched.total_files == 100

    def test_update_progress(self, tracker: JobTracker):
        tracker.create_job("prog1", Path("/data"), total_files=50)
        tracker.update_progress("prog1", extracted=30, processed=25, failed=2)

        job = tracker.get_job("prog1")
        assert job is not None
        assert job.extracted == 30
        assert job.processed == 25
        assert job.failed == 2

    def test_update_total_files(self, tracker: JobTracker):
        tracker.create_job("total1", Path("/data"), total_files=0)
        tracker.update_total_files("total1", 500)

        job = tracker.get_job("total1")
        assert job is not None
        assert job.total_files == 500

    def test_record_and_get_errors(self, tracker: JobTracker):
        tracker.create_job("err1", Path("/data"), total_files=10)
        tracker.record_error("err1", Path("/data/bad.pdf"), "ExtractionError", "corrupt file")
        tracker.record_error("err1", Path("/data/bad2.doc"), "TimeoutError", "tika timeout")

        errors = tracker.get_errors("err1")
        assert len(errors) == 2
        assert errors[0].error_type == "ExtractionError"
        assert errors[1].file_path == Path("/data/bad2.doc")

    def test_complete_job(self, tracker: JobTracker):
        tracker.create_job("done1", Path("/data"), total_files=5)
        tracker.complete_job("done1", JobStatus.COMPLETED)

        job = tracker.get_job("done1")
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        assert job.finished_at is not None

    def test_list_jobs_filter(self, tracker: JobTracker):
        tracker.create_job("a", Path("/a"), total_files=1)
        tracker.create_job("b", Path("/b"), total_files=1)
        tracker.complete_job("a", JobStatus.COMPLETED)

        running = tracker.list_jobs(status=JobStatus.RUNNING)
        assert len(running) == 1
        assert running[0].job_id == "b"

        all_jobs = tracker.list_jobs()
        assert len(all_jobs) == 2

    def test_get_nonexistent_job(self, tracker: JobTracker):
        assert tracker.get_job("nope") is None

    def test_create_job_default_type_is_ingest(self, tracker: JobTracker):
        job = tracker.create_job("type1", Path("/data"), total_files=1)
        assert job.job_type == JobType.INGEST

        fetched = tracker.get_job("type1")
        assert fetched is not None
        assert fetched.job_type == JobType.INGEST

    def test_create_embed_job(self, tracker: JobTracker):
        job = tracker.create_job("emb1", Path("."), total_files=50, job_type=JobType.EMBED)
        assert job.job_type == JobType.EMBED

        fetched = tracker.get_job("emb1")
        assert fetched is not None
        assert fetched.job_type == JobType.EMBED


class TestGetFailedPaths:
    def test_returns_distinct_paths(self, tracker: JobTracker):
        tracker.create_job("fp1", Path("/data"), total_files=10)
        # Same file fails twice with different errors
        tracker.record_error("fp1", Path("/data/a.pdf"), "ExtractionError", "err1")
        tracker.record_error("fp1", Path("/data/a.pdf"), "IndexingError", "err2")
        tracker.record_error("fp1", Path("/data/b.pdf"), "ExtractionError", "err3")

        paths = tracker.get_failed_paths("fp1")
        assert len(paths) == 2
        assert set(paths) == {Path("/data/a.pdf"), Path("/data/b.pdf")}

    def test_excludes_empty_extraction_by_default(self, tracker: JobTracker):
        tracker.create_job("fp2", Path("/data"), total_files=10)
        tracker.record_error("fp2", Path("/data/a.pdf"), "ExtractionError", "corrupt")
        tracker.record_error("fp2", Path("/data/b.pdf"), "EmptyExtraction", "no text")

        paths = tracker.get_failed_paths("fp2")
        assert paths == [Path("/data/a.pdf")]

    def test_include_empty_flag(self, tracker: JobTracker):
        tracker.create_job("fp3", Path("/data"), total_files=10)
        tracker.record_error("fp3", Path("/data/a.pdf"), "ExtractionError", "corrupt")
        tracker.record_error("fp3", Path("/data/b.pdf"), "EmptyExtraction", "no text")

        paths = tracker.get_failed_paths("fp3", include_empty=True)
        assert len(paths) == 2

    def test_only_failed(self, tracker: JobTracker):
        tracker.create_job("fp5", Path("/data"), total_files=10)
        tracker.record_error("fp5", Path("/data/a.pdf"), "ExtractionError", "corrupt")
        tracker.record_error("fp5", Path("/data/b.pdf"), "EmptyExtraction", "no text")

        paths = tracker.get_failed_paths("fp5", only="failed")
        assert paths == [Path("/data/a.pdf")]

    def test_only_empty(self, tracker: JobTracker):
        tracker.create_job("fp6", Path("/data"), total_files=10)
        tracker.record_error("fp6", Path("/data/a.pdf"), "ExtractionError", "corrupt")
        tracker.record_error("fp6", Path("/data/b.pdf"), "EmptyExtraction", "no text")

        paths = tracker.get_failed_paths("fp6", only="empty")
        assert paths == [Path("/data/b.pdf")]

    def test_empty_when_no_errors(self, tracker: JobTracker):
        tracker.create_job("fp4", Path("/data"), total_files=5)
        assert tracker.get_failed_paths("fp4") == []


class TestGetFailedDocIds:
    def test_returns_distinct_doc_ids(self, tracker: JobTracker):
        tracker.create_job("ed1", Path("."), total_files=10, job_type=JobType.EMBED)
        tracker.record_error("ed1", Path("abc123"), "EmbeddingError", "timeout")
        tracker.record_error("ed1", Path("def456"), "EmbeddingError", "oom")
        # Same doc fails twice
        tracker.record_error("ed1", Path("abc123"), "EmbeddingError", "retry failed")

        doc_ids = tracker.get_failed_doc_ids("ed1")
        assert len(doc_ids) == 2
        assert set(doc_ids) == {"abc123", "def456"}

    def test_empty_when_no_errors(self, tracker: JobTracker):
        tracker.create_job("ed2", Path("."), total_files=5, job_type=JobType.EMBED)
        assert tracker.get_failed_doc_ids("ed2") == []


class TestEmbeddingModelTracking:
    def test_get_returns_none_initially(self, tracker: JobTracker):
        assert tracker.get_embedding_model("myindex") is None

    def test_set_and_get(self, tracker: JobTracker):
        tracker.set_embedding_model("myindex", "snowflake-arctic-embed2", "ollama", 1024)
        result = tracker.get_embedding_model("myindex")
        assert result == ("snowflake-arctic-embed2", "ollama", 1024)

    def test_upsert_overwrites(self, tracker: JobTracker):
        tracker.set_embedding_model("myindex", "model-a", "ollama", 768)
        tracker.set_embedding_model("myindex", "model-b", "openai", 1024)
        result = tracker.get_embedding_model("myindex")
        assert result == ("model-b", "openai", 1024)

    def test_separate_indices(self, tracker: JobTracker):
        tracker.set_embedding_model("idx1", "model-a", "ollama", 768)
        tracker.set_embedding_model("idx2", "model-b", "openai", 1024)
        assert tracker.get_embedding_model("idx1") == ("model-a", "ollama", 768)
        assert tracker.get_embedding_model("idx2") == ("model-b", "openai", 1024)


# ---------------------------------------------------------------------------
# Pipeline integration tests (real tracker, mock extractor + backend)
# ---------------------------------------------------------------------------


def _stub_extractor(extract_fn):
    """Create a mock Extractor that delegates to extract_fn."""
    ext = MagicMock()
    ext.extract.side_effect = extract_fn
    ext.supports.return_value = True
    return ext


def _stub_backend():
    """Create a mock SearchBackend that always succeeds."""
    backend = MagicMock()
    backend.index_batch.return_value = []  # no failures
    return backend


class TestPipelineEmptyFiles:
    """Verify empty-file counting and error recording flow end-to-end
    through IngestPipeline with a real JobTracker."""

    def test_empty_file_counted_and_error_logged(self, tmp_path: Path, tracker: JobTracker) -> None:
        """A non-zero file that produces no text should:
        - have empty=1 in job stats
        - have an EmptyExtraction error in the tracker
        - still be indexed (1 document)
        """
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        (docs_dir / "scan.pdf").write_bytes(b"binary content, no text")

        def fake_extract(file_path: Path, record_error: RecordErrorFn | None = None) -> list[Document]:
            doc = Document(source_path=file_path, content="", metadata={"Content-Type": "application/pdf"})
            if record_error is not None:
                record_error(file_path, "EmptyExtraction", f"no text for {file_path}")
            return [doc]

        pipeline = IngestPipeline(
            extractor=_stub_extractor(fake_extract),
            search_backend=_stub_backend(),
            tracker=tracker,
            batch_size=10,
            max_workers=1,
        )
        job, _, _ = pipeline.run(docs_dir)

        # Reload job from DB to verify persisted state
        job = tracker.get_job(job.job_id, include_errors=True)
        assert job is not None
        assert job.empty == 1, f"expected empty=1, got empty={job.empty}"
        assert job.processed == 1
        assert len(job.errors) == 1
        assert job.errors[0].error_type == "EmptyExtraction"

    def test_multiple_empty_files(self, tmp_path: Path, tracker: JobTracker) -> None:
        """Multiple empty files should all be counted."""
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        for i in range(3):
            (docs_dir / f"empty_{i}.bin").write_bytes(b"x" * (i + 1))

        def fake_extract(file_path: Path, record_error: RecordErrorFn | None = None) -> list[Document]:
            doc = Document(source_path=file_path, content="", metadata={})
            if record_error is not None:
                record_error(file_path, "EmptyExtraction", "no text")
            return [doc]

        pipeline = IngestPipeline(
            extractor=_stub_extractor(fake_extract),
            search_backend=_stub_backend(),
            tracker=tracker,
            batch_size=10,
            max_workers=1,
        )
        job, _, _ = pipeline.run(docs_dir)

        job = tracker.get_job(job.job_id, include_errors=True)
        assert job is not None
        assert job.empty == 3
        assert job.processed == 3
        assert len(job.errors) == 3

    def test_nested_docs_all_indexed(self, tmp_path: Path, tracker: JobTracker) -> None:
        """A container with 2 embedded docs should index 3 documents total."""
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        (docs_dir / "email.eml").write_bytes(b"email")

        def fake_extract(file_path: Path, record_error: RecordErrorFn | None = None) -> list[Document]:
            return [
                Document(source_path=file_path, content="Email body", metadata={}),
                Document(
                    source_path=file_path, content="Attachment 1", metadata={"_aum_extracted_from": str(file_path)}
                ),
                Document(source_path=file_path, content="", metadata={"_aum_extracted_from": str(file_path)}),
            ]

        pipeline = IngestPipeline(
            extractor=_stub_extractor(fake_extract),
            search_backend=_stub_backend(),
            tracker=tracker,
            batch_size=10,
            max_workers=1,
        )
        job, _, _ = pipeline.run(docs_dir)

        job = tracker.get_job(job.job_id)
        assert job is not None
        assert job.processed == 3
        assert job.extracted == 2  # 3 docs - 1 container = 2 extracted


# ---------------------------------------------------------------------------
# Tracker — resume helpers
# ---------------------------------------------------------------------------


class TestFindResumableJob:
    def test_finds_most_recent_running_ingest(self, tracker: JobTracker) -> None:
        tracker.create_job("old", Path("/data"), total_files=10)
        tracker.complete_job("old", JobStatus.COMPLETED)
        tracker.create_job("stale", Path("/data"), total_files=20)
        # "stale" is still RUNNING

        found = tracker.find_resumable_job()
        assert found is not None
        assert found.job_id == "stale"

    def test_returns_none_when_no_running(self, tracker: JobTracker) -> None:
        tracker.create_job("done", Path("/data"), total_files=5)
        tracker.complete_job("done", JobStatus.COMPLETED)

        assert tracker.find_resumable_job() is None

    def test_filters_by_source_dir(self, tracker: JobTracker) -> None:
        tracker.create_job("a", Path("/dir_a"), total_files=10)
        tracker.create_job("b", Path("/dir_b"), total_files=10)

        found = tracker.find_resumable_job(source_dir=Path("/dir_a"))
        assert found is not None
        assert found.job_id == "a"

    def test_ignores_embed_jobs(self, tracker: JobTracker) -> None:
        tracker.create_job("emb", Path("/data"), total_files=10, job_type=JobType.EMBED)
        assert tracker.find_resumable_job() is None


class TestInterruptedStatus:
    def test_complete_as_interrupted(self, tracker: JobTracker) -> None:
        tracker.create_job("int1", Path("/data"), total_files=5)
        tracker.complete_job("int1", JobStatus.INTERRUPTED)

        job = tracker.get_job("int1")
        assert job is not None
        assert job.status == JobStatus.INTERRUPTED
        assert job.finished_at is not None

    def test_list_interrupted_jobs(self, tracker: JobTracker) -> None:
        tracker.create_job("a", Path("/data"), total_files=1)
        tracker.complete_job("a", JobStatus.INTERRUPTED)
        tracker.create_job("b", Path("/data"), total_files=1)
        tracker.complete_job("b", JobStatus.COMPLETED)

        interrupted = tracker.list_jobs(status=JobStatus.INTERRUPTED)
        assert len(interrupted) == 1
        assert interrupted[0].job_id == "a"


class TestSkippedProgress:
    def test_update_progress_with_skipped(self, tracker: JobTracker) -> None:
        tracker.create_job("skip1", Path("/data"), total_files=100)
        tracker.update_progress("skip1", extracted=20, processed=20, failed=0, skipped=80)

        job = tracker.get_job("skip1")
        assert job is not None
        assert job.skipped == 80
        assert job.processed == 20

    def test_skipped_defaults_to_zero(self, tracker: JobTracker) -> None:
        tracker.create_job("skip2", Path("/data"), total_files=10)
        tracker.update_progress("skip2", extracted=10, processed=10, failed=0)

        job = tracker.get_job("skip2")
        assert job is not None
        assert job.skipped == 0


# ---------------------------------------------------------------------------
# Pipeline — resume with skip-existing
# ---------------------------------------------------------------------------


class TestPipelineResume:
    def test_resume_skips_existing_documents(self, tmp_path: Path, tracker: JobTracker) -> None:
        """Files whose primary doc_id already exists in the backend should
        be skipped during resume, avoiding Tika extraction."""
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        # Create 5 files
        for i in range(5):
            (docs_dir / f"doc_{i}.txt").write_text(f"content {i}")

        extracted_files: list[Path] = []

        def fake_extract(file_path: Path, record_error=None) -> list[Document]:
            extracted_files.append(file_path)
            return [Document(source_path=file_path, content=f"text of {file_path.name}", metadata={})]

        from aum.ingest.pipeline import _file_doc_id

        # Pre-compute doc_ids for the first 3 files (they "already exist")
        existing_ids = set()
        for i in range(3):
            path = docs_dir / f"doc_{i}.txt"
            existing_ids.add(_file_doc_id(path, 0))

        backend = _stub_backend()
        backend.get_existing_doc_ids.side_effect = lambda ids: existing_ids & set(ids)

        pipeline = IngestPipeline(
            extractor=_stub_extractor(fake_extract),
            search_backend=backend,
            tracker=tracker,
            batch_size=50,
            max_workers=1,
        )
        job, _, _ = pipeline.run_resume(docs_dir, parent_job_id="old_job")

        job = tracker.get_job(job.job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        # Only 2 files should have been extracted (doc_3.txt and doc_4.txt)
        assert len(extracted_files) == 2
        assert job.skipped == 3
        assert job.processed == 2

    def test_resume_processes_all_when_none_exist(self, tmp_path: Path, tracker: JobTracker) -> None:
        """When no documents exist in the backend, all files should be processed."""
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        for i in range(3):
            (docs_dir / f"doc_{i}.txt").write_text(f"content {i}")

        def fake_extract(file_path: Path, record_error=None) -> list[Document]:
            return [Document(source_path=file_path, content="text", metadata={})]

        backend = _stub_backend()
        backend.get_existing_doc_ids.return_value = set()

        pipeline = IngestPipeline(
            extractor=_stub_extractor(fake_extract),
            search_backend=backend,
            tracker=tracker,
            batch_size=50,
            max_workers=1,
        )
        job, _, _ = pipeline.run_resume(docs_dir, parent_job_id="old_job")

        job = tracker.get_job(job.job_id)
        assert job is not None
        assert job.processed == 3
        assert job.skipped == 0

    def test_resume_fallback_on_check_failure(self, tmp_path: Path, tracker: JobTracker) -> None:
        """If the existence check fails, all files should be processed (safe fallback)."""
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        (docs_dir / "doc.txt").write_text("content")

        def fake_extract(file_path: Path, record_error=None) -> list[Document]:
            return [Document(source_path=file_path, content="text", metadata={})]

        backend = _stub_backend()
        backend.get_existing_doc_ids.side_effect = RuntimeError("network error")

        pipeline = IngestPipeline(
            extractor=_stub_extractor(fake_extract),
            search_backend=backend,
            tracker=tracker,
            batch_size=50,
            max_workers=1,
        )
        job, _, _ = pipeline.run_resume(docs_dir, parent_job_id="old_job")

        job = tracker.get_job(job.job_id)
        assert job is not None
        assert job.processed == 1
        assert job.skipped == 0
