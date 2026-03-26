from pathlib import Path

from aum.ingest.tracker import JobTracker
from aum.models import JobStatus


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


class TestEmbeddingModelTracking:
    def test_get_returns_none_initially(self, tracker: JobTracker):
        assert tracker.get_embedding_model("myindex") is None

    def test_set_and_get(self, tracker: JobTracker):
        tracker.set_embedding_model("myindex", "snowflake-arctic-embed2", 1024)
        result = tracker.get_embedding_model("myindex")
        assert result == ("snowflake-arctic-embed2", 1024)

    def test_upsert_overwrites(self, tracker: JobTracker):
        tracker.set_embedding_model("myindex", "model-a", 768)
        tracker.set_embedding_model("myindex", "model-b", 1024)
        result = tracker.get_embedding_model("myindex")
        assert result == ("model-b", 1024)

    def test_separate_indices(self, tracker: JobTracker):
        tracker.set_embedding_model("idx1", "model-a", 768)
        tracker.set_embedding_model("idx2", "model-b", 1024)
        assert tracker.get_embedding_model("idx1") == ("model-a", 768)
        assert tracker.get_embedding_model("idx2") == ("model-b", 1024)
