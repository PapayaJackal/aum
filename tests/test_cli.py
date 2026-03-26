from click.testing import CliRunner

from aum.cli import main


def test_version():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "0.2.0" in result.output


def test_config_command(monkeypatch, tmp_path):
    monkeypatch.setenv("AUM_DATA_DIR", str(tmp_path))
    runner = CliRunner()
    result = runner.invoke(main, ["config"])
    assert result.exit_code == 0
    assert "search_backend:" in result.output
    assert "ocr_enabled:" in result.output


def test_jobs_empty(monkeypatch, tmp_path):
    monkeypatch.setenv("AUM_DATA_DIR", str(tmp_path))
    runner = CliRunner()
    result = runner.invoke(main, ["jobs"])
    assert result.exit_code == 0
    assert "No jobs found" in result.output


def test_job_not_found(monkeypatch, tmp_path):
    monkeypatch.setenv("AUM_DATA_DIR", str(tmp_path))
    runner = CliRunner()
    result = runner.invoke(main, ["job", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_retry_not_found(monkeypatch, tmp_path):
    monkeypatch.setenv("AUM_DATA_DIR", str(tmp_path))
    runner = CliRunner()
    result = runner.invoke(main, ["retry", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_retry_no_failures_or_empty(monkeypatch, tmp_path):
    monkeypatch.setenv("AUM_DATA_DIR", str(tmp_path))

    from aum.ingest.tracker import JobTracker
    from aum.models import JobStatus

    tracker = JobTracker(db_path=str(tmp_path / "aum.db"))
    tracker.create_job("ok1", tmp_path, total_files=5)
    tracker.update_progress("ok1", extracted=5, processed=5, failed=0, empty=0)
    tracker.complete_job("ok1", JobStatus.COMPLETED)

    runner = CliRunner()
    result = runner.invoke(main, ["retry", "ok1"])
    assert result.exit_code == 0
    assert "no failed or empty items" in result.output


def test_user_list_empty(monkeypatch, tmp_path):
    monkeypatch.setenv("AUM_DATA_DIR", str(tmp_path))
    runner = CliRunner()
    result = runner.invoke(main, ["user", "list"])
    assert result.exit_code == 0
    assert "No users found" in result.output
