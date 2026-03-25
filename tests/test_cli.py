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


def test_user_list_empty(monkeypatch, tmp_path):
    monkeypatch.setenv("AUM_DATA_DIR", str(tmp_path))
    runner = CliRunner()
    result = runner.invoke(main, ["user", "list"])
    assert result.exit_code == 0
    assert "No users found" in result.output
