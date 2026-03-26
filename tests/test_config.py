from aum.config import AumConfig


def test_default_config():
    config = AumConfig()
    assert config.search_backend == "elasticsearch"
    assert config.tika_server_url == "http://localhost:9998"
    assert config.ocr_enabled is False
    assert config.ocr_language == "eng"
    assert config.embeddings_enabled is False
    assert config.log_level == "INFO"
    assert config.ingest_batch_size == 50


def test_config_override(monkeypatch):
    monkeypatch.setenv("AUM_SEARCH_BACKEND", "elasticsearch")
    monkeypatch.setenv("AUM_OCR_ENABLED", "false")
    monkeypatch.setenv("AUM_OCR_LANGUAGE", "deu")
    config = AumConfig()
    assert config.search_backend == "elasticsearch"
    assert config.ocr_enabled is False
    assert config.ocr_language == "deu"
