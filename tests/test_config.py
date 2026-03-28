from aum.config import AumConfig


def test_config_override(monkeypatch):
    monkeypatch.setenv("AUM_SEARCH_BACKEND", "elasticsearch")
    monkeypatch.setenv("AUM_OCR_ENABLED", "false")
    monkeypatch.setenv("AUM_OCR_LANGUAGE", "deu")
    config = AumConfig()
    assert config.search_backend == "elasticsearch"
    assert config.ocr_enabled is False
    assert config.ocr_language == "deu"
