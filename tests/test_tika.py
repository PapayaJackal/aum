from pathlib import Path

from prometheus_client import REGISTRY

from aum.tika import TikaTextExtractor

DATA_DIR = Path(__file__).parent / "data"


def test_find_test_data():
    test_pdf = DATA_DIR / "test.pdf"
    test_docx = DATA_DIR / "test.docx"

    assert test_pdf.exists()
    assert test_docx.exists()


def test_extract_pdf():
    e = TikaTextExtractor("test_index")
    request_counter_before = (
        REGISTRY.get_sample_value(
            "tika_extraction_request_total",
            {"tika_url": "localhost", "index_name": "test_index"},
        )
        or 0
    )
    pdf_counter_before = (
        REGISTRY.get_sample_value(
            "tika_extraction_content_type_total",
            {
                "tika_url": "localhost",
                "index_name": "test_index",
                "content_type": "application/pdf",
            },
        )
        or 0
    )

    resp = e.extract_text(DATA_DIR / "test.pdf")

    request_counter_after = REGISTRY.get_sample_value(
        "tika_extraction_request_total",
        {"tika_url": "localhost", "index_name": "test_index"},
    )
    pdf_counter_after = REGISTRY.get_sample_value(
        "tika_extraction_content_type_total",
        {
            "tika_url": "localhost",
            "index_name": "test_index",
            "content_type": "application/pdf",
        },
    )
    assert resp is not None
    assert resp[0]["Content-Type"] == "application/pdf"
    assert (
        "Would it save you a lot of time if I just gave up and went mad now?" in resp[1]
    )
    assert (request_counter_after - request_counter_before) == 1.0
    assert (pdf_counter_after - pdf_counter_before) == 1.0


def test_extract_docx():
    e = TikaTextExtractor("test_index")
    request_counter_before = (
        REGISTRY.get_sample_value(
            "tika_extraction_request_total",
            {"tika_url": "localhost", "index_name": "test_index"},
        )
        or 0
    )
    docx_counter_before = (
        REGISTRY.get_sample_value(
            "tika_extraction_content_type_total",
            {
                "tika_url": "localhost",
                "index_name": "test_index",
                "content_type": "application/vnd.openxmlformats"
                "-officedocument.wordprocessingml.document",
            },
        )
        or 0
    )
    resp = e.extract_text(DATA_DIR / "test.docx")

    request_counter_after = REGISTRY.get_sample_value(
        "tika_extraction_request_total",
        {"tika_url": "localhost", "index_name": "test_index"},
    )
    docx_counter_after = REGISTRY.get_sample_value(
        "tika_extraction_content_type_total",
        {
            "tika_url": "localhost",
            "index_name": "test_index",
            "content_type": "application/vnd.openxmlformats"
            "-officedocument.wordprocessingml.document",
        },
    )
    assert resp is not None
    assert (
        resp[0]["Content-Type"]
        == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    assert (
        "Would it save you a lot of time if I just gave up and went mad now?" in resp[1]
    )
    assert (request_counter_after - request_counter_before) == 1.0
    assert (docx_counter_after - docx_counter_before) == 1.0
