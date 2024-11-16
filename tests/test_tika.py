from pathlib import Path

from aum.tika import TikaTextExtractor

DATA_DIR = Path(__file__).parent / "data"


def test_find_test_data():
    test_pdf = DATA_DIR / "test.pdf"
    test_docx = DATA_DIR / "test.docx"

    assert test_pdf.exists()
    assert test_docx.exists()


def test_extract_pdf():
    e = TikaTextExtractor()
    resp = e.extract_text(DATA_DIR / "test.pdf")

    assert resp is not None
    assert resp[0]["Content-Type"] == "application/pdf"
    assert (
        "Would it save you a lot of time if I just gave up and went mad now?" in resp[1]
    )


def test_extract_docx():
    e = TikaTextExtractor()
    resp = e.extract_text(DATA_DIR / "test.docx")

    assert resp is not None
    assert (
        resp[0]["Content-Type"]
        == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    assert (
        "Would it save you a lot of time if I just gave up and went mad now?" in resp[1]
    )
