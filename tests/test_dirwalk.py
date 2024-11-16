from pathlib import Path

from aum.dirwalk import dirwalk

TEST_DIRECTORY = Path(__file__).parent / "data"


def test_dirwalk():
    files = dirwalk(TEST_DIRECTORY)

    expected_files = {
        "test.docx",
        "test.pdf",
        "test/test.txt",
    }

    assert set(files) == expected_files
