from pathlib import Path


def dirwalk(directory):
    path = Path(directory)

    for file in path.rglob("*"):
        if file.is_file():
            yield str(file.relative_to(directory))
