import atexit
import random
import shutil
import subprocess

from tika_client import TikaClient

from . import TextExtractor

RANDOM_PORT = random.randint(1024, 65535)
TIKA_PROCESSES = {}


def __tika_cleanup():
    for _, process in TIKA_PROCESSES.items():
        process.kill()


atexit.register(__tika_cleanup)


def __tika_resolve():
    return shutil.which("tika-server")


def tika_start_singleton(tika_host, tika_port):
    if f"{tika_host}:{tika_port}" in TIKA_PROCESSES:
        return TIKA_PROCESSES[f"{tika_host}:{tika_port}"]

    # pylint: disable=R1732
    tika_process = subprocess.Popen(
        [__tika_resolve(), "-h", tika_host, "-p", str(tika_port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    while True:
        output = tika_process.stderr.readline().decode("utf-8")
        if "Started Apache Tika server" in output:
            break

    TIKA_PROCESSES[f"{tika_host}:{tika_port}"] = tika_process
    return tika_process


class TikaTextExtractor(TextExtractor):

    def __init__(self, tika_url=None):
        if tika_url is None:
            tika_start_singleton("127.0.0.1", RANDOM_PORT)
            tika_url = f"http://127.0.0.1:{RANDOM_PORT}"
        self.tika_client = TikaClient(tika_url=tika_url)

    def extract_text(self, document_path):
        resp = self.tika_client.tika.as_text.from_file(document_path)

        metadata = {}
        for k, v in resp.data.items():
            if not k.startswith("X-"):
                metadata[k] = v

        return (metadata, resp.content)
