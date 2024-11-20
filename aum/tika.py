import atexit
import random
import shutil
import subprocess
import time

from prometheus_client import Counter, Gauge, Histogram
from tika_client import TikaClient

from . import TextExtractor

RANDOM_PORT = random.randint(1024, 65535)
TIKA_PROCESSES = {}

EXTRACTION_REQUEST_COUNTER = Counter(
    "tika_extraction_request_total",
    "Total number of text extraction requests",
    ["index_name", "tika_url"],
)
EXTRACTION_CONTENT_TYPE_COUNTER = Counter(
    "tika_extraction_content_type_total",
    "Total number of text extraction responses per content type",
    ["index_name", "tika_url", "content_type"],
)
EXTRACTION_DURATION_HISTOGRAM = Histogram(
    "tika_extraction_duration_seconds",
    "Duration of text extraction requests in seconds",
    ["index_name", "tika_url"],
)
EXTRACTION_IN_FLIGHT_GAUGE = Gauge(
    "tika_extraction_in_flight",
    "Current number of text extractions in flight",
    ["index_name", "tika_url"],
)
EXTRACTION_ERROR_COUNTER = Counter(
    "tika_extraction_error_total",
    "Total number of text extraction errors",
    ["index_name", "tika_url", "exception_type"],
)


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

    def __init__(self, index_name, tika_url=None):
        self.index_name = index_name
        self.prometheus_tika_url = tika_url or "localhost"
        if tika_url is None:
            tika_start_singleton("127.0.0.1", RANDOM_PORT)
            tika_url = f"http://127.0.0.1:{RANDOM_PORT}"
        self.tika_client = TikaClient(tika_url=tika_url)

    def extract_text(self, document_path):
        EXTRACTION_REQUEST_COUNTER.labels(
            index_name=self.index_name, tika_url=self.prometheus_tika_url
        ).inc()
        EXTRACTION_IN_FLIGHT_GAUGE.labels(
            index_name=self.index_name, tika_url=self.prometheus_tika_url
        ).inc()
        start_time = time.time()

        try:
            resp = self.tika_client.tika.as_text.from_file(document_path)

            metadata = {}
            for k, v in resp.data.items():
                if not k.startswith("X-"):
                    metadata[k] = v
            EXTRACTION_CONTENT_TYPE_COUNTER.labels(
                index_name=self.index_name,
                tika_url=self.prometheus_tika_url,
                content_type=metadata["Content-Type"],
            ).inc()

            return (metadata, resp.content)
        except Exception as e:
            EXTRACTION_ERROR_COUNTER.labels(
                index_name=self.index_name,
                tika_url=self.prometheus_tika_url,
                exception_type=type(e).__name__,
            ).inc()
            raise e from None
        finally:
            end_time = time.time()
            EXTRACTION_DURATION_HISTOGRAM.labels(
                index_name=self.index_name, tika_url=self.prometheus_tika_url
            ).observe(end_time - start_time)
            EXTRACTION_IN_FLIGHT_GAUGE.labels(
                index_name=self.index_name, tika_url=self.prometheus_tika_url
            ).dec()
