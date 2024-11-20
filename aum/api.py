from pathlib import Path

from prometheus_client import Counter, Gauge, Histogram, generate_latest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Match, Mount, Route
from starlette.staticfiles import StaticFiles

REQUEST_COUNT = Counter(
    "http_request_total",
    "Requests",
    ["method", "endpoint"],
)
REQUEST_LATENCY = Histogram(
    "http_request_latency_bucket",
    "Request Latency",
    ["method", "endpoint"],
)
EXCEPTION_COUNT = Counter(
    "http_exception_total",
    "Exceptions Raised",
    ["method", "endpoint", "exception_type"],
)
REQUEST_IN_PROGRESS_GAUGE = Gauge(
    "http_request_in_progress_count",
    "Requests In-Flight",
    ["method", "endpoint"],
)
SEARCH_QUERY_COUNT = Counter("aum_search_query_total", "Search Queries", ["index_name"])

SEARCH_ENGINE = None
SEARCH_INDEX = None
STATIC_DIR = Path(__file__).parent / "public"


class PrometheusMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            method = scope["method"]
            path, is_handled_path = self.get_path_template(scope)

            if not is_handled_path:
                return await self.app(scope, receive, send)

            REQUEST_COUNT.labels(method=method, endpoint=path).inc()
            REQUEST_IN_PROGRESS_GAUGE.labels(method=method, endpoint=path).inc()

            with REQUEST_LATENCY.labels(method=method, endpoint=path).time():
                try:
                    await self.app(scope, receive, send)
                # pylint: disable=broad-exception-caught
                except Exception as e:
                    EXCEPTION_COUNT.labels(
                        method=method, endpoint=path, exception_type=type(e).__name__
                    ).inc()
                    raise e from None
                finally:
                    REQUEST_IN_PROGRESS_GAUGE.labels(method=method, endpoint=path).dec()
        else:
            await self.app(scope, receive, send)

    @staticmethod
    def get_path_template(scope):
        for route in scope["app"].routes:
            match, _ = route.matches(scope)
            if match == Match.FULL:
                return route.path, True
        return scope["path"], None


async def search(request):
    query = request.query_params.get("q")
    if not query:
        return JSONResponse(
            {"error": 'Query parameter "q" is required.'}, status_code=400
        )
    SEARCH_QUERY_COUNT.labels(index_name=SEARCH_INDEX).inc()
    return JSONResponse(SEARCH_ENGINE.search(SEARCH_INDEX, query))


async def metrics(_):
    return PlainTextResponse(generate_latest())


def app_init(search_engine, index_name, debug=True):
    # pylint: disable=global-statement
    global SEARCH_ENGINE, SEARCH_INDEX
    SEARCH_ENGINE = search_engine
    SEARCH_INDEX = index_name
    return Starlette(
        debug=debug,
        routes=[
            Route("/search", search),
            Route("/metrics", metrics),
            Mount("/", app=StaticFiles(directory=STATIC_DIR, html=True)),
        ],
        middleware=[Middleware(PrometheusMiddleware)],
    )
