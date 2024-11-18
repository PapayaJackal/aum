from pathlib import Path

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

SEARCH_ENGINE = None
SEARCH_INDEX = None
STATIC_DIR = Path(__file__).parent / "public"


async def search(request):
    query = request.query_params.get("q")
    if not query:
        return JSONResponse(
            {"error": 'Query parameter "q" is required.'}, status_code=400
        )
    return JSONResponse(SEARCH_ENGINE.search(SEARCH_INDEX, query))


def app_init(search_engine, index_name, debug=True):
    # pylint: disable=global-statement
    global SEARCH_ENGINE, SEARCH_INDEX
    SEARCH_ENGINE = search_engine
    SEARCH_INDEX = index_name
    return Starlette(
        debug=debug,
        routes=[
            Route("/search", search),
            Mount("/", app=StaticFiles(directory=STATIC_DIR, html=True)),
        ],
    )
