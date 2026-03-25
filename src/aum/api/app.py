from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from prometheus_client import make_asgi_app

from aum import __version__
from aum.api.middleware import MetricsMiddleware
from aum.api.routes import auth, jobs, search
from aum.config import AumConfig
from aum.metrics import BUILD_INFO


def create_app(config: AumConfig | None = None) -> FastAPI:
    cfg = config or AumConfig()

    app = FastAPI(
        title="aum",
        version=__version__,
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )
    app.state.config = cfg

    # CORS for SPA dev server
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173"],  # Vite dev server
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Metrics middleware
    app.add_middleware(MetricsMiddleware)

    # Prometheus metrics endpoint
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    # API routes
    app.include_router(auth.router)
    app.include_router(search.router)
    app.include_router(jobs.router)

    # Serve SPA static files in production
    frontend_dist = Path(__file__).parent.parent.parent.parent / "frontend" / "dist"
    if frontend_dist.is_dir():
        app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="spa")

    BUILD_INFO.info({"version": __version__})

    return app
