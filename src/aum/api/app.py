from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

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
        docs_url="/api/docs" if cfg.enable_docs else None,
        openapi_url="/api/openapi.json" if cfg.enable_docs else None,
    )
    app.state.config = cfg

    # CORS — only enabled when explicit origins are configured
    if cfg.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cfg.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Session middleware — required by authlib for OAuth CSRF state
    if cfg.oauth_providers:
        app.add_middleware(SessionMiddleware, secret_key=cfg.jwt_secret)

    # Metrics middleware
    app.add_middleware(MetricsMiddleware)

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
