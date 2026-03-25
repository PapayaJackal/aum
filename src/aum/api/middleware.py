from __future__ import annotations

import time

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from aum.metrics import SEARCH_LATENCY, SEARCH_REQUESTS


class MetricsMiddleware(BaseHTTPMiddleware):
    """Track request metrics for search endpoints."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path.startswith("/api/search"):
            search_type = request.query_params.get("type", "text")
            SEARCH_REQUESTS.labels(type=search_type).inc()
            start = time.monotonic()
            response = await call_next(request)
            elapsed = time.monotonic() - start
            backend = request.app.state.config.search_backend if hasattr(request.app.state, "config") else "unknown"
            SEARCH_LATENCY.labels(type=search_type, backend=backend).observe(elapsed)
            return response

        return await call_next(request)
