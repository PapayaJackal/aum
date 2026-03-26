from __future__ import annotations

import ipaddress
import time
from collections import defaultdict
from threading import Lock
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from aum.api.deps import (
    get_local_auth,
    get_oauth_manager,
    get_token_manager,
)
from aum.auth.local import AuthError, LocalAuth
from aum.auth.oauth import OAuthManager
from aum.auth.tokens import TokenError, TokenManager
from aum.metrics import AUTH_RATE_LIMITED

log = structlog.get_logger()
router = APIRouter(prefix="/api/auth", tags=["auth"])

# ---------------------------------------------------------------------------
# Login rate limiter — per-IP sliding window
# ---------------------------------------------------------------------------

_MAX_FAILURES = 5
_WINDOW_SECONDS = 900  # 15 minutes

_login_failures: dict[str, list[float]] = defaultdict(list)
_login_lock = Lock()


def _rate_limit_key(client_ip: str) -> str:
    """Return the rate-limit bucket key for an IP address.

    IPv4 addresses are keyed individually.  IPv6 addresses are keyed by
    their /64 prefix since a single host typically owns the entire /64.
    """
    try:
        addr = ipaddress.ip_address(client_ip)
    except ValueError:
        return client_ip
    if isinstance(addr, ipaddress.IPv6Address):
        return str(ipaddress.IPv6Network((addr, 64), strict=False))
    return client_ip


_CLEANUP_THRESHOLD = 1000


def _check_rate_limit(client_ip: str) -> None:
    """Raise 429 if the IP has exceeded the failed-login threshold."""
    key = _rate_limit_key(client_ip)
    now = time.monotonic()
    cutoff = now - _WINDOW_SECONDS

    with _login_lock:
        # Periodic sweep of stale keys to prevent unbounded memory growth
        if len(_login_failures) > _CLEANUP_THRESHOLD:
            stale = [k for k, v in _login_failures.items() if not v or v[-1] <= cutoff]
            for k in stale:
                del _login_failures[k]

        timestamps = _login_failures[key]
        # Prune old entries for this key
        _login_failures[key] = [t for t in timestamps if t > cutoff]
        if not _login_failures[key]:
            del _login_failures[key]
        elif len(_login_failures[key]) >= _MAX_FAILURES:
            AUTH_RATE_LIMITED.inc()
            log.warning("login rate limited", client_ip=client_ip, rate_key=key)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many failed login attempts. Try again later.",
            )


def _record_failure(client_ip: str) -> None:
    key = _rate_limit_key(client_ip)
    with _login_lock:
        _login_failures[key].append(time.monotonic())


# ---------------------------------------------------------------------------


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    refresh_token: str


class ProvidersResponse(BaseModel):
    providers: list[str]


@router.post("/login", response_model=TokenResponse)
async def login(
    request: Request,
    credentials: LoginRequest,
    auth: Annotated[LocalAuth, Depends(get_local_auth)],
    tokens: Annotated[TokenManager, Depends(get_token_manager)],
) -> TokenResponse:
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)

    try:
        user = auth.authenticate(credentials.username, credentials.password)
    except AuthError as exc:
        _record_failure(client_ip)
        log.warning("login failed", username=credentials.username, client_ip=client_ip)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    log.info("login successful", username=user.username, is_admin=user.is_admin)
    return TokenResponse(
        access_token=tokens.create_access_token(user),
        refresh_token=tokens.create_refresh_token(user),
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    data: RefreshRequest,
    tokens: Annotated[TokenManager, Depends(get_token_manager)],
    auth: Annotated[LocalAuth, Depends(get_local_auth)],
) -> TokenResponse:
    try:
        payload = tokens.verify_refresh_token(data.refresh_token)
    except TokenError as exc:
        log.warning("token refresh failed", error=str(exc))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    user = auth.get_user(int(payload["sub"]))
    if user is None:
        log.warning("token refresh for deleted user", user_id=payload["sub"])
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    return TokenResponse(
        access_token=tokens.create_access_token(user),
        refresh_token=tokens.create_refresh_token(user),
    )


@router.get("/providers", response_model=ProvidersResponse)
async def list_providers(
    oauth: Annotated[OAuthManager | None, Depends(get_oauth_manager)],
) -> ProvidersResponse:
    if oauth is None:
        return ProvidersResponse(providers=[])
    return ProvidersResponse(providers=oauth.provider_names)


@router.get("/oauth/{provider}/authorize")
async def oauth_authorize(provider: str, request: Request):  # noqa: ANN201
    oauth = get_oauth_manager()
    if oauth is None:
        raise HTTPException(status_code=404, detail="OAuth not configured")

    try:
        client = oauth.get_client(provider)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Unknown provider: {provider}")

    redirect_uri = str(request.url_for("oauth_callback", provider=provider))
    return await client.authorize_redirect(request, redirect_uri)


@router.get("/oauth/{provider}/callback")
async def oauth_callback(
    provider: str,
    request: Request,
    tokens: Annotated[TokenManager, Depends(get_token_manager)],
) -> TokenResponse:
    oauth = get_oauth_manager()
    if oauth is None:
        raise HTTPException(status_code=404, detail="OAuth not configured")

    try:
        client = oauth.get_client(provider)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Unknown provider: {provider}")

    try:
        token = await client.authorize_access_token(request)
    except Exception as exc:
        log.warning("oauth callback failed", provider=provider, error=str(exc))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="OAuth authentication failed")
    userinfo = token.get("userinfo")
    if userinfo is None:
        userinfo = await client.userinfo(token=token)

    user = oauth.get_or_create_user(provider, dict(userinfo))
    log.info("oauth login successful", provider=provider, username=user.username)

    return TokenResponse(
        access_token=tokens.create_access_token(user),
        refresh_token=tokens.create_refresh_token(user),
    )
