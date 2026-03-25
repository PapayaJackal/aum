from __future__ import annotations

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

log = structlog.get_logger()
router = APIRouter(prefix="/api/auth", tags=["auth"])


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
    credentials: LoginRequest,
    auth: Annotated[LocalAuth, Depends(get_local_auth)],
    tokens: Annotated[TokenManager, Depends(get_token_manager)],
) -> TokenResponse:
    try:
        user = auth.authenticate(credentials.username, credentials.password)
    except AuthError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

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
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    user = auth.get_user(int(payload["sub"]))
    if user is None:
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

    token = await client.authorize_access_token(request)
    userinfo = token.get("userinfo")
    if userinfo is None:
        userinfo = await client.userinfo(token=token)

    user = oauth.get_or_create_user(provider, dict(userinfo))

    return TokenResponse(
        access_token=tokens.create_access_token(user),
        refresh_token=tokens.create_refresh_token(user),
    )
