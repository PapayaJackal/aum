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
    get_config,
    get_current_user,
    get_local_auth,
    get_oauth_manager,
    get_token_manager,
    get_webauthn_manager,
)
from aum.auth.local import AuthError, LocalAuth, PasswordPolicyError
from aum.auth.models import User
from aum.auth.oauth import OAuthManager
from aum.auth.tokens import TokenError, TokenManager
from aum.auth.webauthn import WebAuthnError, WebAuthnManager
from aum.config import AumConfig
from aum.metrics import AUTH_RATE_LIMITED, PASSKEY_ENROLLMENTS, PASSKEY_LOGINS

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


class PasskeyEnrollmentRequired(BaseModel):
    passkey_enrollment_required: bool = True
    enrollment_token: str


class PasskeyLoginBeginResponse(BaseModel):
    options: str  # JSON string of WebAuthn authentication options
    session_id: str


class PasskeyLoginCompleteRequest(BaseModel):
    session_id: str
    credential: dict


class PasskeyEnrollBeginRequest(BaseModel):
    enrollment_token: str


class PasskeyEnrollCompleteRequest(BaseModel):
    enrollment_token: str
    credential: dict


class InviteValidationResponse(BaseModel):
    username: str
    valid: bool = True


class RedeemInviteRequest(BaseModel):
    password: str | None = None
    passkey_credential: dict | None = None


class ProvidersResponse(BaseModel):
    providers: list[str]
    passkey_required: bool = False
    passkey_login_enabled: bool = True
    public_mode: bool = False


@router.post("/login")
async def login(
    request: Request,
    credentials: LoginRequest,
    auth: Annotated[LocalAuth, Depends(get_local_auth)],
    tokens: Annotated[TokenManager, Depends(get_token_manager)],
    webauthn_mgr: Annotated[WebAuthnManager, Depends(get_webauthn_manager)],
    config: Annotated[AumConfig, Depends(get_config)],
) -> TokenResponse | PasskeyEnrollmentRequired:
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)

    try:
        user = auth.authenticate(credentials.username, credentials.password)
    except AuthError as exc:
        _record_failure(client_ip)
        log.warning("login failed", username=credentials.username, client_ip=client_ip)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    log.info("password login successful", username=user.username, is_admin=user.is_admin)

    # If passkey_required and user has no passkeys yet, force enrollment
    if config.passkey_required and not webauthn_mgr.has_credentials(user.id):
        enrollment_token = tokens.create_passkey_enroll_token(user)
        log.info("passkey enrollment required", username=user.username)
        return PasskeyEnrollmentRequired(enrollment_token=enrollment_token)

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
    config: Annotated[AumConfig, Depends(get_config)],
) -> ProvidersResponse:
    providers = oauth.provider_names if oauth else []
    return ProvidersResponse(
        providers=providers,
        passkey_required=config.passkey_required,
        passkey_login_enabled=config.passkey_enabled,
        public_mode=config.public_mode,
    )


# ---------------------------------------------------------------------------
# Passkey login (primary auth — no password required)
# ---------------------------------------------------------------------------


@router.post("/passkey/begin", response_model=PasskeyLoginBeginResponse)
async def passkey_login_begin(
    config: Annotated[AumConfig, Depends(get_config)],
    webauthn_mgr: Annotated[WebAuthnManager, Depends(get_webauthn_manager)],
) -> PasskeyLoginBeginResponse:
    if not config.passkey_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Passkey login is not enabled")
    options_json, session_id = webauthn_mgr.generate_discoverable_authentication_options()
    return PasskeyLoginBeginResponse(options=options_json, session_id=session_id)


@router.post("/passkey/complete", response_model=TokenResponse)
async def passkey_login_complete(
    request: Request,
    data: PasskeyLoginCompleteRequest,
    tokens: Annotated[TokenManager, Depends(get_token_manager)],
    webauthn_mgr: Annotated[WebAuthnManager, Depends(get_webauthn_manager)],
) -> TokenResponse:
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)

    challenge = webauthn_mgr.pop_passkey_session(data.session_id)
    if challenge is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Passkey session expired or invalid",
        )

    try:
        user, _cred = webauthn_mgr.verify_discoverable_authentication(challenge, data.credential)
    except WebAuthnError as exc:
        _record_failure(client_ip)
        PASSKEY_LOGINS.labels(result="failure").inc()
        log.warning("passkey login failed", client_ip=client_ip, error=str(exc))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    PASSKEY_LOGINS.labels(result="success").inc()
    log.info("passkey login successful", username=user.username, is_admin=user.is_admin)
    return TokenResponse(
        access_token=tokens.create_access_token(user),
        refresh_token=tokens.create_refresh_token(user),
    )


# ---------------------------------------------------------------------------
# WebAuthn passkey registration (for already-authenticated users)
# ---------------------------------------------------------------------------


@router.post("/webauthn/register/begin")
async def webauthn_register_begin(
    user: Annotated[User, Depends(get_current_user)],
    webauthn_mgr: Annotated[WebAuthnManager, Depends(get_webauthn_manager)],
) -> dict:
    options_json, _challenge = webauthn_mgr.generate_registration_options(user)
    return {"options": options_json}


@router.post("/webauthn/register/complete")
async def webauthn_register_complete(
    request: Request,
    user: Annotated[User, Depends(get_current_user)],
    webauthn_mgr: Annotated[WebAuthnManager, Depends(get_webauthn_manager)],
) -> dict:
    body = await request.json()
    credential = body.get("credential")
    if not credential:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing credential")

    challenge = webauthn_mgr._pop_challenge(user.id)
    if challenge is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Registration challenge expired or missing")

    try:
        webauthn_mgr.verify_registration(user, challenge, credential)
    except WebAuthnError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    return {"registered": True}


# ---------------------------------------------------------------------------
# Forced passkey enrollment (for users without passkeys when passkey_required=true)
# ---------------------------------------------------------------------------


@router.post("/passkey/enroll/begin")
async def passkey_enroll_begin(
    data: PasskeyEnrollBeginRequest,
    tokens: Annotated[TokenManager, Depends(get_token_manager)],
    auth: Annotated[LocalAuth, Depends(get_local_auth)],
    webauthn_mgr: Annotated[WebAuthnManager, Depends(get_webauthn_manager)],
) -> dict:
    try:
        payload = tokens.verify_passkey_enroll_token(data.enrollment_token)
    except TokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    user_id = int(payload["sub"])
    user = auth.get_user(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    options_json, _challenge = webauthn_mgr.generate_registration_options(user)
    return {"options": options_json}


@router.post("/passkey/enroll/complete", response_model=TokenResponse)
async def passkey_enroll_complete(
    data: PasskeyEnrollCompleteRequest,
    tokens: Annotated[TokenManager, Depends(get_token_manager)],
    auth: Annotated[LocalAuth, Depends(get_local_auth)],
    webauthn_mgr: Annotated[WebAuthnManager, Depends(get_webauthn_manager)],
) -> TokenResponse:
    try:
        payload = tokens.verify_passkey_enroll_token(data.enrollment_token)
    except TokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    user_id = int(payload["sub"])
    user = auth.get_user(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    challenge = webauthn_mgr._pop_challenge(user.id)
    if challenge is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Enrollment challenge expired or missing")

    try:
        webauthn_mgr.verify_registration(user, challenge, data.credential)
    except WebAuthnError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    PASSKEY_ENROLLMENTS.inc()
    log.info("passkey enrollment completed", username=user.username)
    return TokenResponse(
        access_token=tokens.create_access_token(user),
        refresh_token=tokens.create_refresh_token(user),
    )


# ---------------------------------------------------------------------------
# User invitations
# ---------------------------------------------------------------------------


@router.get("/invite/{token}", response_model=InviteValidationResponse)
async def validate_invite(
    token: str,
    auth: Annotated[LocalAuth, Depends(get_local_auth)],
) -> InviteValidationResponse:
    invitation = auth.get_invitation(token)
    if invitation is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid or expired invitation")
    return InviteValidationResponse(username=invitation.username)


@router.post("/invite/{token}/webauthn/begin")
async def invite_webauthn_begin(
    token: str,
    auth: Annotated[LocalAuth, Depends(get_local_auth)],
    config: Annotated[AumConfig, Depends(get_config)],
    webauthn_mgr: Annotated[WebAuthnManager, Depends(get_webauthn_manager)],
) -> dict:
    if not config.passkey_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Passkey registration is not enabled")
    invitation = auth.get_invitation(token)
    if invitation is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid or expired invitation")

    # Create a temporary User object for registration options generation
    temp_user = User(id=0, username=invitation.username, password_hash=None, is_admin=invitation.is_admin)
    options_json, _challenge = webauthn_mgr.generate_registration_options(temp_user)
    # Store challenge keyed by a negative hash of the token to avoid collision with real user IDs
    webauthn_mgr._store_challenge(-hash(token), _challenge)
    return {"options": options_json}


@router.post("/invite/{token}/redeem")
async def redeem_invite(
    token: str,
    data: RedeemInviteRequest,
    auth: Annotated[LocalAuth, Depends(get_local_auth)],
    tokens: Annotated[TokenManager, Depends(get_token_manager)],
    webauthn_mgr: Annotated[WebAuthnManager, Depends(get_webauthn_manager)],
) -> TokenResponse:
    if not data.password and not data.passkey_credential:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Must provide a password and/or passkey credential",
        )

    try:
        user = auth.redeem_invitation(token, password=data.password)
    except (AuthError, PasswordPolicyError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    if data.passkey_credential:
        challenge = webauthn_mgr._pop_challenge(-hash(token))
        if challenge is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Passkey challenge expired or missing — start registration again",
            )
        try:
            webauthn_mgr.verify_registration(user, challenge, data.passkey_credential)
        except WebAuthnError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    log.info("invitation redeemed via API", username=user.username)
    return TokenResponse(
        access_token=tokens.create_access_token(user),
        refresh_token=tokens.create_refresh_token(user),
    )


# ---------------------------------------------------------------------------
# OAuth
# ---------------------------------------------------------------------------


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
