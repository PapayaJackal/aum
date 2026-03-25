import pytest

from aum.auth.local import AuthError, LocalAuth
from aum.auth.permissions import PermissionDeniedError, PermissionManager
from aum.auth.tokens import TokenError, TokenManager


class TestLocalAuth:
    def test_create_and_authenticate(self, local_auth: LocalAuth):
        user = local_auth.create_user("alice", "secret123")
        assert user.username == "alice"
        assert user.is_admin is False

        authed = local_auth.authenticate("alice", "secret123")
        assert authed.id == user.id

    def test_bad_password(self, local_auth: LocalAuth):
        local_auth.create_user("bob", "correct")
        with pytest.raises(AuthError):
            local_auth.authenticate("bob", "wrong")

    def test_unknown_user(self, local_auth: LocalAuth):
        with pytest.raises(AuthError):
            local_auth.authenticate("nobody", "pass")

    def test_list_users(self, local_auth: LocalAuth):
        local_auth.create_user("a", "pass")
        local_auth.create_user("b", "pass")
        users = local_auth.list_users()
        assert len(users) == 2

    def test_delete_user(self, local_auth: LocalAuth):
        local_auth.create_user("temp", "pass")
        assert local_auth.delete_user("temp") is True
        assert local_auth.delete_user("temp") is False

    def test_set_admin(self, local_auth: LocalAuth):
        local_auth.create_user("mod", "pass")
        assert local_auth.set_admin("mod", True) is True
        user = local_auth.get_user_by_username("mod")
        assert user is not None
        assert user.is_admin is True


class TestPermissions:
    def test_grant_and_check(self, local_auth: LocalAuth, permissions: PermissionManager):
        user = local_auth.create_user("viewer", "pass")
        assert permissions.check(user, "docs") is False

        permissions.grant("viewer", "docs")
        assert permissions.check(user, "docs") is True

    def test_admin_has_access(self, local_auth: LocalAuth, permissions: PermissionManager):
        admin = local_auth.create_user("admin", "pass", is_admin=True)
        assert permissions.check(admin, "anything") is True

    def test_revoke(self, local_auth: LocalAuth, permissions: PermissionManager):
        local_auth.create_user("revoker", "pass")
        permissions.grant("revoker", "docs")
        assert permissions.revoke("revoker", "docs") is True
        user = local_auth.get_user_by_username("revoker")
        assert user is not None
        assert permissions.check(user, "docs") is False

    def test_require_raises(self, local_auth: LocalAuth, permissions: PermissionManager):
        user = local_auth.create_user("blocked", "pass")
        with pytest.raises(PermissionDeniedError):
            permissions.require(user, "secret")


class TestTokens:
    def test_access_token_roundtrip(self, local_auth: LocalAuth):
        user = local_auth.create_user("tokenuser", "pass")
        mgr = TokenManager(secret="testsecret")
        token = mgr.create_access_token(user)
        payload = mgr.verify_access_token(token)
        assert payload["sub"] == str(user.id)
        assert payload["username"] == "tokenuser"

    def test_refresh_token_roundtrip(self, local_auth: LocalAuth):
        user = local_auth.create_user("refreshuser", "pass")
        mgr = TokenManager(secret="testsecret")
        token = mgr.create_refresh_token(user)
        payload = mgr.verify_refresh_token(token)
        assert payload["sub"] == str(user.id)

    def test_wrong_token_type(self, local_auth: LocalAuth):
        user = local_auth.create_user("wrongtype", "pass")
        mgr = TokenManager(secret="testsecret")
        refresh = mgr.create_refresh_token(user)
        with pytest.raises(TokenError):
            mgr.verify_access_token(refresh)

    def test_invalid_token(self):
        mgr = TokenManager(secret="testsecret")
        with pytest.raises(TokenError):
            mgr.verify_access_token("garbage.token.here")
