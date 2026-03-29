import pytest

from aum.auth.local import AuthError, LocalAuth, PasswordPolicyError, validate_password
from aum.auth.permissions import PermissionDeniedError, PermissionManager
from aum.auth.tokens import TokenError, TokenManager

# Meets the 8-char, upper+lower+digit+special policy.
_VALID_PW = "Test12!@pass"


class TestPasswordPolicy:
    def test_valid_password(self):
        validate_password("Secure9!")

    def test_too_short(self):
        with pytest.raises(PasswordPolicyError, match="at least 8 characters"):
            validate_password("Ab1!")

    def test_no_uppercase(self):
        with pytest.raises(PasswordPolicyError, match="at least one uppercase letter"):
            validate_password("alllower1!")

    def test_no_lowercase(self):
        with pytest.raises(PasswordPolicyError, match="at least one lowercase letter"):
            validate_password("ALLUPPER1!")

    def test_no_digits(self):
        with pytest.raises(PasswordPolicyError, match="at least one digit"):
            validate_password("NoDigits!!")

    def test_no_special(self):
        with pytest.raises(PasswordPolicyError, match="at least one special character"):
            validate_password("NoSpecial1A")

    def test_create_user_rejects_weak(self, local_auth: LocalAuth):
        with pytest.raises(PasswordPolicyError):
            local_auth.create_user("weakuser", "short")

    def test_set_password_rejects_weak(self, local_auth: LocalAuth):
        local_auth.create_user("pwuser", _VALID_PW)
        with pytest.raises(PasswordPolicyError):
            local_auth.set_password("pwuser", "short")


class TestLocalAuth:
    def test_create_and_authenticate(self, local_auth: LocalAuth):
        user = local_auth.create_user("alice", _VALID_PW)
        assert user.username == "alice"
        assert user.is_admin is False

        authed = local_auth.authenticate("alice", _VALID_PW)
        assert authed.id == user.id

    def test_bad_password(self, local_auth: LocalAuth):
        local_auth.create_user("bob", _VALID_PW)
        with pytest.raises(AuthError):
            local_auth.authenticate("bob", "wrong")

    def test_unknown_user(self, local_auth: LocalAuth):
        with pytest.raises(AuthError):
            local_auth.authenticate("nobody", "pass")

    def test_list_users(self, local_auth: LocalAuth):
        local_auth.create_user("a", _VALID_PW)
        local_auth.create_user("b", _VALID_PW)
        users = local_auth.list_users()
        assert len(users) == 2

    def test_delete_user(self, local_auth: LocalAuth):
        local_auth.create_user("temp", _VALID_PW)
        assert local_auth.delete_user("temp") is True
        assert local_auth.delete_user("temp") is False

    def test_set_admin(self, local_auth: LocalAuth):
        local_auth.create_user("mod", _VALID_PW)
        assert local_auth.set_admin("mod", True) is True
        user = local_auth.get_user_by_username("mod")
        assert user is not None
        assert user.is_admin is True


class TestInvitations:
    def test_create_invitation(self, local_auth: LocalAuth):
        inv = local_auth.create_invitation("newuser")
        assert inv.username == "newuser"
        assert inv.is_admin is False
        assert inv.used_at is None
        assert len(inv.token) > 0

    def test_get_valid_invitation(self, local_auth: LocalAuth):
        inv = local_auth.create_invitation("invitee")
        fetched = local_auth.get_invitation(inv.token)
        assert fetched is not None
        assert fetched.username == "invitee"

    def test_get_invalid_token(self, local_auth: LocalAuth):
        assert local_auth.get_invitation("nonexistent") is None

    def test_get_expired_invitation(self, local_auth: LocalAuth, db_conn):
        inv = local_auth.create_invitation("expired_user")
        # Manually expire it
        db_conn.execute(
            "UPDATE invitations SET expires_at = '2000-01-01T00:00:00+00:00' WHERE id = ?",
            (inv.id,),
        )
        db_conn.commit()
        assert local_auth.get_invitation(inv.token) is None

    def test_get_used_invitation(self, local_auth: LocalAuth):
        inv = local_auth.create_invitation("used_user")
        local_auth.redeem_invitation(inv.token, password=_VALID_PW)
        assert local_auth.get_invitation(inv.token) is None

    def test_redeem_with_password(self, local_auth: LocalAuth):
        inv = local_auth.create_invitation("pwuser", is_admin=True)
        user = local_auth.redeem_invitation(inv.token, password=_VALID_PW)
        assert user.username == "pwuser"
        assert user.is_admin is True
        assert user.password_hash is not None

        # Can authenticate with password
        authed = local_auth.authenticate("pwuser", _VALID_PW)
        assert authed.id == user.id

    def test_redeem_without_password(self, local_auth: LocalAuth):
        inv = local_auth.create_invitation("passkeyuser")
        user = local_auth.redeem_invitation(inv.token)
        assert user.username == "passkeyuser"
        assert user.password_hash is None

    def test_redeem_double_use(self, local_auth: LocalAuth):
        inv = local_auth.create_invitation("doubleuser")
        local_auth.redeem_invitation(inv.token, password=_VALID_PW)
        with pytest.raises(AuthError, match="Invalid or expired"):
            local_auth.redeem_invitation(inv.token, password=_VALID_PW)

    def test_redeem_username_conflict(self, local_auth: LocalAuth):
        local_auth.create_user("existing", _VALID_PW)
        inv = local_auth.create_invitation("existing")
        with pytest.raises(AuthError, match="already taken"):
            local_auth.redeem_invitation(inv.token, password=_VALID_PW)

    def test_create_user_without_password(self, local_auth: LocalAuth):
        user = local_auth.create_user_without_password("nopassuser")
        assert user.password_hash is None
        assert user.is_admin is False
        with pytest.raises(AuthError, match="OAuth login only"):
            local_auth.authenticate("nopassuser", "anything")


class TestPermissions:
    def test_grant_and_check(self, local_auth: LocalAuth, permissions: PermissionManager):
        user = local_auth.create_user("viewer", _VALID_PW)
        assert permissions.check(user, "docs") is False

        permissions.grant("viewer", "docs")
        assert permissions.check(user, "docs") is True

    def test_admin_has_access(self, local_auth: LocalAuth, permissions: PermissionManager):
        admin = local_auth.create_user("admin", _VALID_PW, is_admin=True)
        assert permissions.check(admin, "anything") is True

    def test_revoke(self, local_auth: LocalAuth, permissions: PermissionManager):
        local_auth.create_user("revoker", _VALID_PW)
        permissions.grant("revoker", "docs")
        assert permissions.revoke("revoker", "docs") is True
        user = local_auth.get_user_by_username("revoker")
        assert user is not None
        assert permissions.check(user, "docs") is False

    def test_require_raises(self, local_auth: LocalAuth, permissions: PermissionManager):
        user = local_auth.create_user("blocked", _VALID_PW)
        with pytest.raises(PermissionDeniedError):
            permissions.require(user, "secret")


class TestTokens:
    def test_access_token_roundtrip(self, local_auth: LocalAuth):
        user = local_auth.create_user("tokenuser", _VALID_PW)
        mgr = TokenManager(secret="testsecret-thats-long-enough!!XY")
        token = mgr.create_access_token(user)
        payload = mgr.verify_access_token(token)
        assert payload["sub"] == str(user.id)
        assert payload["username"] == "tokenuser"

    def test_refresh_token_roundtrip(self, local_auth: LocalAuth):
        user = local_auth.create_user("refreshuser", _VALID_PW)
        mgr = TokenManager(secret="testsecret-thats-long-enough!!XY")
        token = mgr.create_refresh_token(user)
        payload = mgr.verify_refresh_token(token)
        assert payload["sub"] == str(user.id)

    def test_wrong_token_type(self, local_auth: LocalAuth):
        user = local_auth.create_user("wrongtype", _VALID_PW)
        mgr = TokenManager(secret="testsecret-thats-long-enough!!XY")
        refresh = mgr.create_refresh_token(user)
        with pytest.raises(TokenError):
            mgr.verify_access_token(refresh)

    def test_invalid_token(self):
        mgr = TokenManager(secret="testsecret-thats-long-enough!!XY")
        with pytest.raises(TokenError):
            mgr.verify_access_token("garbage.token.here")

    def test_passkey_enroll_token_roundtrip(self, local_auth: LocalAuth):
        user = local_auth.create_user("enrolluser", _VALID_PW)
        mgr = TokenManager(secret="testsecret-thats-long-enough!!XY")
        token = mgr.create_passkey_enroll_token(user)
        payload = mgr.verify_passkey_enroll_token(token)
        assert payload["sub"] == str(user.id)
        assert payload["type"] == "passkey_enroll"

    def test_passkey_enroll_token_wrong_type(self, local_auth: LocalAuth):
        user = local_auth.create_user("enrollwrong", _VALID_PW)
        mgr = TokenManager(secret="testsecret-thats-long-enough!!XY")
        access = mgr.create_access_token(user)
        with pytest.raises(TokenError, match="Not a passkey enrollment"):
            mgr.verify_passkey_enroll_token(access)
