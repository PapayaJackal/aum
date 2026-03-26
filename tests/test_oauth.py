"""Tests for OAuth user creation and linking logic."""

from __future__ import annotations

import pytest

from aum.auth.oauth import OAuthManager


class TestOAuthGetOrCreateUser:
    @pytest.fixture(autouse=True)
    def setup(self, db_conn):
        # Empty provider list — we only test the DB logic, not authlib clients
        self.manager = OAuthManager(db_conn, providers=[])
        self.conn = db_conn

    def test_creates_new_user(self):
        user = self.manager.get_or_create_user("github", {
            "sub": "gh-123",
            "email": "alice@example.com",
            "name": "Alice Smith",
        })
        assert user.username == "alice_smith"
        assert user.is_admin is False

    def test_returns_existing_linked_user(self):
        user1 = self.manager.get_or_create_user("github", {
            "sub": "gh-123",
            "email": "bob@example.com",
            "name": "Bob",
        })
        user2 = self.manager.get_or_create_user("github", {
            "sub": "gh-123",
            "email": "bob@example.com",
            "name": "Bob",
        })
        assert user1.id == user2.id
        assert user1.username == user2.username

    def test_links_by_email(self):
        # First login via GitHub
        user1 = self.manager.get_or_create_user("github", {
            "sub": "gh-456",
            "email": "carol@example.com",
            "name": "Carol",
        })
        # Second login via Google with same email → links to existing user
        user2 = self.manager.get_or_create_user("google", {
            "sub": "goog-789",
            "email": "carol@example.com",
            "name": "Carol D",
        })
        assert user1.id == user2.id

    def test_unique_username_collision(self):
        user1 = self.manager.get_or_create_user("github", {
            "sub": "gh-100",
            "email": "dave1@example.com",
            "name": "Dave",
        })
        user2 = self.manager.get_or_create_user("github", {
            "sub": "gh-200",
            "email": "dave2@example.com",
            "name": "Dave",
        })
        assert user1.username == "dave"
        assert user2.username == "dave_1"
        assert user1.id != user2.id

    def test_empty_name_fallback(self):
        user = self.manager.get_or_create_user("github", {
            "sub": "gh-300",
            "email": "anon@example.com",
        })
        # Falls back to email as name, which becomes the username
        assert user.username == "anon@example.com"
