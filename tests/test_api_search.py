"""Tests for the API search endpoint, focusing on multi-index support."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from aum.api.routes.search import _get_embedder_for_indices


class TestGetEmbedderForIndices:
    """Unit tests for the hybrid embedding validation logic."""

    def test_single_index_no_embeddings(self):
        mock_tracker = MagicMock()
        mock_tracker.get_embedding_model.return_value = None

        with patch("aum.api.routes.search.get_config") as mock_config, \
             patch("aum.api.deps.make_tracker", return_value=mock_tracker):
            mock_config.return_value = MagicMock()
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                _get_embedder_for_indices(["idx1"])
            assert exc_info.value.status_code == 400
            assert "No embeddings found" in exc_info.value.detail

    def test_multi_index_one_missing_embeddings(self):
        mock_tracker = MagicMock()
        mock_tracker.get_embedding_model.side_effect = [
            ("model-a", "ollama", 768),
            None,
        ]

        with patch("aum.api.routes.search.get_config") as mock_config, \
             patch("aum.api.deps.make_tracker", return_value=mock_tracker):
            mock_config.return_value = MagicMock()
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                _get_embedder_for_indices(["idx1", "idx2"])
            assert exc_info.value.status_code == 400
            assert "idx2" in exc_info.value.detail

    def test_multi_index_model_mismatch(self):
        mock_tracker = MagicMock()
        mock_tracker.get_embedding_model.side_effect = [
            ("model-a", "ollama", 768),
            ("model-b", "ollama", 768),
        ]

        with patch("aum.api.routes.search.get_config") as mock_config, \
             patch("aum.api.deps.make_tracker", return_value=mock_tracker):
            mock_config.return_value = MagicMock()
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                _get_embedder_for_indices(["idx1", "idx2"])
            assert exc_info.value.status_code == 400
            assert "mismatch" in exc_info.value.detail.lower()

    def test_multi_index_same_model_succeeds(self):
        mock_tracker = MagicMock()
        mock_tracker.get_embedding_model.side_effect = [
            ("model-a", "ollama", 768),
            ("model-a", "ollama", 768),
        ]
        mock_embedder = MagicMock()

        with patch("aum.api.routes.search.get_config") as mock_config, \
             patch("aum.api.deps.make_tracker", return_value=mock_tracker), \
             patch("aum.api.deps.make_embedder", return_value=mock_embedder):
            cfg = MagicMock()
            mock_config.return_value = cfg
            result = _get_embedder_for_indices(["idx1", "idx2"])
            assert result is mock_embedder
            assert cfg.embeddings_model == "model-a"
            assert cfg.embeddings_backend == "ollama"
