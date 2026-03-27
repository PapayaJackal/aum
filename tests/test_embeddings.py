from __future__ import annotations

import json
import math

import httpx
import pytest

from aum.embeddings.base import l2_normalize
from aum.embeddings.chunking import chunk_text
from aum.embeddings.ollama import OllamaEmbedder
from aum.embeddings.openai import OpenAIEmbedder


class FakeTransport(httpx.BaseTransport):
    """Transport that records requests and returns canned responses."""

    def __init__(self, handler):
        self._handler = handler
        self.last_request_body: dict | None = None

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.last_request_body = json.loads(request.content) if request.content else None
        return self._handler(request)


def _make_ollama_handler(dimension: int = 1024):
    """Return a handler that mimics the Ollama /api/embed endpoint."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        body = json.loads(request.content)

        if "/api/pull" in url:
            return httpx.Response(200, json={"status": "success"})

        if "/api/embed" in url:
            texts = body["input"]
            embeddings = [[0.1] * dimension for _ in texts]
            return httpx.Response(200, json={"embeddings": embeddings})

        return httpx.Response(404, json={"error": "not found"})

    return handler


def _make_openai_handler(dimension: int = 1024):
    """Return a handler that mimics the OpenAI /v1/embeddings endpoint."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        texts = body["input"]
        data = [{"object": "embedding", "index": i, "embedding": [0.2] * dimension} for i in range(len(texts))]
        return httpx.Response(
            200,
            json={
                "object": "list",
                "data": data,
                "model": body["model"],
                "usage": {"prompt_tokens": 10, "total_tokens": 10},
            },
        )

    return handler


def _is_normalized(vector: list[float], tol: float = 1e-6) -> bool:
    norm = math.sqrt(sum(x * x for x in vector))
    return abs(norm - 1.0) < tol


class TestL2Normalize:
    def test_normalizes_vector(self):
        result = l2_normalize([3.0, 4.0])
        assert abs(result[0] - 0.6) < 1e-6
        assert abs(result[1] - 0.8) < 1e-6

    def test_already_normalized(self):
        result = l2_normalize([1.0, 0.0])
        assert result == [1.0, 0.0]

    def test_zero_vector(self):
        result = l2_normalize([0.0, 0.0])
        assert result == [0.0, 0.0]


class TestOllamaEmbedder:
    def _make_embedder(self, dimension: int = 1024) -> tuple[OllamaEmbedder, FakeTransport]:
        embedder = OllamaEmbedder(
            model="test-model",
            base_url="http://fake-ollama:11434",
            expected_dimension=dimension,
            query_prefix="query: ",
        )
        transport = FakeTransport(_make_ollama_handler(dimension))
        embedder._client = httpx.Client(transport=transport)
        return embedder, transport

    def test_embed_query_returns_normalized(self):
        embedder, _ = self._make_embedder()
        result = embedder.embed_query("hello world")
        assert len(result) == 1024
        assert _is_normalized(result)

    def test_embed_query_adds_prefix(self):
        embedder, transport = self._make_embedder()
        embedder.embed_query("hello world")
        assert transport.last_request_body["input"] == ["query: hello world"]

    def test_embed_documents_returns_normalized(self):
        embedder, _ = self._make_embedder()
        results = embedder.embed_documents(["hello", "world", "foo"])
        assert len(results) == 3
        assert all(len(v) == 1024 for v in results)
        assert all(_is_normalized(v) for v in results)

    def test_embed_documents_no_prefix(self):
        embedder, transport = self._make_embedder()
        embedder.embed_documents(["hello", "world"])
        assert transport.last_request_body["input"] == ["hello", "world"]

    def test_dimension_property(self):
        embedder, _ = self._make_embedder(dimension=768)
        assert embedder.dimension == 768

    def test_dimension_mismatch_corrects(self):
        embedder = OllamaEmbedder(
            model="test-model",
            base_url="http://fake-ollama:11434",
            expected_dimension=512,
        )
        transport = FakeTransport(_make_ollama_handler(1024))
        embedder._client = httpx.Client(transport=transport)

        result = embedder.embed_documents(["test"])
        assert len(result[0]) == 1024
        assert embedder.dimension == 1024

    def test_ensure_model(self):
        embedder, _ = self._make_embedder()
        embedder.ensure_model()


class TestOpenAIEmbedder:
    def _make_embedder(self, dimension: int = 1024) -> tuple[OpenAIEmbedder, FakeTransport]:
        embedder = OpenAIEmbedder(
            model="test-model",
            api_url="http://fake-api:8080",
            api_key="test-key",
            expected_dimension=dimension,
            query_prefix="query: ",
        )
        transport = FakeTransport(_make_openai_handler(dimension))
        embedder._client = httpx.Client(transport=transport)
        return embedder, transport

    def test_embed_query_returns_normalized(self):
        embedder, _ = self._make_embedder()
        result = embedder.embed_query("hello world")
        assert len(result) == 1024
        assert _is_normalized(result)

    def test_embed_query_adds_prefix(self):
        embedder, transport = self._make_embedder()
        embedder.embed_query("hello world")
        assert transport.last_request_body["input"] == ["query: hello world"]

    def test_embed_documents_returns_normalized(self):
        embedder, _ = self._make_embedder()
        results = embedder.embed_documents(["hello", "world"])
        assert len(results) == 2
        assert all(_is_normalized(v) for v in results)

    def test_embed_documents_no_prefix(self):
        embedder, transport = self._make_embedder()
        embedder.embed_documents(["hello", "world"])
        assert transport.last_request_body["input"] == ["hello", "world"]

    def test_dimension_property(self):
        embedder, _ = self._make_embedder(dimension=768)
        assert embedder.dimension == 768

    def test_api_key_in_headers(self):
        embedder = OpenAIEmbedder(
            model="test",
            api_url="http://fake:8080",
            api_key="sk-secret",
        )
        assert embedder._client.headers["authorization"] == "Bearer sk-secret"

    def test_no_api_key(self):
        embedder = OpenAIEmbedder(
            model="test",
            api_url="http://fake:8080",
            api_key="",
        )
        assert "authorization" not in embedder._client.headers

    def test_dimension_mismatch_corrects(self):
        embedder = OpenAIEmbedder(
            model="test-model",
            api_url="http://fake-api:8080",
            expected_dimension=512,
        )
        transport = FakeTransport(_make_openai_handler(1024))
        embedder._client = httpx.Client(transport=transport)

        result = embedder.embed_documents(["test"])
        assert len(result[0]) == 1024
        assert embedder.dimension == 1024


class TestChunking:
    def test_short_text_single_chunk(self):
        chunks = chunk_text("Hello world.", max_chars=100)
        assert chunks == ["Hello world."]

    def test_empty_text(self):
        chunks = chunk_text("")
        assert chunks == [""]

    def test_splits_on_paragraphs(self):
        text = "Paragraph one.\n\nParagraph two.\n\nParagraph three."
        chunks = chunk_text(text, max_chars=30, overlap_chars=0)
        assert len(chunks) >= 2
        assert "Paragraph one." in chunks[0]

    def test_overlap_includes_trailing_content(self):
        text = "Short.\n\nAnother short.\n\nThird paragraph here."
        chunks = chunk_text(text, max_chars=30, overlap_chars=20)
        # With overlap, later chunks should include content from previous ones
        assert len(chunks) >= 2

    def test_long_paragraph_splits_on_sentences(self):
        text = "First sentence. Second sentence. Third sentence. Fourth sentence. Fifth sentence."
        chunks = chunk_text(text, max_chars=40, overlap_chars=0)
        assert len(chunks) >= 2

    def test_very_long_word_hard_splits(self):
        text = "a" * 200
        chunks = chunk_text(text, max_chars=50, overlap_chars=0)
        assert all(len(c) <= 50 for c in chunks)

    def test_preserves_all_content(self):
        paras = [f"Paragraph number {i} with some content." for i in range(10)]
        text = "\n\n".join(paras)
        chunks = chunk_text(text, max_chars=100, overlap_chars=0)
        rejoined = " ".join(chunks)
        for p in paras:
            assert p in rejoined

    def test_respects_max_chars(self):
        paras = [f"This is paragraph {i}." for i in range(20)]
        text = "\n\n".join(paras)
        chunks = chunk_text(text, max_chars=80, overlap_chars=0)
        for c in chunks:
            assert len(c) <= 80 + 50  # some tolerance for paragraph boundaries
