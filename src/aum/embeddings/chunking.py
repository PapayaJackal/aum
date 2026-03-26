from __future__ import annotations

import re


def chunk_text(
    text: str,
    max_chars: int = 2000,
    overlap_chars: int = 200,
) -> list[str]:
    """Split text into overlapping chunks, breaking at paragraph boundaries.

    Tries to split on double-newline paragraph breaks. When a single paragraph
    exceeds ``max_chars``, falls back to sentence boundaries, then hard splits.

    Returns at least one chunk even for empty input.
    """
    if not text or not text.strip():
        return [text or ""]

    # Split into paragraphs (double-newline separated)
    paragraphs = re.split(r"\n\s*\n", text)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    if not paragraphs:
        return [text.strip()]

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para)

        # If this single paragraph is too long, split it further
        if para_len > max_chars:
            # Flush current buffer first
            if current:
                chunks.append("\n\n".join(current))
                current, current_len = _take_overlap(current, overlap_chars)

            for sub in _split_long_paragraph(para, max_chars, overlap_chars):
                chunks.append(sub)
            continue

        # Would adding this paragraph exceed the limit?
        # Account for the "\n\n" join separator
        sep_len = 2 if current else 0
        if current_len + sep_len + para_len > max_chars and current:
            chunks.append("\n\n".join(current))
            current, current_len = _take_overlap(current, overlap_chars)

        current.append(para)
        current_len += (2 if len(current) > 1 else 0) + para_len

    if current:
        chunks.append("\n\n".join(current))

    return chunks


def _take_overlap(paragraphs: list[str], overlap_chars: int) -> tuple[list[str], int]:
    """Take trailing paragraphs that fit within the overlap budget."""
    if overlap_chars <= 0:
        return [], 0
    result: list[str] = []
    total = 0
    for para in reversed(paragraphs):
        cost = len(para) + (2 if result else 0)
        if total + cost > overlap_chars:
            break
        result.append(para)
        total += cost
    result.reverse()
    return result, total


_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


def _split_long_paragraph(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    """Split an oversized paragraph, preferring sentence boundaries."""
    sentences = _SENTENCE_SPLIT.split(text)

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for sent in sentences:
        sent_len = len(sent)

        # Single sentence too long — hard split
        if sent_len > max_chars:
            if current:
                chunks.append(" ".join(current))
                current = []
                current_len = 0
            for i in range(0, sent_len, max_chars - overlap_chars):
                chunks.append(sent[i : i + max_chars])
            continue

        sep_len = 1 if current else 0
        if current_len + sep_len + sent_len > max_chars and current:
            chunks.append(" ".join(current))
            # Overlap: take trailing sentences
            current, current_len = _take_sentence_overlap(current, overlap_chars)

        current.append(sent)
        current_len += sep_len + sent_len

    if current:
        chunks.append(" ".join(current))

    return chunks


def _take_sentence_overlap(sentences: list[str], overlap_chars: int) -> tuple[list[str], int]:
    """Take trailing sentences that fit within the overlap budget."""
    if overlap_chars <= 0:
        return [], 0
    result: list[str] = []
    total = 0
    for sent in reversed(sentences):
        cost = len(sent) + (1 if result else 0)
        if total + cost > overlap_chars:
            break
        result.append(sent)
        total += cost
    result.reverse()
    return result, total
