//! Text chunking for embedding pipelines.
//!
//! [`chunk_text`] splits a document into overlapping character-based chunks
//! that respect paragraph and sentence boundaries, keeping individual chunks
//! within a configurable size limit.

use std::sync::LazyLock;

use regex::Regex;

#[expect(clippy::expect_used, reason = "infallible constant regex")]
static PARA_SPLIT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\n\s*\n").expect("valid regex"));

/// Split *text* into overlapping chunks no longer than `max_chars`, breaking
/// preferably at paragraph boundaries (double-newline), then sentence
/// boundaries, then hard-splitting at the character limit.
///
/// `overlap_chars` controls how many characters of context are carried over
/// from the end of one chunk into the beginning of the next.
///
/// Always returns at least one element, even for empty input.
pub fn chunk_text(text: &str, max_chars: usize, overlap_chars: usize) -> Vec<String> {
    if text.is_empty() || text.trim().is_empty() {
        return vec![text.to_owned()];
    }

    let paragraphs: Vec<&str> = PARA_SPLIT
        .split(text)
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();

    if paragraphs.is_empty() {
        return vec![text.trim().to_owned()];
    }

    let mut chunks: Vec<String> = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    let mut current_len: usize = 0;

    for para in &paragraphs {
        let para_len = para.len();

        if para_len > max_chars {
            // Flush current buffer first.
            if !current.is_empty() {
                chunks.push(current.join("\n\n"));
                let (overlap, overlap_len) = take_para_overlap(&current, overlap_chars);
                current = overlap;
                current_len = overlap_len;
            }
            for sub in split_long_paragraph(para, max_chars, overlap_chars) {
                chunks.push(sub);
            }
            continue;
        }

        // Would adding this paragraph exceed the limit?
        let sep_len = if current.is_empty() { 0 } else { 2 };
        if !current.is_empty() && current_len + sep_len + para_len > max_chars {
            chunks.push(current.join("\n\n"));
            let (overlap, overlap_len) = take_para_overlap(&current, overlap_chars);
            current = overlap;
            current_len = overlap_len;
        }

        let new_sep = if current.is_empty() { 0 } else { 2 };
        current_len += new_sep + para_len;
        current.push(para);
    }

    if !current.is_empty() {
        chunks.push(current.join("\n\n"));
    }

    chunks
}

/// Take trailing paragraphs from *paragraphs* that fit within `overlap_chars`.
///
/// Returns the new current-buffer slice and its total character length.
fn take_para_overlap<'a>(paragraphs: &[&'a str], overlap_chars: usize) -> (Vec<&'a str>, usize) {
    if overlap_chars == 0 {
        return (Vec::new(), 0);
    }
    let mut result: Vec<&str> = Vec::new();
    let mut total: usize = 0;
    for &para in paragraphs.iter().rev() {
        let cost = para.len() + if result.is_empty() { 0 } else { 2 };
        if total + cost > overlap_chars {
            break;
        }
        result.push(para);
        total += cost;
    }
    result.reverse();
    (result, total)
}

/// Split an oversized paragraph at sentence boundaries, falling back to hard
/// character splits for sentences that are themselves too long.
fn split_long_paragraph(text: &str, max_chars: usize, overlap_chars: usize) -> Vec<String> {
    let sentences = split_sentences(text);

    let mut chunks: Vec<String> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut current_len: usize = 0;

    for sent in sentences {
        let sent_len = sent.len();

        if sent_len > max_chars {
            // Flush first.
            if !current.is_empty() {
                chunks.push(current.join(" "));
                current.clear();
                current_len = 0;
            }
            // Hard-split the oversized sentence with overlap.
            let step = if max_chars > overlap_chars {
                max_chars - overlap_chars
            } else {
                max_chars
            };
            let bytes = sent.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                let end = (i + max_chars).min(bytes.len());
                // Adjust to a valid UTF-8 boundary.
                let end = find_char_boundary(&sent, end);
                chunks.push(sent[i..end].to_owned());
                i += step;
            }
            continue;
        }

        let sep_len = usize::from(!current.is_empty());
        if !current.is_empty() && current_len + sep_len + sent_len > max_chars {
            chunks.push(current.join(" "));
            let (overlap, overlap_len) = take_sentence_overlap(&current, overlap_chars);
            current = overlap;
            current_len = overlap_len;
        }

        let new_sep = usize::from(!current.is_empty());
        current_len += new_sep + sent_len;
        current.push(sent);
    }

    if !current.is_empty() {
        chunks.push(current.join(" "));
    }

    chunks
}

/// Split *text* on sentence-ending punctuation followed by whitespace,
/// keeping the punctuation attached to the sentence that ends with it.
///
/// Equivalent to Python's `re.compile(r"(?<=[.!?])\s+").split(text)` but
/// without requiring lookbehind (not supported by the `regex` crate).
fn split_sentences(text: &str) -> Vec<String> {
    // Match `[.!?]` followed by one or more whitespace chars.
    // Capture group 1 holds the punctuation so we can re-attach it.
    #[expect(clippy::expect_used, reason = "infallible constant regex")]
    static SENT_SPLIT: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"([.!?])\s+").expect("valid regex"));

    let mut result: Vec<String> = Vec::new();
    let mut last_end = 0;

    for cap in SENT_SPLIT.captures_iter(text) {
        // Group 0 and group 1 are always present when the pattern matches.
        let Some(full_match) = cap.get(0) else {
            continue;
        };
        let Some(punct) = cap.get(1) else { continue };
        // Sentence ends after the punctuation char.
        let sentence_end = punct.end();
        result.push(text[last_end..sentence_end].to_owned());
        last_end = full_match.end();
    }

    if last_end < text.len() {
        result.push(text[last_end..].to_owned());
    }

    result
}

/// Take trailing sentences from *sentences* that fit within `overlap_chars`.
fn take_sentence_overlap(sentences: &[String], overlap_chars: usize) -> (Vec<String>, usize) {
    if overlap_chars == 0 {
        return (Vec::new(), 0);
    }
    let mut result: Vec<String> = Vec::new();
    let mut total: usize = 0;
    for sent in sentences.iter().rev() {
        let cost = sent.len() + usize::from(!result.is_empty());
        if total + cost > overlap_chars {
            break;
        }
        result.push(sent.clone());
        total += cost;
    }
    result.reverse();
    (result, total)
}

/// Return the largest index ≤ *pos* that lies on a UTF-8 character boundary.
fn find_char_boundary(s: &str, pos: usize) -> usize {
    if pos >= s.len() {
        return s.len();
    }
    let mut p = pos;
    while p > 0 && !s.is_char_boundary(p) {
        p -= 1;
    }
    p
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_returns_one_empty_chunk() {
        assert_eq!(chunk_text("", 100, 20), vec![""]);
    }

    #[test]
    fn whitespace_only_returns_one_chunk() {
        let result = chunk_text("   \n\n   ", 100, 20);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn short_text_no_split() {
        let text = "Hello world.";
        let result = chunk_text(text, 100, 20);
        assert_eq!(result, vec!["Hello world."]);
    }

    #[test]
    fn two_short_paragraphs_fit_in_one_chunk() {
        let text = "Para one.\n\nPara two.";
        let result = chunk_text(text, 100, 20);
        assert_eq!(result, vec!["Para one.\n\nPara two."]);
    }

    #[test]
    fn paragraphs_split_at_limit() {
        let text = "AAAAAAAAAA\n\nBBBBBBBBBB\n\nCCCCCCCCCC";
        // max=22: "AAAAAAAAAA\n\nBBBBBBBBBB" = 22 chars exactly
        let result = chunk_text(text, 22, 0);
        assert_eq!(result, vec!["AAAAAAAAAA\n\nBBBBBBBBBB", "CCCCCCCCCC"]);
    }

    #[test]
    fn paragraph_overlap_carries_context() {
        // Each para is 10 chars; overlap_chars=12 means 1 para carries over.
        let text = "AAAAAAAAAA\n\nBBBBBBBBBB\n\nCCCCCCCCCC";
        let result = chunk_text(text, 22, 12);
        assert_eq!(result[0], "AAAAAAAAAA\n\nBBBBBBBBBB");
        assert_eq!(result[1], "BBBBBBBBBB\n\nCCCCCCCCCC");
    }

    #[test]
    fn long_paragraph_falls_back_to_sentences() {
        let text = "First sentence. Second sentence. Third sentence.";
        // max=20 forces split but individual sentences fit
        let result = chunk_text(text, 20, 0);
        assert!(result.len() > 1);
        for chunk in &result {
            assert!(chunk.len() <= 20, "chunk too long: {chunk:?}");
        }
    }

    #[test]
    fn sentence_split_preserves_punctuation() {
        let sents = split_sentences("Hello. World! How are you?");
        assert_eq!(sents, vec!["Hello.", "World!", "How are you?"]);
    }

    #[test]
    fn sentence_split_no_boundaries() {
        let sents = split_sentences("No punctuation here");
        assert_eq!(sents, vec!["No punctuation here"]);
    }

    #[test]
    fn hard_split_for_huge_sentence() {
        let long_sent = "A".repeat(50);
        let result = chunk_text(&long_sent, 20, 5);
        for chunk in &result {
            assert!(chunk.len() <= 20, "chunk too long: {}", chunk.len());
        }
    }

    #[test]
    fn overlap_between_sentence_chunks() {
        // 3 sentences of 11 chars each; max=22 forces flush after the first
        // sentence when the second would exceed the limit.
        // Overlap of 12 carries "AAAAAAAAAA." into the next chunk.
        let text = "AAAAAAAAAA. BBBBBBBBBB. CCCCCCCCCC.";
        let result = split_long_paragraph(text, 22, 12);
        assert!(
            result.len() >= 2,
            "expected at least 2 chunks, got {result:?}"
        );
        // Chunk 0 is just the first sentence.
        assert_eq!(result[0], "AAAAAAAAAA.");
        // Chunk 1 starts with the overlap (AAAAAAAAAA.) then adds BBBBBBBBBB.
        assert_eq!(result[1], "AAAAAAAAAA. BBBBBBBBBB.");
    }
}
