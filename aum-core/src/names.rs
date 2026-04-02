//! Human-readable name generator for job IDs.
//!
//! Produces jellybean/ice-cream inspired `adjective_flavour_suffix` names
//! (e.g. `fuzzy_pistachio_a3f`) that are easy to remember and communicate.

use rand::RngExt as _;

// ---------------------------------------------------------------------------
// Word lists
// ---------------------------------------------------------------------------

const ADJECTIVES: &[&str] = &[
    "bitter", "bright", "bubbly", "buttery", "candied", "chunky", "creamy", "crispy", "crunchy",
    "crystal", "dipped", "dreamy", "fizzy", "fluffy", "frosty", "frozen", "fruity", "fudgy",
    "fuzzy", "glazed", "gooey", "golden", "icy", "jelly", "juicy", "layered", "malted", "melted",
    "minty", "nutty", "peachy", "puffy", "rich", "rippled", "roasted", "salty", "silky", "smooth",
    "soft", "spiced", "sticky", "sugary", "sunny", "swirled", "tangy", "toasty", "tropical",
    "velvety", "warm", "whipped", "wild", "zesty",
];

const NOUNS: &[&str] = &[
    "almond",
    "apricot",
    "banana",
    "berry",
    "biscuit",
    "blueberry",
    "brownie",
    "butterscotch",
    "caramel",
    "cashew",
    "cherry",
    "chestnut",
    "chocolate",
    "cinnamon",
    "clementine",
    "cobbler",
    "coconut",
    "coffee",
    "cookie",
    "cranberry",
    "custard",
    "espresso",
    "fig",
    "fudge",
    "ganache",
    "ginger",
    "guava",
    "hazelnut",
    "honeycomb",
    "lemon",
    "licorice",
    "lychee",
    "macaron",
    "mango",
    "maple",
    "marshmallow",
    "meringue",
    "mocha",
    "nougat",
    "orange",
    "passionfruit",
    "peach",
    "peanut",
    "pecan",
    "peppermint",
    "pistachio",
    "plum",
    "praline",
    "pumpkin",
    "raspberry",
    "rhubarb",
    "sorbet",
    "strawberry",
    "sundae",
    "toffee",
    "truffle",
    "vanilla",
    "waffle",
    "walnut",
];

/// Length of the random suffix appended to each name.
const SUFFIX_LEN: usize = 3;

/// Alphabet used for the random suffix (lowercase ASCII + digits).
const SUFFIX_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generate a unique human-readable name like `fuzzy_pistachio_a3f`.
#[must_use]
pub fn generate_name() -> String {
    let mut rng = rand::rng();

    let adj = ADJECTIVES[rng.random_range(0..ADJECTIVES.len())];
    let noun = NOUNS[rng.random_range(0..NOUNS.len())];

    let suffix: String = (0..SUFFIX_LEN)
        .map(|_| {
            let idx = rng.random_range(0..SUFFIX_CHARS.len());
            SUFFIX_CHARS[idx] as char
        })
        .collect();

    format!("{adj}_{noun}_{suffix}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn format_is_adj_noun_suffix() {
        let name = generate_name();
        let parts: Vec<&str> = name.split('_').collect();
        assert_eq!(parts.len(), 3, "expected adj_noun_suffix: {name}");
        assert!(
            ADJECTIVES.contains(&parts[0]),
            "bad adjective: {}",
            parts[0]
        );
        assert!(NOUNS.contains(&parts[1]), "bad noun: {}", parts[1]);
        assert_eq!(parts[2].len(), SUFFIX_LEN);
        assert!(
            parts[2]
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
    }

    #[test]
    fn names_are_unique() {
        let names: HashSet<String> = (0..100).map(|_| generate_name()).collect();
        // With 52 * 58 * 36^3 ≈ 140M possibilities, 100 names should all be unique.
        assert_eq!(names.len(), 100);
    }

    #[test]
    fn all_chars_are_ascii_alphanumeric_or_underscore() {
        for _ in 0..50 {
            let name = generate_name();
            assert!(
                name.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "unexpected char in: {name}"
            );
        }
    }
}
