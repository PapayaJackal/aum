//! Password validation, hashing (Argon2id), and generation.

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use rand::seq::{IndexedRandom, SliceRandom};

use super::error::{AuthError, AuthResult};

/// Validate a password against the policy requirements.
///
/// Requirements: minimum length, at least one lowercase, uppercase, digit, and special character.
///
/// # Errors
///
/// Returns [`AuthError::PasswordPolicy`] listing the unmet requirements.
pub fn validate_password(password: &str, min_length: u32) -> AuthResult<()> {
    let mut violations = Vec::new();
    if password.len() < min_length as usize {
        violations.push(format!("at least {min_length} characters"));
    }
    if !password.bytes().any(|b| b.is_ascii_lowercase()) {
        violations.push("at least one lowercase letter".into());
    }
    if !password.bytes().any(|b| b.is_ascii_uppercase()) {
        violations.push("at least one uppercase letter".into());
    }
    if !password.bytes().any(|b| b.is_ascii_digit()) {
        violations.push("at least one digit".into());
    }
    if !password.bytes().any(|b| !b.is_ascii_alphanumeric()) {
        violations.push("at least one special character".into());
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(AuthError::PasswordPolicy(format!(
            "Password must contain: {}",
            violations.join(", ")
        )))
    }
}

/// Hash a password using Argon2id with default parameters and a random salt.
///
/// # Errors
///
/// Returns [`AuthError::PasswordPolicy`] if hashing fails.
pub fn hash_password(password: &str) -> AuthResult<String> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| AuthError::PasswordPolicy(format!("failed to hash password: {e}")))?;
    Ok(hash.to_string())
}

/// Verify a password against a stored Argon2 PHC-format hash.
///
/// Returns `true` if the password matches, `false` otherwise.
///
/// # Errors
///
/// Returns [`AuthError::PasswordPolicy`] if the hash string is malformed.
pub fn verify_password(password: &str, hash: &str) -> AuthResult<bool> {
    let parsed = PasswordHash::new(hash)
        .map_err(|e| AuthError::PasswordPolicy(format!("invalid password hash: {e}")))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

/// Check whether a stored hash should be re-hashed with current default parameters.
///
/// Returns `true` if the hash uses different Argon2 parameters than the current defaults.
/// Compares the PHC-format parameter string directly instead of hashing a probe value.
#[must_use]
pub fn needs_rehash(hash: &str) -> bool {
    use std::sync::LazyLock;

    /// (algorithm, version, params) strings for the current Argon2 defaults, computed once.
    static DEFAULT_PARAMS: LazyLock<(String, String, String)> = LazyLock::new(|| {
        let salt = SaltString::generate(&mut OsRng);
        #[allow(clippy::expect_used)] // one-time init with a constant input; failure is a build bug
        let h = Argon2::default()
            .hash_password(b"probe", &salt)
            .expect("hashing a probe value should not fail");
        (
            h.algorithm.to_string(),
            h.version.map_or(String::new(), |v| v.to_string()),
            h.params.to_string(),
        )
    });

    let Ok(parsed) = PasswordHash::new(hash) else {
        return true;
    };

    let (ref algo, ref ver, ref params) = *DEFAULT_PARAMS;
    parsed.algorithm.as_str() != algo
        || parsed.version.map_or(String::new(), |v| v.to_string()) != *ver
        || parsed.params.to_string() != *params
}

/// Generate a random password meeting the policy requirements.
///
/// The password is `length` characters drawn from `a-zA-Z0-9!@#$%^&*`, with at least one
/// character from each category guaranteed.
///
/// # Panics
///
/// Panics if `length < 4` (need at least one character from each category).
#[must_use]
pub fn generate_password(length: usize) -> String {
    const LOWER: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    const UPPER: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const DIGITS: &[u8] = b"0123456789";
    const SPECIAL: &[u8] = b"!@#$%^&*";
    const ALL: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*";

    // Helper: pick a random byte from a non-empty slice.
    fn pick(rng: &mut impl rand::Rng, chars: &[u8]) -> u8 {
        #[allow(clippy::expect_used)] // slices are compile-time non-empty constants
        *chars.choose(rng).expect("non-empty")
    }

    assert!(length >= 4, "password length must be at least 4");

    let mut rng = rand::rng();

    loop {
        let mut password = Vec::with_capacity(length);

        // Guarantee one from each category.
        password.push(pick(&mut rng, LOWER));
        password.push(pick(&mut rng, UPPER));
        password.push(pick(&mut rng, DIGITS));
        password.push(pick(&mut rng, SPECIAL));

        // Fill the rest randomly.
        for _ in 4..length {
            password.push(pick(&mut rng, ALL));
        }

        // Shuffle to avoid predictable positions.
        password.shuffle(&mut rng);

        // All bytes in ALL are ASCII, so this is always valid UTF-8.
        let Ok(s) = String::from_utf8(password) else {
            continue;
        };
        if validate_password(&s, 1).is_ok() {
            return s;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_password_ok() -> anyhow::Result<()> {
        validate_password("Abcdef1!", 8)?;
        Ok(())
    }

    /// Extract the error message from a password validation failure, or fail the test.
    fn policy_err(result: AuthResult<()>) -> anyhow::Result<String> {
        let Err(e) = result else {
            anyhow::bail!("expected password policy error, got Ok");
        };
        Ok(e.to_string())
    }

    #[test]
    fn test_validate_password_too_short() -> anyhow::Result<()> {
        let msg = policy_err(validate_password("Ab1!", 8))?;
        assert!(msg.contains("at least 8 characters"));
        Ok(())
    }

    #[test]
    fn test_validate_password_missing_lowercase() -> anyhow::Result<()> {
        let msg = policy_err(validate_password("ABCDEF1!", 8))?;
        assert!(msg.contains("lowercase"));
        Ok(())
    }

    #[test]
    fn test_validate_password_missing_uppercase() -> anyhow::Result<()> {
        let msg = policy_err(validate_password("abcdef1!", 8))?;
        assert!(msg.contains("uppercase"));
        Ok(())
    }

    #[test]
    fn test_validate_password_missing_digit() -> anyhow::Result<()> {
        let msg = policy_err(validate_password("Abcdefg!", 8))?;
        assert!(msg.contains("digit"));
        Ok(())
    }

    #[test]
    fn test_validate_password_missing_special() -> anyhow::Result<()> {
        let msg = policy_err(validate_password("Abcdefg1", 8))?;
        assert!(msg.contains("special"));
        Ok(())
    }

    #[test]
    fn test_validate_password_multiple_violations() -> anyhow::Result<()> {
        let msg = policy_err(validate_password("abc", 8))?;
        assert!(msg.contains("at least 8 characters"));
        assert!(msg.contains("uppercase"));
        assert!(msg.contains("digit"));
        assert!(msg.contains("special"));
        Ok(())
    }

    #[test]
    fn test_hash_and_verify_roundtrip() -> anyhow::Result<()> {
        let hash = hash_password("TestPass1!")?;
        assert!(verify_password("TestPass1!", &hash)?);
        assert!(!verify_password("WrongPass1!", &hash)?);
        Ok(())
    }

    #[test]
    fn test_needs_rehash_current_params() -> anyhow::Result<()> {
        let hash = hash_password("TestPass1!")?;
        assert!(!needs_rehash(&hash));
        Ok(())
    }

    #[test]
    fn test_needs_rehash_invalid_hash() {
        assert!(needs_rehash("not-a-valid-hash"));
    }

    #[test]
    fn test_generate_password_meets_policy() {
        for _ in 0..20 {
            let pw = generate_password(20);
            assert_eq!(pw.len(), 20);
            assert!(
                validate_password(&pw, 8).is_ok(),
                "generated password failed policy: {pw}"
            );
        }
    }

    #[test]
    fn test_generate_password_minimum_length() {
        let pw = generate_password(4);
        assert_eq!(pw.len(), 4);
        assert!(validate_password(&pw, 1).is_ok());
    }
}
