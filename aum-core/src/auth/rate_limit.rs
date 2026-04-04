//! In-memory per-IP login rate limiter with IPv6 /64 bucketing.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::error::{AuthError, AuthResult};

/// Default maximum failed attempts before rate-limiting kicks in.
const DEFAULT_MAX_FAILURES: u32 = 5;

/// Default sliding window duration.
const DEFAULT_WINDOW: Duration = Duration::from_secs(900); // 15 minutes

/// When the failure map exceeds this many keys, stale entries are pruned.
const CLEANUP_THRESHOLD: usize = 1000;

/// Per-IP sliding-window rate limiter for login attempts.
pub struct LoginRateLimiter {
    failures: Mutex<HashMap<String, Vec<Instant>>>,
    max_failures: u32,
    window: Duration,
}

impl LoginRateLimiter {
    /// Create a rate limiter with the default thresholds (5 failures / 15 minutes).
    #[must_use]
    pub fn new() -> Self {
        Self {
            failures: Mutex::new(HashMap::new()),
            max_failures: DEFAULT_MAX_FAILURES,
            window: DEFAULT_WINDOW,
        }
    }

    /// Create a rate limiter with custom thresholds.
    #[must_use]
    pub fn with_config(max_failures: u32, window: Duration) -> Self {
        Self {
            failures: Mutex::new(HashMap::new()),
            max_failures,
            window,
        }
    }

    /// Check whether the given IP is rate-limited.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::RateLimited`] if the IP has exceeded the failure threshold.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn check(&self, client_ip: &str) -> AuthResult<()> {
        let key = rate_limit_key(client_ip);
        let now = Instant::now();
        let cutoff = now.checked_sub(self.window).unwrap_or(now);

        #[allow(clippy::expect_used)]
        let mut map = self.failures.lock().expect("rate limiter lock poisoned");

        // Periodic cleanup of stale entries.
        if map.len() > CLEANUP_THRESHOLD {
            map.retain(|_, timestamps| timestamps.last().is_some_and(|&t| t > cutoff));
        }

        if let Some(timestamps) = map.get_mut(&key) {
            timestamps.retain(|&t| t > cutoff);
            if timestamps.is_empty() {
                map.remove(&key);
            } else if timestamps.len() >= self.max_failures as usize {
                return Err(AuthError::RateLimited);
            }
        }

        Ok(())
    }

    /// Record a failed login attempt for the given IP.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn record_failure(&self, client_ip: &str) {
        let key = rate_limit_key(client_ip);
        #[allow(clippy::expect_used)]
        let mut map = self.failures.lock().expect("rate limiter lock poisoned");
        map.entry(key).or_default().push(Instant::now());
    }
}

impl Default for LoginRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute the rate-limit bucket key for an IP address.
///
/// IPv4 addresses are keyed individually. IPv6 addresses are keyed by their /64
/// prefix, since a single host typically owns the entire /64.
fn rate_limit_key(client_ip: &str) -> String {
    let Ok(addr) = client_ip.parse::<IpAddr>() else {
        return client_ip.to_owned();
    };
    match addr {
        IpAddr::V4(_) => client_ip.to_owned(),
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let masked = bits & !((1u128 << 64) - 1); // zero lower 64 bits
            let masked_addr: std::net::Ipv6Addr = masked.into();
            format!("{masked_addr}/64")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_key_ipv4() {
        assert_eq!(rate_limit_key("192.168.1.1"), "192.168.1.1");
    }

    #[test]
    fn test_rate_limit_key_ipv6_masks_to_64() {
        // Two addresses in the same /64 should map to the same key.
        let key1 = rate_limit_key("2001:db8::1");
        let key2 = rate_limit_key("2001:db8::ffff");
        assert_eq!(key1, key2);
        assert!(key1.ends_with("/64"));
    }

    #[test]
    fn test_rate_limit_key_different_ipv6_subnets() {
        let key1 = rate_limit_key("2001:db8:1::1");
        let key2 = rate_limit_key("2001:db8:2::1");
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_rate_limit_key_invalid_ip() {
        assert_eq!(rate_limit_key("not-an-ip"), "not-an-ip");
    }

    #[test]
    fn test_allows_under_threshold() -> anyhow::Result<()> {
        let limiter = LoginRateLimiter::new();
        for _ in 0..4 {
            limiter.record_failure("10.0.0.1");
        }
        limiter.check("10.0.0.1")?; // 4 failures, should still be allowed
        Ok(())
    }

    #[test]
    fn test_blocks_at_threshold() {
        let limiter = LoginRateLimiter::new();
        for _ in 0..5 {
            limiter.record_failure("10.0.0.1");
        }
        assert!(matches!(
            limiter.check("10.0.0.1"),
            Err(AuthError::RateLimited)
        ));
    }

    #[test]
    fn test_different_ips_independent() -> anyhow::Result<()> {
        let limiter = LoginRateLimiter::new();
        for _ in 0..5 {
            limiter.record_failure("10.0.0.1");
        }
        limiter.check("10.0.0.2")?; // different IP, not rate-limited
        Ok(())
    }

    #[test]
    fn test_ipv6_same_subnet_shares_bucket() {
        let limiter = LoginRateLimiter::new();
        // Spread failures across different addresses in the same /64.
        for i in 1..=5 {
            limiter.record_failure(&format!("2001:db8::{i}"));
        }
        assert!(matches!(
            limiter.check("2001:db8::99"),
            Err(AuthError::RateLimited)
        ));
    }

    #[test]
    fn test_window_expiry() -> anyhow::Result<()> {
        // Use a very short window so failures expire immediately.
        let limiter = LoginRateLimiter::with_config(2, Duration::from_millis(1));
        limiter.record_failure("10.0.0.1");
        limiter.record_failure("10.0.0.1");

        // Wait for the window to expire.
        std::thread::sleep(Duration::from_millis(5));
        limiter.check("10.0.0.1")?;

        Ok(())
    }
}
