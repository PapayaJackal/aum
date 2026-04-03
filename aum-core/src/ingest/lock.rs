//! flock-based ingest lock for crash detection.
//!
//! Each ingest job acquires an exclusive advisory lock on a file keyed by the
//! canonical source directory.  If the process crashes or is killed the OS
//! automatically releases the lock, allowing [`is_locked`] to detect the stale
//! job so it can be marked as interrupted.
//!
//! Only one ingest job can run per source directory at a time.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use fs2::FileExt as _;
use tracing::{debug, warn};

/// Compute the lock file path for a given source directory.
///
/// Uses a truncated blake3 hash of the canonical path to avoid filesystem
/// issues with long or special-character paths.
fn lock_path(lock_dir: &Path, source_dir: &Path) -> PathBuf {
    let hash = blake3::hash(source_dir.as_os_str().as_encoded_bytes());
    lock_dir.join(format!("ingest.{}.lock", &hash.to_hex()[..16]))
}

/// An exclusive advisory lock backed by `{lock_dir}/ingest.{hash}.lock`.
///
/// The lock is held for the lifetime of this struct.  Dropping it releases the
/// lock and removes the lock file.
pub struct IngestLock {
    path: PathBuf,
    file: File,
}

impl IngestLock {
    /// Try to acquire an exclusive lock for `source_dir`.
    ///
    /// On success returns `Ok(Some(lock))`.  If another process already holds
    /// the lock returns `Ok(None)`.  I/O errors are propagated.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the lock directory cannot be created or the lock
    /// file cannot be opened/written.
    pub fn try_acquire(lock_dir: &Path, source_dir: &Path) -> io::Result<Option<Self>> {
        fs::create_dir_all(lock_dir)?;
        let path = lock_path(lock_dir, source_dir);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        if file.try_lock_exclusive().is_err() {
            return Ok(None);
        }

        // Write holder info for diagnostics.
        file.set_len(0)?;
        let mut f = &file;
        writeln!(f, "pid={}", std::process::id())?;
        writeln!(f, "dir={}", source_dir.display())?;

        debug!(path = %path.display(), "acquired ingest lock");
        Ok(Some(Self { path, file }))
    }

    /// Read the PID from a lock file, if present and parseable.
    #[must_use]
    pub fn read_holder_pid(lock_dir: &Path, source_dir: &Path) -> Option<u32> {
        let path = lock_path(lock_dir, source_dir);
        let text = fs::read_to_string(&path).ok()?;
        for line in text.lines() {
            if let Some(val) = line.strip_prefix("pid=") {
                return val.trim().parse().ok();
            }
        }
        None
    }
}

impl Drop for IngestLock {
    fn drop(&mut self) {
        if let Err(e) = self.file.unlock() {
            warn!(error = %e, path = %self.path.display(), "failed to unlock ingest lock");
        }
        if let Err(e) = fs::remove_file(&self.path)
            && e.kind() != io::ErrorKind::NotFound
        {
            warn!(error = %e, path = %self.path.display(), "failed to remove lock file");
        }
        debug!(path = %self.path.display(), "released ingest lock");
    }
}

/// Check whether a lock for the given source directory is currently held by
/// another process.
///
/// Returns `true` if the lock file exists and cannot be exclusively locked
/// (i.e. another live process holds it).
#[must_use]
pub fn is_locked(lock_dir: &Path, source_dir: &Path) -> bool {
    let path = lock_path(lock_dir, source_dir);
    let Ok(file) = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&path)
    else {
        return false;
    };
    file.try_lock_exclusive().is_err()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn test_acquire_and_release() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let lock_dir = dir.path().join("locks");
        let source = dir.path().join("docs");
        fs::create_dir_all(&source)?;

        let lock = IngestLock::try_acquire(&lock_dir, &source)?;
        assert!(lock.is_some(), "should acquire lock");

        // While held, a second acquire should fail.
        let second = IngestLock::try_acquire(&lock_dir, &source)?;
        assert!(second.is_none(), "should not acquire while held");

        // After dropping, should be acquirable again.
        drop(lock);
        let third = IngestLock::try_acquire(&lock_dir, &source)?;
        assert!(third.is_some(), "should acquire after release");
        Ok(())
    }

    #[test]
    fn test_independent_directories() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let lock_dir = dir.path().join("locks");
        let source_a = dir.path().join("a");
        let source_b = dir.path().join("b");
        fs::create_dir_all(&source_a)?;
        fs::create_dir_all(&source_b)?;

        let lock_a = IngestLock::try_acquire(&lock_dir, &source_a)?;
        assert!(lock_a.is_some());
        let lock_b = IngestLock::try_acquire(&lock_dir, &source_b)?;
        assert!(lock_b.is_some(), "different dirs should not conflict");
        Ok(())
    }

    #[test]
    fn test_is_locked() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let lock_dir = dir.path().join("locks");
        let source = dir.path().join("docs");
        fs::create_dir_all(&source)?;

        assert!(!is_locked(&lock_dir, &source));

        let lock = IngestLock::try_acquire(&lock_dir, &source)?;
        assert!(lock.is_some());
        assert!(is_locked(&lock_dir, &source));

        drop(lock);
        assert!(!is_locked(&lock_dir, &source));
        Ok(())
    }

    #[test]
    fn test_holder_pid() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let lock_dir = dir.path().join("locks");
        let source = dir.path().join("docs");
        fs::create_dir_all(&source)?;

        let _lock = IngestLock::try_acquire(&lock_dir, &source)?;
        let pid = IngestLock::read_holder_pid(&lock_dir, &source);
        assert_eq!(pid, Some(std::process::id()));
        Ok(())
    }

    #[test]
    fn test_process_death_releases_lock() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let lock_dir = dir.path().join("locks");
        let source = dir.path().join("docs");
        fs::create_dir_all(&source)?;
        fs::create_dir_all(&lock_dir)?;

        let lock_path = lock_path(&lock_dir, &source);

        // Spawn a child that acquires the lock and then exits.
        let status = Command::new("bash")
            .arg("-c")
            .arg(format!(
                "exec 9>>'{}' && flock -xn 9 && echo pid=$$ >&9",
                lock_path.display()
            ))
            .status()?;
        assert!(status.success());

        // Child has exited — the OS released the flock.
        assert!(
            !is_locked(&lock_dir, &source),
            "lock should be released after process death"
        );

        // We should be able to acquire it.
        let lock = IngestLock::try_acquire(&lock_dir, &source)?;
        assert!(lock.is_some());
        Ok(())
    }
}
