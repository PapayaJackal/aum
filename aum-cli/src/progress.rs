//! Global [`MultiProgress`] instance and a [`MakeWriter`] bridge that routes
//! tracing log events through indicatif so they appear above the progress bar
//! without corrupting it.

use std::io::{self, Write as _};
use std::sync::OnceLock;

use indicatif::MultiProgress;
use tracing_subscriber::fmt::MakeWriter;

static MULTI_PROGRESS: OnceLock<MultiProgress> = OnceLock::new();

/// Initialise the global [`MultiProgress`].
///
/// Must be called once, before [`get`] or [`IndicatifWriter`] are used.
pub fn init() {
    MULTI_PROGRESS.get_or_init(MultiProgress::new);
}

/// Return a reference to the global [`MultiProgress`].
///
/// # Panics
///
/// Panics if [`init`] has not been called.
pub fn get() -> &'static MultiProgress {
    #[allow(clippy::expect_used)]
    MULTI_PROGRESS.get().expect("progress::init() not called")
}

// ---------------------------------------------------------------------------
// MakeWriter bridge
// ---------------------------------------------------------------------------

/// A [`MakeWriter`] that routes each log line through the global
/// [`MultiProgress`], preventing tracing output from corrupting progress bars.
///
/// When no progress bars are active the behaviour is identical to writing
/// directly to stderr.
pub struct IndicatifWriter;

impl<'a> MakeWriter<'a> for IndicatifWriter {
    type Writer = LineWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LineWriter { buf: Vec::new() }
    }
}

/// Buffers one log event and flushes it through [`MultiProgress::println`] on drop.
pub struct LineWriter {
    buf: Vec<u8>,
}

impl io::Write for LineWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit();
        Ok(())
    }
}

impl Drop for LineWriter {
    fn drop(&mut self) {
        self.emit();
    }
}

impl LineWriter {
    fn emit(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let raw = std::mem::take(&mut self.buf);
        let msg = String::from_utf8_lossy(&raw);
        let msg = msg.trim_end_matches('\n');
        if msg.is_empty() {
            return;
        }
        if let Some(mp) = MULTI_PROGRESS.get() {
            for line in msg.split('\n') {
                mp.println(line).ok();
            }
        } else {
            // Fallback: not yet initialised, write directly.
            let _ = writeln!(io::stderr(), "{msg}");
        }
    }
}
