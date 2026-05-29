//! `log` facade backend that writes every record to `/dev/console`.
//!
//! Why this instead of `eprintln!`: the agent is started by stage-1 initrd
//! (via `boot.initrd.postMountCommands`), inheriting that init's stderr fd.
//! After `switch_root`, the file behind that fd may be gone — stderr writes
//! silently disappear. `/dev/console` is provided by devtmpfs, which the
//! kernel re-creates fresh in the new rootfs, so opening it lazily on each
//! log call always lands on whatever the current real console is.
//!
//! Output ends up on the host's serial-console reader (we boot with
//! `console=ttyS0`), so log lines reach the cradle host without going
//! through vsock — invaluable for debugging snapshot/restore issues.

use std::io::Write;

use log::{Level, LevelFilter, Log, Metadata, Record};

struct ConsoleLogger;

impl Log for ConsoleLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        let line = format!("agent [{}]: {}", short_level(record.level()), record.args());
        // Belt-and-suspenders: write to both /dev/console (forwarded to serial
        // regardless of console_loglevel) AND /dev/kmsg (kernel ring buffer,
        // visible via `dmesg` even if serial routing is broken). If the agent
        // process is alive at all, one of these will show output.
        if let Ok(mut f) = std::fs::OpenOptions::new().write(true).open("/dev/console") {
            let _ = writeln!(f, "{line}");
        }
        if let Ok(mut f) = std::fs::OpenOptions::new().write(true).open("/dev/kmsg") {
            // Prefix `<3>` (KERN_ERR) so the message bypasses default
            // console_loglevel filtering and shows up on serial too.
            let _ = writeln!(f, "<3>{line}");
        }
    }

    fn flush(&self) {}
}

fn short_level(l: Level) -> &'static str {
    match l {
        Level::Error => "ERR",
        Level::Warn => "WRN",
        Level::Info => "INF",
        Level::Debug => "DBG",
        Level::Trace => "TRC",
    }
}

static LOGGER: ConsoleLogger = ConsoleLogger;

/// Initialize the global `log` backend. Idempotent in practice — calling
/// twice is an error per the log crate, so callers should only invoke this
/// once at startup.
pub fn init() {
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(LevelFilter::Info);
}
