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
use std::sync::OnceLock;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use log::{Level, LevelFilter, Log, Metadata, Record};

/// Depth of the log queue. Under a burst beyond this, records are dropped
/// (diagnostics, not data) — the alternative is the old behavior, where a
/// blocking `/dev/console` write on the effectively-single-worker runtime
/// stalled the socket read loop and the 20 ms heartbeat that detects dead
/// connections.
const LOG_QUEUE_DEPTH: usize = 1024;

static LOG_TX: OnceLock<SyncSender<String>> = OnceLock::new();

struct ConsoleLogger;

impl Log for ConsoleLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        let line = format!("agent [{}]: {}", short_level(record.level()), record.args());
        // Hand off to the writer thread — the open+write against a slow
        // serial console must never run on an async worker. try_send: on a
        // full queue, dropping a log line beats stalling the heartbeat.
        if let Some(tx) = LOG_TX.get() {
            let _ = tx.try_send(line);
        }
    }

    fn flush(&self) {}
}

/// Dedicated writer thread: drains the queue and does the blocking writes.
fn writer_loop(rx: Receiver<String>) {
    while let Ok(line) = rx.recv() {
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
    let (tx, rx) = sync_channel::<String>(LOG_QUEUE_DEPTH);
    if LOG_TX.set(tx).is_ok() {
        let _ = std::thread::Builder::new()
            .name("agent-console-log".into())
            .spawn(move || writer_loop(rx));
    }
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(LevelFilter::Info);
}
