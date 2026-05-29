//! pty-bridge: run a command under a PTY, bridging the PTY to stdin/stdout.
//!
//! This is the in-guest "user binary that does PTY work" — cradle itself
//! stays pipes-only; all terminal semantics (ONLCR line discipline,
//! window size, signal generation) live here, inside the guest, exactly
//! where a PTY can do them. The cradle CLI invokes it as the step's
//! command:
//!
//!   pty-bridge --rows R --cols C -- <argv...>
//!
//! Wire contract over the agent's pipes:
//!
//! - **stdout (fd 1)**: raw PTY master output — the command's terminal
//!   output, byte for byte. The CLI feeds this straight into its vt100
//!   parser. No framing.
//! - **stdin (fd 0)**: a framed control stream from the CLI:
//!
//!       [type: u8][len: u32 big-endian][payload (len bytes)]
//!
//!       type 0 = input bytes  → written verbatim to the PTY master
//!                               (keystrokes, including ^C/^D/^Z which the
//!                               line discipline turns into signals/EOF)
//!       type 1 = window size  → payload = [rows: u16 BE][cols: u16 BE]
//!                               → TIOCSWINSZ on the master, which makes
//!                               the kernel SIGWINCH the foreground group
//!
//! There is no handshake, no auth, no negotiation — cradle's transport is
//! already a trusted byte pipe. The bridge exits when the command exits;
//! that closes stdout, the agent observes EOF/ProcessExit, and the host
//! snapshots. Exit codes are intentionally not propagated.

use std::io::{Read, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::ptr;
use std::thread;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let parsed = match parse_args(&args) {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("pty-bridge: {msg}");
            eprintln!("usage: pty-bridge --rows R --cols C -- <command> [args...]");
            std::process::exit(2);
        }
    };

    // Allocate the PTY pair with the initial window size baked in, so the
    // command sees a correct TIOCGWINSZ from its very first query (no
    // startup round-trip with the CLI to learn the size).
    let mut master: RawFd = -1;
    let mut slave: RawFd = -1;
    let initial_ws = libc::winsize {
        ws_row: parsed.rows,
        ws_col: parsed.cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: out-params are valid locals; termios is null (defaults, which
    // include OPOST|ONLCR — that's the whole point), winsize is a valid ref.
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            ptr::null_mut(),
            ptr::null(),
            &initial_ws,
        )
    };
    if rc != 0 {
        eprintln!(
            "pty-bridge: openpty failed: {}",
            std::io::Error::last_os_error()
        );
        std::process::exit(1);
    }

    // Spawn the command on the slave end. Crucially we do NOT hand the
    // slave to `Command` as Stdio — those fds would stay owned by the
    // `Command` struct for the rest of `main`, keeping the slave open in
    // the parent so the master never EOFs after the child exits (the bug
    // that hung the session). Instead we set up the child's 0/1/2 from the
    // slave inside `pre_exec` (default `inherit` stdio is overwritten by
    // the dup2s), and the parent keeps only the master.
    let mut command = Command::new(&parsed.command[0]);
    command.args(&parsed.command[1..]);
    let slave_raw = slave;
    // SAFETY: only async-signal-safe libc calls between fork and exec.
    unsafe {
        command.pre_exec(move || {
            // New session, so the command becomes a session/group leader.
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            // Put the PTY slave on stdin/stdout/stderr (overwriting the
            // inherited agent pipes), then drop the now-redundant slave fd.
            for target in 0..3 {
                if libc::dup2(slave_raw, target) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            if slave_raw > 2 {
                libc::close(slave_raw);
            }
            // Make the slave (now fd 0) the controlling terminal, so job
            // control and ^C→SIGINT work for the command.
            if libc::ioctl(0, libc::TIOCSCTTY, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = match command.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("pty-bridge: spawn {:?} failed: {e}", parsed.command);
            std::process::exit(1);
        }
    };

    // Parent keeps only the master. Closing the slave here means that once
    // the child exits (closing its 0/1/2), NO slave fd remains open, so the
    // master read returns EIO and the output pump finishes.
    unsafe {
        libc::close(slave);
    }

    // Two views of the master: one for the output pump (read), one for the
    // input pump (write). Separate fds = clean per-thread ownership.
    let master_r = master;
    let master_w = dup_or_exit(master);

    // Output pump: PTY master → our stdout, verbatim. Ends when the master
    // returns EOF/EIO (which Linux does once the child closes the slave).
    let reader = thread::spawn(move || {
        let mut master_file = unsafe { std::fs::File::from_raw_fd(master_r) };
        let mut stdout = std::io::stdout().lock();
        let mut buf = [0u8; 16 * 1024];
        loop {
            match master_file.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if stdout.write_all(&buf[..n]).is_err() {
                        break;
                    }
                    let _ = stdout.flush();
                }
            }
        }
    });

    // Input pump: framed stdin → PTY master / winsize. Runs until stdin
    // EOF or a short read; abandoned (process exit) if the command exits
    // first while it's blocked reading.
    thread::spawn(move || {
        let mut master_file = unsafe { std::fs::File::from_raw_fd(master_w) };
        let mut stdin = std::io::stdin().lock();
        loop {
            let mut header = [0u8; 5];
            if stdin.read_exact(&mut header).is_err() {
                break;
            }
            let frame_type = header[0];
            let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            if stdin.read_exact(&mut payload).is_err() {
                break;
            }
            match frame_type {
                0 => {
                    if master_file.write_all(&payload).is_err() {
                        break;
                    }
                }
                1 => {
                    if payload.len() >= 4 {
                        let rows = u16::from_be_bytes([payload[0], payload[1]]);
                        let cols = u16::from_be_bytes([payload[2], payload[3]]);
                        let ws = libc::winsize {
                            ws_row: rows,
                            ws_col: cols,
                            ws_xpixel: 0,
                            ws_ypixel: 0,
                        };
                        // SAFETY: master_w is a valid fd; ws is a valid ref.
                        unsafe {
                            libc::ioctl(master_w, libc::TIOCSWINSZ, &ws);
                        }
                    }
                }
                // Unknown frame types are ignored (forward-compat).
                _ => {}
            }
        }
    });

    // Wait for the command, then drain any remaining PTY output before we
    // exit (joining the reader guarantees the last bytes reach stdout).
    // The input pump thread is left blocked on stdin; process exit reaps it.
    let _ = child.wait();
    let _ = reader.join();
}

struct Args {
    rows: u16,
    cols: u16,
    command: Vec<String>,
}

fn parse_args(args: &[String]) -> Result<Args, String> {
    let mut rows: u16 = 24;
    let mut cols: u16 = 80;
    let mut i = 1;
    let mut command: Vec<String> = Vec::new();
    while i < args.len() {
        match args[i].as_str() {
            "--rows" => {
                i += 1;
                rows = args
                    .get(i)
                    .ok_or("--rows needs a value")?
                    .parse()
                    .map_err(|_| "--rows must be a u16")?;
            }
            "--cols" => {
                i += 1;
                cols = args
                    .get(i)
                    .ok_or("--cols needs a value")?
                    .parse()
                    .map_err(|_| "--cols must be a u16")?;
            }
            "--" => {
                command = args[i + 1..].to_vec();
                break;
            }
            other => return Err(format!("unexpected argument: {other}")),
        }
        i += 1;
    }
    if command.is_empty() {
        return Err("no command given after --".into());
    }
    Ok(Args {
        rows: rows.max(1),
        cols: cols.max(1),
        command,
    })
}

/// `dup(2)` that exits the process on failure — used only at startup for
/// fds we cannot proceed without.
fn dup_or_exit(fd: RawFd) -> RawFd {
    let new = unsafe { libc::dup(fd) };
    if new == -1 {
        eprintln!("pty-bridge: dup failed: {}", std::io::Error::last_os_error());
        std::process::exit(1);
    }
    new
}
