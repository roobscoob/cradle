mod cgroup;
mod console_log;

use std::os::unix::process::ExitStatusExt;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agent_protocol::{AgentMessage, ExitResult, HostMessage, HostMessageDecoder, Stream};
use log::{error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStdin, Command};
use tokio::sync::mpsc::{self, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_vsock::{VsockAddr, VsockStream};

/// Well-known vsock CID of the host. The agent dials *out* to the host
/// (the host is the listener now) — see the lifecycle comment on
/// `AgentMessage` in agent-protocol. Values are defined once in
/// agent-protocol so host and agent can't drift.
const HOST_CID: u32 = agent_protocol::VSOCK_HOST_CID;
/// Port the host listens on (host-side it's a UDS at `<uds>_<port>`; the
/// guest connects to (HOST_CID, HOST_PORT) and firecracker forwards).
const HOST_PORT: u32 = agent_protocol::VSOCK_HOST_PORT;
/// How often the agent writes a `Heartbeat` on the connection. A failed
/// heartbeat write is how the agent detects that a snapshot/restore killed
/// the connection — reads don't wake on the guest kernel's TRANSPORT_RESET,
/// but writes return EPIPE. Tight enough that post-restore detection +
/// reconnect is fast; cheap enough (50 tiny writes/sec) to be free.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(20);
/// Backoff between failed dial attempts. The agent dials *after* detecting
/// a dead connection (which means the kernel already processed the reset),
/// so the first dial usually succeeds; this is just a gentle safety net,
/// deliberately NOT a tight hammer (which would starve the guest's vsock
/// reset processing — see git history).
const DIAL_RETRY: Duration = Duration::from_millis(20);
/// Bump on every build of the agent — visible in `AGENT-DBG-{VERSION}: …`
/// lines on the serial log so we can verify the new binary is actually
/// what's running (rules out stale nix builds).
const DBG_VERSION: &str = "v21";
const THREAD_HB_PERIOD: Duration = Duration::from_secs(5);
const READ_BUF: usize = 8 * 1024;
/// Outbound channel depth. Bounded so per-eval forwarders backpressure into
/// the child's pipe when the host stalls reading the socket.
const OUTBOUND_DEPTH: usize = 32;
/// Cap on stdin bytes buffered toward a child that isn't reading them. The
/// buffer sits behind unbounded channels (so Kill stays responsive even when
/// the child's stdin pipe is full); without a byte cap, a host streaming
/// stdin at a non-reader would grow agent memory until the guest OOMs and
/// the microVM dies mid-step. Past the cap, stdin is dropped with a log.
const STDIN_BUF_CAP: usize = 8 * 1024 * 1024;
/// A session that ends faster than this was born dead (connected during the
/// post-restore TRANSPORT_RESET window and failed on the first write) — back
/// off before redialing instead of hammering the resetting vsock device.
const MIN_SESSION_FOR_IMMEDIATE_REDIAL: Duration = Duration::from_millis(100);

/// In-process messages sent from the per-session read loop to the running
/// `handle_eval` task. Stays out of the wire protocol.
enum EvalControl {
    Stdin(Vec<u8>),
    StdinClose,
    Kill,
}

fn main() {
    // Pre-tokio: banner + panic hook + a low-frequency thread heartbeat that
    // proves the process is alive even if the tokio runtime is wedged.
    diag_banner();
    install_panic_hook();
    std::thread::Builder::new()
        .name("agent-thread-hb".into())
        .spawn(thread_heartbeat)
        .expect("spawn thread-hb");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(async_main());
}

async fn async_main() {
    console_log::init();
    info!("starting up");

    // systemd's Delegate=yes guarantees our cgroup exists by the time the
    // service is started. Sanity-check; warn rather than fail.
    if let Err(e) = cgroup::ensure_parent() {
        warn!("cgroup sanity-check failed: {e} (will retry per-eval)");
    }
    cgroup::reap_stale();

    // Agent dials OUT to the host (the host listens). This inverts the old
    // host-dials-agent model and is the key to surviving snapshot/restore:
    // the agent detects a dead connection via a failed heartbeat *write*
    // (writes return EPIPE post-restore; reads never wake), then reconnects.
    // A fresh dial post-reset lands on a clean vsock device. The host's
    // `accept()` of the reconnect is its "agent is alive again" signal — so
    // the host never has to probe/hammer.
    loop {
        match VsockStream::connect(VsockAddr::new(HOST_CID, HOST_PORT)).await {
            Ok(stream) => {
                info!("connected to host vsock (cid={HOST_CID} port={HOST_PORT})");
                let started = Instant::now();
                let eval_completed = handle_session(stream).await;
                info!("session ended; reconnecting");
                // Two reasons to park for one tick instead of redialing hot:
                //
                // - A connect-then-instant-fail session (born dead in a
                //   TRANSPORT_RESET window) must back off like a failed
                //   dial; redialing at full speed starves the guest
                //   kernel's vsock reset processing.
                //
                // - A session that served a COMPLETED eval is over because
                //   the host is about to pause + snapshot. Redialing now
                //   would land in the dying listener's backlog and race the
                //   freeze mid-handshake — the snapshot then captures torn
                //   vsock state that costs the next restore ~200-350ms to
                //   untangle (measured). Parked, the freeze captures a
                //   sleeping task: nothing in flight, and the post-restore
                //   wake dials the NEW listener within one tick (~15ms
                //   attach). If no snapshot comes (cancelled op), waking
                //   20ms later and dialing is exactly the old behavior.
                //
                // A session that ran but served no eval (a seed build's
                // userspace-ready probe) redials immediately.
                if eval_completed || started.elapsed() < MIN_SESSION_FOR_IMMEDIATE_REDIAL {
                    tokio::time::sleep(DIAL_RETRY).await;
                }
            }
            Err(e) => {
                // Dial failed (host listener not up yet, or a born-dead
                // dial during a reset window). Back off gently and retry.
                error!("dial host failed: {e}");
                tokio::time::sleep(DIAL_RETRY).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Diagnostic infrastructure (lightweight; kept for postmortems)
// ---------------------------------------------------------------------------

fn diag_banner() {
    let pid = std::process::id();
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    diag_kmsg(&format!(
        "AGENT-DBG-{DBG_VERSION}: entry pid={pid} unix_ms={unix}"
    ));
}

fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "?".into());
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string>");
        diag_kmsg(&format!(
            "AGENT-DBG-{DBG_VERSION}: PANIC location={location} payload={payload}"
        ));
        default(info);
    }));
}

/// std::thread heartbeat — runs outside tokio so it keeps ticking even if
/// the runtime gets wedged. Low frequency (5s) since we're not actively
/// debugging anymore; this just provides a postmortem trail.
fn thread_heartbeat() {
    let mut tick: u64 = 0;
    loop {
        std::thread::sleep(THREAD_HB_PERIOD);
        tick = tick.wrapping_add(1);
        let unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        diag_kmsg(&format!("AGENT-DBG: thread-hb tick={tick} unix_ms={unix}"));
    }
}

/// Direct write of a single line to both `/dev/console` and `/dev/kmsg`.
/// `/dev/console` is the unfiltered path; `/dev/kmsg` is a fallback.
fn diag_kmsg(msg: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new().write(true).open("/dev/console") {
        let _ = writeln!(f, "{msg}");
    }
    if let Ok(mut f) = std::fs::OpenOptions::new().write(true).open("/dev/kmsg") {
        let _ = writeln!(f, "<3>{msg}");
    }
}

// ---------------------------------------------------------------------------
// Per-session handler — spawned per accepted connection
// ---------------------------------------------------------------------------

/// Serve one host session. Returns whether an eval ran to completion in it
/// — the caller uses that as the "snapshot imminent" signal (see the park
/// logic in `async_main`).
async fn handle_session(stream: VsockStream) -> bool {
    let (mut read_half, write_half) = tokio::io::split(stream);

    let (tx, mut rx) = mpsc::channel::<AgentMessage>(OUTBOUND_DEPTH);

    // Hello first — confirms the byte path works (not "born dead"). Queued
    // ahead of any heartbeat, so it's the first thing the writer emits.
    if tx.send(AgentMessage::Hello).await.is_err() {
        return false;
    }

    // Writer owns `write_half` and serializes all writes, so a `write_all`
    // is never cancelled mid-message by the read loop's select. On the
    // first write failure it signals `dead_tx` (the connection is gone —
    // post-restore this is the heartbeat write hitting EPIPE) and then
    // keeps draining `rx` so senders never block on a dead socket.
    let (dead_tx, dead_rx) = oneshot::channel::<()>();
    let writer = tokio::spawn(async move {
        let mut write_half = write_half;
        let mut dead_tx = Some(dead_tx);
        let mut buf = Vec::with_capacity(READ_BUF);
        let mut alive = true;
        while let Some(msg) = rx.recv().await {
            if !alive {
                continue;
            }
            buf.clear();
            if msg.encode_to(&mut buf).is_err() {
                continue;
            }
            if let Err(e) = write_half.write_all(&buf).await {
                error!("socket write error: {e}");
                alive = false;
                if let Some(t) = dead_tx.take() {
                    let _ = t.send(());
                }
            }
        }
    });

    // Heartbeat: push a `Heartbeat` into `tx` on a fixed interval. The
    // writer attempts to write it; if the connection is dead the write
    // fails and `dead_rx` fires, breaking the read loop below. This is
    // the post-restore dead-connection detector.
    let hb_tx = tx.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        // Skip the immediate first tick — Hello already proved the path.
        interval.tick().await;
        loop {
            interval.tick().await;
            if hb_tx.send(AgentMessage::Heartbeat).await.is_err() {
                break;
            }
        }
    });

    // Per-eval handshake: handle_eval sends () on `done_rx` BEFORE it puts
    // ProcessExit on `tx`. The main read loop's biased select polls
    // `done_rx` ahead of socket reads, so `current_eval` is always cleared
    // before the next host-sent Eval is parsed.
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<()>();

    let mut current_eval: Option<UnboundedSender<EvalControl>> = None;
    let mut eval_completed = false;
    let mut decoder = HostMessageDecoder::new();
    let mut buf = vec![0u8; READ_BUF];
    let mut dead_rx = dead_rx;

    'read: loop {
        let n = tokio::select! {
            biased;
            Some(()) = done_rx.recv() => {
                current_eval = None;
                eval_completed = true;
                continue 'read;
            }
            // A write failed → the connection is dead (snapshot/restore).
            // Break and let `async_main` reconnect.
            _ = &mut dead_rx => {
                info!("vsock session: write failed, reconnecting");
                break 'read;
            }
            r = read_half.read(&mut buf) => match r {
                Ok(0) => {
                    info!("vsock session: clean EOF");
                    break 'read;
                }
                Ok(n) => n,
                Err(e) => {
                    error!("vsock session: read error: {e}");
                    break 'read;
                }
            }
        };
        decoder.push(&buf[..n]);
        loop {
            match decoder.next_message() {
                Ok(Some(msg)) => dispatch_host_message(msg, &mut current_eval, &tx, &done_tx).await,
                Ok(None) => break,
                Err(e) => {
                    error!("decoder error: {e}");
                    break 'read;
                }
            }
        }
    }

    // Dropping the control sender lets a running eval observe EOF on its
    // control channel and kill its child before exiting (handle_eval calls
    // cg.kill() on ctrl_rx == None). Dropping our tx + aborting the
    // heartbeat lets the writer task wind down.
    drop(current_eval);
    heartbeat.abort();
    drop(tx);
    let _ = writer.await;
    eval_completed
}

async fn dispatch_host_message(
    msg: HostMessage,
    current_eval: &mut Option<UnboundedSender<EvalControl>>,
    tx: &Sender<AgentMessage>,
    done_tx: &UnboundedSender<()>,
) {
    if current_eval.as_ref().is_some_and(|c| c.is_closed()) {
        *current_eval = None;
    }

    match msg {
        HostMessage::Eval {
            pwd_path,
            binary_path,
            argv,
        } => {
            if current_eval.is_some() {
                let err = std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "eval received while previous eval still running",
                );
                let _ = tx.send(AgentMessage::ProcessErr(err)).await;
                return;
            }
            let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel();
            tokio::spawn(handle_eval(
                pwd_path,
                binary_path,
                argv,
                tx.clone(),
                ctrl_rx,
                done_tx.clone(),
            ));
            *current_eval = Some(ctrl_tx);
        }
        HostMessage::Stdin(data) => {
            if let Some(c) = current_eval.as_ref() {
                let _ = c.send(EvalControl::Stdin(data));
            }
        }
        HostMessage::StdinClose => {
            if let Some(c) = current_eval.as_ref() {
                let _ = c.send(EvalControl::StdinClose);
            }
        }
        HostMessage::Kill => {
            if let Some(c) = current_eval.as_ref() {
                let _ = c.send(EvalControl::Kill);
            }
        }
    }
}

async fn handle_eval(
    pwd_path: String,
    binary_path: String,
    argv: Vec<String>,
    tx: Sender<AgentMessage>,
    mut ctrl_rx: UnboundedReceiver<EvalControl>,
    done_tx: UnboundedSender<()>,
) {
    let cg = match cgroup::Cgroup::create() {
        Ok(c) => c,
        Err(err) => {
            let _ = done_tx.send(());
            let _ = tx.send(AgentMessage::ProcessErr(err)).await;
            return;
        }
    };

    // Owned CString moves into the pre_exec closure so the child can open
    // cgroup.procs without touching the heap (post-fork, pre-exec).
    let procs_path = cg.procs_path_cstr().to_owned();
    let mut cmd = Command::new(&binary_path);
    cmd.args(&argv)
        .current_dir(&pwd_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: migrate_self only uses async-signal-safe syscalls and a
    // pre-allocated CString — no heap, no Rust formatting, no locks.
    unsafe {
        cmd.pre_exec(move || cgroup::migrate_self(&procs_path));
    }

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(err) => {
            let _ = done_tx.send(());
            let _ = tx.send(AgentMessage::ProcessErr(err)).await;
            return;
        }
    };

    let stdin = child.stdin.take().expect("stdin was piped");
    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");

    let stdout_task = tokio::spawn(forward(stdout, Stream::Stdout, tx.clone()));
    let stderr_task = tokio::spawn(forward(stderr, Stream::Stderr, tx.clone()));

    // Stdin pipe lives in a tiny task so a slow stdin write can't stall the
    // control loop (Kill must stay responsive even if the child has stopped
    // reading stdin). Dropping the sender EOFs the child's stdin. The channel
    // stays unbounded (an awaited send here would block Kill — the reason it
    // exists), so the memory bound is enforced separately: `stdin_buffered`
    // tracks bytes queued but not yet written, and past STDIN_BUF_CAP new
    // stdin is dropped instead of OOMing the guest.
    let (stdin_writer_tx, stdin_writer_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let stdin_buffered = Arc::new(AtomicUsize::new(0));
    let stdin_task = tokio::spawn(write_stdin(stdin, stdin_writer_rx, Arc::clone(&stdin_buffered)));
    let mut stdin_writer_tx: Option<UnboundedSender<Vec<u8>>> = Some(stdin_writer_tx);

    let wait_fut = child.wait();
    tokio::pin!(wait_fut);

    let mut ctrl_open = true;
    let wait_result = loop {
        tokio::select! {
            ctrl = ctrl_rx.recv(), if ctrl_open => match ctrl {
                Some(EvalControl::Stdin(data)) => {
                    if let Some(t) = stdin_writer_tx.as_ref() {
                        let queued = stdin_buffered.load(Ordering::Relaxed);
                        if queued + data.len() > STDIN_BUF_CAP {
                            warn!(
                                "stdin buffer over {STDIN_BUF_CAP} bytes (child not \
                                 reading); dropping {} bytes",
                                data.len()
                            );
                        } else {
                            stdin_buffered.fetch_add(data.len(), Ordering::Relaxed);
                            let _ = t.send(data);
                        }
                    }
                }
                Some(EvalControl::StdinClose) => {
                    stdin_writer_tx = None;
                }
                Some(EvalControl::Kill) => cg.kill(),
                None => {
                    // Session is shutting down (host closed the connection
                    // or the per-session task is exiting). Tear down the
                    // whole process tree so nothing outlives the session.
                    ctrl_open = false;
                    cg.kill();
                }
            },
            status = &mut wait_fut => break status,
        }
    };

    // Direct child has exited. Anything else still in the cgroup is a
    // backgrounded/daemonized descendant — kill it so the task does not
    // outlive the eval. Idempotent if Kill already drained the cgroup.
    cg.kill();

    // Close stdin (if not already) and drain output so the host never sees a
    // StreamChunk arrive after ProcessExit.
    drop(stdin_writer_tx);
    let _ = stdin_task.await;
    let _ = stdout_task.await;
    let _ = stderr_task.await;

    // Wait for the killed tree to actually drain, then rmdir — BEFORE
    // ProcessExit goes out (which is what lets the host snapshot). An
    // immediate rmdir raced slow-exiting descendants into EBUSY, and the
    // leaked eval-N dir was captured into the frame and inherited by every
    // fork down the lineage.
    cg.destroy().await;

    // Signal the session loop BEFORE putting ProcessExit on the wire.
    let _ = done_tx.send(());

    let final_msg = match wait_result {
        Ok(status) => {
            let result = if let Some(sig) = status.signal() {
                ExitResult::Signal(sig)
            } else {
                ExitResult::Code(status.code().unwrap_or(-1))
            };
            AgentMessage::ProcessExit(result)
        }
        // wait() errored AFTER the child ran — distinct from ProcessErr
        // (spawn-time failure): the frame may have been mutated, and the
        // host must not report "spawn failed" for a command that executed.
        Err(err) => AgentMessage::ProcessWaitErr(err),
    };
    let _ = tx.send(final_msg).await;
}

async fn write_stdin(
    mut stdin: ChildStdin,
    mut rx: UnboundedReceiver<Vec<u8>>,
    buffered: Arc<AtomicUsize>,
) {
    while let Some(data) = rx.recv().await {
        let n = data.len();
        let ok = stdin.write_all(&data).await.is_ok();
        buffered.fetch_sub(n, Ordering::Relaxed);
        if !ok {
            return;
        }
    }
    // Falling out drops `stdin` → child sees EOF on its stdin pipe.
}

async fn forward<R>(mut reader: R, stream: Stream, tx: Sender<AgentMessage>)
where
    R: AsyncReadExt + Unpin,
{
    let mut buf = vec![0u8; READ_BUF];
    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };
        let chunk = AgentMessage::StreamChunk {
            stream,
            data: buf[..n].to_vec(),
        };
        if tx.send(chunk).await.is_err() {
            return;
        }
    }
}
