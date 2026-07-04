//! One step session over `GET /frames/{id}/step` (WebSocket upgrade).
//!
//! The step's command is the in-guest `pty-bridge`, which allocates a PTY,
//! runs the user's command on it, and bridges the PTY to its own
//! stdin/stdout — which are the agent's pipes. This module is the CLI half
//! of that bridge.
//!
//! Wire shape over the WebSocket:
//!
//! - **binary frames inbound** = raw PTY output from the command →
//!   [SessionEvent::Stdout] → the REPL's vt100 parser.
//! - **binary frames outbound** = framed control to `pty-bridge`:
//!
//!       [type: u8][len: u32 big-endian][payload]
//!       type 0 = input bytes  (keystrokes → PTY master)
//!       type 1 = window size  ([rows: u16 BE][cols: u16 BE] → TIOCSWINSZ)
//!
//! - **text frames inbound** = cradle control events (phase / stderr /
//!   result), same JSON the SSE transport uses.
//!
//! There's no handshake, no auth, no negotiation — the bytes on the wire
//! are just the bridge's framing over cradle's already-trusted pipe path.
//! [open] does no I/O itself; it spawns a supervisor task and returns, so
//! the REPL stays responsive while the WS connects and the VM restores.

use anyhow::{Result, anyhow};
use base64::Engine;
use client_protocol::{EvalRequest, Outcome, StepControl};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Path the guest image installs `pty-bridge` at (via
/// `environment.systemPackages` in base-module.nix).
const PTY_BRIDGE: &str = "/run/current-system/sw/bin/pty-bridge";

/// Events delivered to the REPL during a step session.
#[derive(Debug)]
pub enum SessionEvent {
    /// Raw PTY output bytes from the running command.
    Stdout(Vec<u8>),
    /// Cradle phase marker (preparing / restoring / evaluating / snapshotting).
    Phase(String),
    /// The agent's spawned-process stderr (here, anything `pty-bridge`
    /// itself writes to its stderr — e.g. a spawn error). Carried as a
    /// host text frame, not part of the PTY stream.
    Stderr(Vec<u8>),
    /// Terminal event for the step: a new frame, or an error. Always the
    /// LAST event emitted on a session.
    Result(StepResult),
}

#[derive(Debug)]
pub enum StepResult {
    Ok {
        frame_id: String,
        outcome: Option<Outcome>,
    },
    Err(String),
}

/// Handle to a live session. Drop to cancel — the abort handle tears down
/// the supervisor task, closing the WS, which the host treats as a
/// cancellation (hard_kill the VM, no frame produced).
pub struct Session {
    pub events: mpsc::UnboundedReceiver<SessionEvent>,
    /// Outbound framed control to the bridge (input + winsize).
    out: mpsc::UnboundedSender<Vec<u8>>,
    _abort: AbortHandle,
}

impl Drop for Session {
    fn drop(&mut self) {
        self._abort.abort();
    }
}

impl Session {
    /// Forward keystroke/input bytes to the running command's PTY stdin.
    pub fn send_input(&self, bytes: &[u8]) {
        let _ = self.out.send(frame(0, bytes));
    }

    /// Send a window-size update; the bridge `TIOCSWINSZ`s the PTY, which
    /// `SIGWINCH`es the running program so it can reflow.
    pub fn send_resize(&self, rows: u16, cols: u16) {
        let mut payload = [0u8; 4];
        payload[0..2].copy_from_slice(&rows.to_be_bytes());
        payload[2..4].copy_from_slice(&cols.to_be_bytes());
        let _ = self.out.send(frame(1, &payload));
    }
}

/// Build one `[type][len][payload]` frame for the bridge's stdin.
fn frame(frame_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(5 + payload.len());
    f.push(frame_type);
    f.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    f.extend_from_slice(payload);
    f
}

/// Open a fresh step session. Synchronous — allocates the channels and
/// spawns the supervisor; the REPL gets back to its loop immediately and
/// sees `SessionEvent`s as the supervisor makes progress.
///
/// `cmd` is the user's shell command; the bridge runs it via
/// `/bin/sh -c <cmd>` so pipes/globs/PATH work. `(rows, cols)` is the
/// initial PTY size (baked into the bridge's args).
pub fn open(
    host_url: &str,
    frame_id: &str,
    cmd: &str,
    rows: u16,
    cols: u16,
) -> Result<Session> {
    let ws_url = http_to_ws(host_url)?;
    let ws_url = format!("{}/frames/{}/step", ws_url.trim_end_matches('/'), frame_id);

    let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();
    let (out_tx, out_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let supervisor = tokio::spawn(run_session(
        ws_url,
        events_tx,
        out_rx,
        cmd.to_owned(),
        rows,
        cols,
    ));

    Ok(Session {
        events: events_rx,
        out: out_tx,
        _abort: supervisor.abort_handle(),
    })
}

async fn run_session(
    ws_url: String,
    events_tx: mpsc::UnboundedSender<SessionEvent>,
    mut out_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    cmd: String,
    rows: u16,
    cols: u16,
) {
    let (ws, _resp) = match connect_async(&ws_url).await {
        Ok(t) => t,
        Err(e) => {
            let _ = events_tx.send(SessionEvent::Result(StepResult::Err(format!(
                "WS connect to {ws_url}: {e}"
            ))));
            return;
        }
    };
    let (mut ws_sink, mut ws_rx) = ws.split();

    // The step command: pty-bridge sizes the PTY from --rows/--cols and
    // runs the user's command via `/bin/sh -c`.
    let eval = EvalRequest {
        binary: PTY_BRIDGE.to_owned(),
        argv: vec![
            "--rows".to_owned(),
            rows.to_string(),
            "--cols".to_owned(),
            cols.to_string(),
            "--".to_owned(),
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            cmd,
        ],
        cwd: "/".to_owned(),
    };
    let eval = serde_json::to_string(&eval).expect("EvalRequest serializes");
    if let Err(e) = ws_sink.send(Message::Text(eval.into())).await {
        let _ = events_tx.send(SessionEvent::Result(StepResult::Err(format!(
            "send Eval: {e}"
        ))));
        return;
    }

    // Multiplex: inbound WS frames → events; outbound control → WS binary.
    loop {
        tokio::select! {
            biased;
            inbound = ws_rx.next() => {
                match inbound {
                    Some(Ok(Message::Binary(b))) => {
                        let _ = events_tx.send(SessionEvent::Stdout(b.to_vec()));
                    }
                    Some(Ok(Message::Text(t))) => {
                        if let Some(ev) = parse_control(t.as_str()) {
                            let _ = events_tx.send(ev);
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = ws_sink.send(Message::Pong(p)).await;
                    }
                    // Clean close: the step is over (the host sends the
                    // Result text frame before closing, so it's already
                    // been forwarded above).
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    // Transport error mid-step: surface it as the session's
                    // Result instead of breaking silently — otherwise the
                    // REPL can only synthesize a generic "session closed
                    // without result" and the real cause is lost.
                    Some(Err(e)) => {
                        let _ = events_tx.send(SessionEvent::Result(StepResult::Err(
                            format!("websocket error: {e}"),
                        )));
                        return;
                    }
                }
            }
            out = out_rx.recv() => {
                match out {
                    Some(bytes) => {
                        if ws_sink.send(Message::Binary(bytes.into())).await.is_err() {
                            break;
                        }
                    }
                    // out_tx dropped (Session gone) — nothing left to send.
                    // The abort will have fired; just stop.
                    None => break,
                }
            }
        }
    }
}

fn http_to_ws(url: &str) -> Result<String> {
    if let Some(rest) = url.strip_prefix("https://") {
        Ok(format!("wss://{rest}"))
    } else if let Some(rest) = url.strip_prefix("http://") {
        Ok(format!("ws://{rest}"))
    } else {
        Err(anyhow!("expected http(s) URL, got: {url}"))
    }
}

/// Decode one host text frame. Shared typed shapes with the host via
/// `client-protocol`; a frame that doesn't parse as any known control is
/// ignored (forward-compat), but every *known* frame — including the
/// pre-step `{"error": ...}` reply to a malformed EvalRequest — reaches the
/// REPL instead of being silently dropped.
fn parse_control(t: &str) -> Option<SessionEvent> {
    let control: StepControl = serde_json::from_str(t).ok()?;
    Some(match control {
        StepControl::Phase(name) => SessionEvent::Phase(name),
        StepControl::Stderr(b64) => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .ok()?;
            SessionEvent::Stderr(bytes)
        }
        StepControl::Error(e) => SessionEvent::Result(StepResult::Err(e)),
        StepControl::Result(r) => {
            if r.ok {
                match r.frame_id {
                    Some(frame_id) => SessionEvent::Result(StepResult::Ok {
                        frame_id,
                        outcome: r.outcome,
                    }),
                    None => SessionEvent::Result(StepResult::Err(
                        "step succeeded but host returned no frame_id".into(),
                    )),
                }
            } else {
                SessionEvent::Result(StepResult::Err(
                    r.error.unwrap_or_else(|| "unknown error".into()),
                ))
            }
        }
    })
}
