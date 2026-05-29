//! Axum router exposing Frame build / step over SSE and WebSocket.
//!
//! `/frames/build` (POST/SSE) and `/frames/{id}/step` each spawn the
//! underlying op in a task and bridge its event stream out to the client.
//! The step endpoint serves two transports off the same URL:
//!
//! - `POST /frames/{id}/step` — JSON body, one-way SSE response. For
//!   fire-and-watch clients (CI runners, simple log tailers).
//! - `GET  /frames/{id}/step` — WebSocket upgrade, duplex. For
//!   interactive clients that need to send stdin / drive an SSH session
//!   inside the guest. The first WS message from the client is the
//!   EvalRequest (JSON text frame); subsequent binary frames are opaque
//!   stdin bytes forwarded to the guest, and outbound binary frames are
//!   stdout bytes from the guest. Phase + result events ride out as JSON
//!   text frames. A plain `GET` without upgrade headers returns 426
//!   pointing at the POST alternative.

use std::{convert::Infallible, io, path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    extract::{
        FromRequestParts, Multipart, Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response, Sse, sse::Event, sse::KeepAlive},
    routing::{get, post},
};
use base64::Engine;
use fctools::vmm::installation::VmmInstallation;
use flate2::read::GzDecoder;
use futures_util::{SinkExt, Stream, StreamExt};
use serde::Serialize;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    frame::{FrameId, FrameStore},
    nix_build::ServerArtifacts,
    ops::{BuildEvent, EvalRequest, StepEvent, StepInput, StepOutcome, build_frame, step_frame},
};

/// Cap on the compressed tarball size we'll accept from a single upload.
const MAX_UPLOAD_BYTES: u64 = 50 * 1024 * 1024;
/// Cap on the total decompressed bytes we'll write to disk per upload —
/// defuses zip-bomb-style uploads (1 MB tarball, 10 GB extracted).
const MAX_EXTRACTED_BYTES: u64 = 500 * 1024 * 1024;

pub struct AppState {
    pub installation: Arc<VmmInstallation>,
    pub frames: Arc<FrameStore>,
    pub artifacts: Arc<ServerArtifacts>,
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/frames", get(list_frames))
        .route("/frames/build", post(build_handler))
        .route(
            "/frames/{id}/step",
            post(step_handler_sse).get(step_handler_ws),
        )
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok\n"
}

#[derive(Serialize)]
struct FrameList {
    frames: Vec<String>,
}

async fn list_frames(State(state): State<Arc<AppState>>) -> Json<FrameList> {
    let ids = state.frames.ids().await;
    Json(FrameList {
        frames: ids.into_iter().map(|i| i.as_str().to_owned()).collect(),
    })
}

async fn build_handler(
    State(state): State<Arc<AppState>>,
    multipart: Option<Multipart>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, String)> {
    // If we got multipart with a `flake` part, extract it now (before kicking
    // off any nix work) so we can return a clean 4xx for bad uploads.
    let user_flake_dir = match multipart {
        Some(mp) => match extract_user_flake(mp, &state.frames).await {
            Ok(Some(dir)) => Some(dir),
            Ok(None) => None,
            Err((code, msg)) => return Err((code, msg)),
        },
        None => None,
    };

    let (event_tx, mut event_rx) = mpsc::channel::<BuildEvent>(64);
    let (sse_tx, sse_rx) = mpsc::channel::<Result<Event, Infallible>>(64);

    let installation = Arc::clone(&state.installation);
    let frames = Arc::clone(&state.frames);
    let artifacts = Arc::clone(&state.artifacts);

    // One owner of `sse_tx`: forward events, then send the terminal result,
    // then drop. That guarantees the SSE stream ends cleanly right after
    // `event: result` — no lingering keep-alive on a "finished" connection.
    tokio::spawn(async move {
        let build_task = tokio::spawn(build_frame(
            installation,
            frames,
            artifacts,
            user_flake_dir,
            event_tx,
        ));
        while let Some(ev) = event_rx.recv().await {
            if sse_tx.send(Ok(build_event_to_sse(ev))).await.is_err() {
                return;
            }
        }
        let result = build_task.await;
        let ev = match result {
            Ok(Ok(id)) => result_event(json!({"ok": true, "frame_id": id.as_str()})),
            Ok(Err(e)) => result_event(json!({"ok": false, "error": e.to_string()})),
            Err(e) => result_event(json!({"ok": false, "error": format!("panic: {e}")})),
        };
        let _ = sse_tx.send(Ok(ev)).await;
    });

    Ok(Sse::new(ReceiverStream::new(sse_rx)).keep_alive(KeepAlive::default()))
}

/// Pull a `flake` part out of a multipart request, write its bytes to a
/// per-request scratch dir under the FrameStore, gunzip + untar, and return
/// the extracted root.
///
/// Returns `Ok(None)` if the multipart had no `flake` part — let the caller
/// fall back to the default storeDisk. Returns `Err((status, msg))` for upload
/// errors that should be reported as HTTP 4xx (oversized, malformed, no
/// flake.nix at root).
async fn extract_user_flake(
    mut multipart: Multipart,
    store: &FrameStore,
) -> Result<Option<PathBuf>, (StatusCode, String)> {
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart: {e}")))?
    {
        if field.name() != Some("flake") {
            continue;
        }

        // Build a per-upload scratch dir under FrameStore.root() so it's
        // cleaned up when the host process exits.
        let upload_dir = store
            .root()
            .join("uploads")
            .join(format!("u-{}", ulid::Ulid::new()));
        let extracted_dir = upload_dir.join("flake");
        tokio::fs::create_dir_all(&extracted_dir)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {e}")))?;

        // Stream the bytes to a tempfile in the upload dir, capped at MAX_UPLOAD_BYTES.
        let tarball_path = upload_dir.join("flake.tgz");
        let bytes = collect_field_bytes(field, MAX_UPLOAD_BYTES).await?;
        tokio::fs::write(&tarball_path, &bytes)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write tarball: {e}")))?;

        // Extract synchronously with a running byte cap. tar+flate2 are not
        // async, but extracting a 50 MB tarball is fast enough to do
        // in-line — and we want to apply the byte cap on the decompressor.
        let extracted_dir_clone = extracted_dir.clone();
        let tarball_path_clone = tarball_path.clone();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let f = std::fs::File::open(&tarball_path_clone)?;
            let gz = GzDecoder::new(f);
            let capped = CappedReader::new(gz, MAX_EXTRACTED_BYTES);
            let mut archive = tar::Archive::new(capped);
            archive.unpack(&extracted_dir_clone)?;
            Ok(())
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {e}")))?
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("extract: {e}")))?;

        // Validate the extracted tree. If the user tar-balled a directory
        // (e.g. `tar czf flake.tgz myflake/`), descend into the single child.
        let root = resolve_flake_root(&extracted_dir).await.map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("extracted tarball has no flake.nix at root: {e}"),
            )
        })?;

        return Ok(Some(root));
    }
    Ok(None)
}

/// Collect a multipart field's bytes into a `Vec<u8>`, rejecting if it grows
/// past `max_bytes`.
async fn collect_field_bytes(
    mut field: axum::extract::multipart::Field<'_>,
    max_bytes: u64,
) -> Result<Vec<u8>, (StatusCode, String)> {
    let mut acc: Vec<u8> = Vec::new();
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart chunk: {e}")))?
    {
        if (acc.len() as u64) + (chunk.len() as u64) > max_bytes {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("upload exceeds {max_bytes} bytes"),
            ));
        }
        acc.extend_from_slice(&chunk);
    }
    Ok(acc)
}

/// Find the directory that contains `flake.nix`. Accepts both flat tarballs
/// (`flake.nix` at the root) and ones that wrap everything in a single
/// subdirectory (the common `tar czf x.tgz mydir/` shape).
async fn resolve_flake_root(extracted: &std::path::Path) -> io::Result<PathBuf> {
    if tokio::fs::try_exists(extracted.join("flake.nix")).await? {
        return Ok(extracted.to_path_buf());
    }
    let mut entries = tokio::fs::read_dir(extracted).await?;
    let mut subdirs: Vec<PathBuf> = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            subdirs.push(entry.path());
        }
    }
    if subdirs.len() == 1 {
        let only = &subdirs[0];
        if tokio::fs::try_exists(only.join("flake.nix")).await? {
            return Ok(only.clone());
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "no flake.nix at extracted tarball root or single-child subdir",
    ))
}

/// Reader adapter that errors out once it has produced `max_bytes` total.
/// Used to bound decompressed output of an untrusted gzip stream.
struct CappedReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: io::Read> CappedReader<R> {
    fn new(inner: R, max_bytes: u64) -> Self {
        Self {
            inner,
            remaining: max_bytes,
        }
    }
}

impl<R: io::Read> io::Read for CappedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decompressed size cap exceeded",
            ));
        }
        let max = std::cmp::min(buf.len() as u64, self.remaining) as usize;
        let n = self.inner.read(&mut buf[..max])?;
        self.remaining -= n as u64;
        Ok(n)
    }
}

async fn step_handler_sse(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(eval): Json<EvalRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, String)> {
    let frame_id = FrameId::from(id);
    let Some(parent) = state.frames.get(&frame_id).await else {
        return Err((StatusCode::NOT_FOUND, format!("no such frame: {frame_id}")));
    };

    let (event_tx, mut event_rx) = mpsc::channel::<StepEvent>(64);
    let (sse_tx, sse_rx) = mpsc::channel::<Result<Event, Infallible>>(64);
    // Lets us tell `step_frame` to abort (and `hard_kill` its VM) the moment
    // the client drops the SSE connection. Without this, `step_task` runs to
    // completion on a detached `JoinHandle` and leaks a firecracker process
    // per Ctrl-C.
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    // SSE is one-way — there is no client→server byte path. Drop the tx
    // immediately so `run_eval`'s `inputs.recv()` returns `None` on its
    // first poll and the branch goes dormant.
    let (_input_tx, input_rx) = mpsc::channel::<StepInput>(1);
    drop(_input_tx);

    let installation = Arc::clone(&state.installation);
    let frames = Arc::clone(&state.frames);

    tokio::spawn(async move {
        let step_task = tokio::spawn(step_frame(
            installation,
            frames,
            parent,
            eval,
            event_tx,
            input_rx,
            cancel_rx,
        ));
        let mut cancel_tx = Some(cancel_tx);
        while let Some(ev) = event_rx.recv().await {
            if sse_tx.send(Ok(step_event_to_sse(ev))).await.is_err() {
                if let Some(tx) = cancel_tx.take() {
                    let _ = tx.send(());
                }
                break;
            }
        }
        // Wait for `step_frame` to finish (clean tear-down via hard_kill)
        // whether we cancelled it or it completed naturally.
        let result = step_task.await;
        let ev = match result {
            Ok(Ok((id, outcome))) => result_event(json!({
                "ok": true,
                "frame_id": id.as_str(),
                "outcome": outcome_to_json(&outcome),
            })),
            Ok(Err(e)) => result_event(json!({"ok": false, "error": e.to_string()})),
            Err(e) => result_event(json!({"ok": false, "error": format!("panic: {e}")})),
        };
        let _ = sse_tx.send(Ok(ev)).await;
    });

    Ok(Sse::new(ReceiverStream::new(sse_rx)).keep_alive(KeepAlive::default()))
}

/// `WebSocketUpgrade` extractor that returns 426 (with our pointer at the
/// POST/SSE alternative) on missing upgrade headers, instead of axum's
/// default 400. axum 0.8's `WebSocketUpgrade` doesn't impl
/// `OptionalFromRequestParts`, so `Option<WebSocketUpgrade>` won't
/// compile; this is the small wrapper that gives us the same shape.
struct RequiredWsUpgrade(WebSocketUpgrade);

impl<S> FromRequestParts<S> for RequiredWsUpgrade
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        match WebSocketUpgrade::from_request_parts(parts, state).await {
            Ok(u) => Ok(Self(u)),
            Err(_) => Err((
                StatusCode::UPGRADE_REQUIRED,
                [("upgrade", "websocket"), ("connection", "Upgrade")],
                "GET /frames/{id}/step requires a WebSocket upgrade.\n\
                 For non-interactive use, POST /frames/{id}/step with a JSON \
                 EvalRequest body to get an SSE event stream.\n",
            )
                .into_response()),
        }
    }
}

/// `GET /frames/{id}/step` — WebSocket upgrade handler. Missing upgrade
/// headers short-circuit to 426 via [RequiredWsUpgrade]. Otherwise we
/// upgrade and hand off to [run_step_ws_session].
async fn step_handler_ws(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    RequiredWsUpgrade(upgrade): RequiredWsUpgrade,
) -> Response {
    tracing::info!(frame = %id, "ws_step: handler invoked");
    let frame_id = FrameId::from(id);
    let Some(parent) = state.frames.get(&frame_id).await else {
        return (
            StatusCode::NOT_FOUND,
            format!("no such frame: {frame_id}"),
        )
            .into_response();
    };
    let installation = Arc::clone(&state.installation);
    let frames = Arc::clone(&state.frames);
    tracing::info!(frame = %frame_id, "ws_step: accepting upgrade");
    upgrade.on_upgrade(move |socket| run_step_ws_session(socket, installation, frames, parent))
}

/// Drive one WebSocket session against `step_frame`.
///
/// Wire shape (small and explicit, no postcard / no extra deps):
///
/// - First client message: text frame containing the JSON `EvalRequest`.
///   Anything else closes the socket.
/// - Subsequent client → server frames:
///   - **binary**: raw bytes appended to the guest child's stdin.
///   - **text** with `{"stdin_close": true}`: EOF the child's stdin.
///   - **text** with `{"kill": true}`: SIGKILL the child process tree.
/// - Server → client frames:
///   - **binary**: stdout bytes from the guest child (stderr is folded in
///     as a JSON text frame so a binary stream stays a single logical
///     stream — appropriate for the dropbear-wraps-shell case where the
///     SSH protocol is on stdout only and stderr carries only dropbear
///     logs we don't want mixed in).
///   - **text**: JSON event with one of these shapes:
///     - `{"phase": "<name>"}`
///     - `{"stderr": "<base64>"}`
///     - `{"result": {"ok": true,  "frame_id": "...", "outcome": ...}}`
///     - `{"result": {"ok": false, "error": "..."}}`
///
/// Closing the socket from the client side cancels `step_frame` and
/// hard-kills the VM (same semantics as dropping the SSE connection).
async fn run_step_ws_session(
    socket: WebSocket,
    installation: Arc<VmmInstallation>,
    frames: Arc<FrameStore>,
    parent: Arc<crate::frame::Frame>,
) {
    let t0 = std::time::Instant::now();
    tracing::info!("ws_step: session entered");
    // Split so `send` and `recv` borrow different halves and the
    // tokio::select! below doesn't double-borrow `socket`.
    let (mut ws_sink, mut ws_stream) = socket.split();

    // Read the first frame: must be the EvalRequest as JSON text. Anything
    // else (binary first, close, ping, error) -> close the socket.
    let eval = match ws_stream.next().await {
        Some(Ok(Message::Text(t))) => match serde_json::from_str::<EvalRequest>(t.as_str()) {
            Ok(e) => e,
            Err(e) => {
                let _ = ws_sink
                    .send(Message::Text(
                        json!({"error": format!("invalid EvalRequest: {e}")})
                            .to_string()
                            .into(),
                    ))
                    .await;
                return;
            }
        },
        _ => return,
    };

    tracing::info!(elapsed_ms = t0.elapsed().as_millis() as u64, "ws_step: Eval frame received");

    let (event_tx, mut event_rx) = mpsc::channel::<StepEvent>(64);
    let (input_tx, input_rx) = mpsc::channel::<StepInput>(64);
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

    let step_task = tokio::spawn(step_frame(
        installation,
        frames,
        parent,
        eval,
        event_tx,
        input_rx,
        cancel_rx,
    ));
    tracing::info!(elapsed_ms = t0.elapsed().as_millis() as u64, "ws_step: step_frame spawned");

    let mut cancel_tx = Some(cancel_tx);

    // Multiplex between:
    //   (a) outbound step events → ws_sink,
    //   (b) inbound ws frames → input_tx (or cancel),
    // until either side terminates. Whichever side dies first triggers
    // cancel + drain.
    loop {
        tokio::select! {
            biased;
            ev = event_rx.recv() => {
                let Some(ev) = ev else { break; };
                if send_step_event(&mut ws_sink, ev).await.is_err() {
                    if let Some(tx) = cancel_tx.take() {
                        let _ = tx.send(());
                    }
                    break;
                }
            }
            frame = ws_stream.next() => {
                match frame {
                    Some(Ok(Message::Binary(b))) => {
                        if input_tx.send(StepInput::Stdin(b.to_vec())).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Text(t))) => {
                        let v: serde_json::Value =
                            serde_json::from_str(t.as_str()).unwrap_or(json!({}));
                        if v.get("kill").and_then(|b| b.as_bool()) == Some(true) {
                            let _ = input_tx.send(StepInput::Kill).await;
                        } else if v.get("stdin_close").and_then(|b| b.as_bool()) == Some(true) {
                            let _ = input_tx.send(StepInput::StdinClose).await;
                        }
                        // Other text frames are ignored as forward-compat.
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = ws_sink.send(Message::Pong(p)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        if let Some(tx) = cancel_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                }
            }
        }
    }

    // Drop input_tx so any pending input branch in run_eval observes EOF
    // promptly and the step_task can wind down.
    drop(input_tx);

    // Wait for step_frame to finish so we can send the terminal result.
    let final_msg = match step_task.await {
        Ok(Ok((id, outcome))) => json!({"result": {
            "ok": true,
            "frame_id": id.as_str(),
            "outcome": outcome_to_json(&outcome),
        }}),
        Ok(Err(e)) => json!({"result": {"ok": false, "error": e.to_string()}}),
        Err(e) => json!({"result": {"ok": false, "error": format!("panic: {e}")}}),
    };
    let _ = ws_sink
        .send(Message::Text(final_msg.to_string().into()))
        .await;
    let _ = ws_sink.send(Message::Close(None)).await;
}

/// Send one `StepEvent` over the WS sink.
async fn send_step_event<S>(sink: &mut S, ev: StepEvent) -> Result<(), axum::Error>
where
    S: futures_util::Sink<Message, Error = axum::Error> + Unpin,
{
    let msg = match ev {
        StepEvent::Phase(name) => Message::Text(json!({"phase": name}).to_string().into()),
        StepEvent::Stream {
            stream: agent_protocol::Stream::Stdout,
            data,
        } => Message::Binary(data.into()),
        StepEvent::Stream {
            stream: agent_protocol::Stream::Stderr,
            data,
        } => {
            // Keep stdout binary as the single logical byte stream and ship
            // stderr separately as base64 text so a client treating binary
            // frames as raw guest stdout (e.g. SSH bytes from dropbear's
            // stdout) doesn't get them interleaved with stderr noise.
            let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
            Message::Text(json!({"stderr": b64}).to_string().into())
        }
    };
    sink.send(msg).await
}

fn build_event_to_sse(ev: BuildEvent) -> Event {
    match ev {
        BuildEvent::Log { source, line } => Event::default()
            .event("log")
            .data(json!({"source": source, "line": line}).to_string()),
        BuildEvent::Phase(name) => Event::default()
            .event("phase")
            .data(json!({"name": name}).to_string()),
        BuildEvent::Ready => Event::default().event("ready").data(""),
    }
}

fn step_event_to_sse(ev: StepEvent) -> Event {
    match ev {
        StepEvent::Phase(name) => Event::default()
            .event("phase")
            .data(json!({"name": name}).to_string()),
        StepEvent::Stream { stream, data } => {
            let kind = match stream {
                agent_protocol::Stream::Stdout => "stdout",
                agent_protocol::Stream::Stderr => "stderr",
            };
            let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
            Event::default()
                .event(kind)
                .data(json!({"data": b64}).to_string())
        }
    }
}

fn result_event(body: serde_json::Value) -> Event {
    Event::default().event("result").data(body.to_string())
}

fn outcome_to_json(o: &StepOutcome) -> serde_json::Value {
    match o {
        StepOutcome::Exited(agent_protocol::ExitResult::Code(c)) => json!({"exited": {"code": c}}),
        StepOutcome::Exited(agent_protocol::ExitResult::Signal(s)) => {
            json!({"exited": {"signal": s}})
        }
        StepOutcome::SpawnFailed(e) => json!({"spawn_failed": e.to_string()}),
    }
}
