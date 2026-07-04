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
        DefaultBodyLimit, FromRequestParts, Multipart, Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response, Sse, sse::Event, sse::KeepAlive},
    routing::{get, post},
};
use base64::Engine;
use client_protocol::{
    BuildResult, DataEvent, Exit, LogEvent, Outcome, PhaseEvent, StepControl, StepResult,
};
use fctools::vmm::installation::VmmInstallation;
use flate2::read::GzDecoder;
use futures_util::{SinkExt, Stream, StreamExt};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
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
        .route(
            "/frames/build",
            // axum's default body limit is 2 MB, which would reject any real
            // flake upload long before our own MAX_UPLOAD_BYTES check runs.
            // Small slack on top of the cap so multipart framing overhead
            // doesn't shave bytes off a maximum-size tarball; the per-field
            // check below enforces the real limit.
            post(build_handler)
                .layer(DefaultBodyLimit::max((MAX_UPLOAD_BYTES + 64 * 1024) as usize)),
        )
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

async fn list_frames(
    State(state): State<Arc<AppState>>,
) -> Result<Json<FrameList>, (StatusCode, String)> {
    let ids = state.frames.ids().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("listing central store: {e}"),
        )
    })?;
    Ok(Json(FrameList {
        frames: ids.into_iter().map(|i| i.as_str().to_owned()).collect(),
    }))
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
    // Fires when the client drops the SSE connection, so `build_frame` can
    // kill the nix build / VM instead of running detached to completion and
    // finalizing a ~1 GiB frame nobody can reference.
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

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
            cancel_rx,
        ));
        let mut cancel_tx = Some(cancel_tx);
        while let Some(ev) = event_rx.recv().await {
            if sse_tx.send(Ok(build_event_to_sse(ev))).await.is_err() {
                if let Some(tx) = cancel_tx.take() {
                    let _ = tx.send(());
                }
                break;
            }
        }
        // Close the event channel BEFORE awaiting the task: once we stop
        // draining it, any in-flight `events.send` inside build_frame must
        // fail fast rather than block on a full channel forever (which would
        // wedge the task with its VM alive).
        drop(event_rx);
        let result = build_task.await;
        let body = match result {
            Ok(Ok(id)) => BuildResult {
                ok: true,
                frame_id: Some(id.as_str().to_owned()),
                error: None,
            },
            Ok(Err(e)) => BuildResult {
                ok: false,
                frame_id: None,
                error: Some(e.to_string()),
            },
            Err(e) => BuildResult {
                ok: false,
                frame_id: None,
                error: Some(format!("panic: {e}")),
            },
        };
        let _ = sse_tx.send(Ok(result_event(&body))).await;
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

        // Stream the bytes to a tempfile in the upload dir, capped at
        // MAX_UPLOAD_BYTES — chunk by chunk, so N concurrent uploads never
        // pin N full tarballs in heap.
        let tarball_path = upload_dir.join("flake.tgz");
        stream_field_to_file(field, &tarball_path, MAX_UPLOAD_BYTES).await?;

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

        // SECURITY: move the flake root to a server-generated name before it
        // goes anywhere near nix. `root` may be the tarball's own top-level
        // directory name — attacker-chosen bytes (quotes are legal in Unix
        // filenames) that user_flake.rs interpolates into a synthesized
        // flake.nix. After this rename, every component of the path the
        // wrapper flake sees was produced by this server.
        let src = upload_dir.join("src");
        tokio::fs::rename(&root, &src).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("normalize upload dir: {e}"),
            )
        })?;

        return Ok(Some(src));
    }
    Ok(None)
}

/// Stream a multipart field to `dest`, rejecting once it grows past
/// `max_bytes`. Never buffers more than one chunk in memory.
async fn stream_field_to_file(
    mut field: axum::extract::multipart::Field<'_>,
    dest: &std::path::Path,
    max_bytes: u64,
) -> Result<(), (StatusCode, String)> {
    let mut file = tokio::fs::File::create(dest)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("create tarball: {e}")))?;
    let mut written: u64 = 0;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart chunk: {e}")))?
    {
        written += chunk.len() as u64;
        if written > max_bytes {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("upload exceeds {max_bytes} bytes"),
            ));
        }
        file.write_all(&chunk)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write tarball: {e}")))?;
    }
    Ok(())
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
            // Only error if there really is MORE data — a payload of exactly
            // the cap is legal, and tar probes for EOF past the last entry.
            let mut probe = [0u8; 1];
            if self.inner.read(&mut probe)? == 0 {
                return Ok(0);
            }
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
    // Local hit or cold fetch from the central store (frames survive host
    // restarts; any machine can serve any committed frame id).
    let parent = match state.frames.get_or_fetch(&frame_id).await {
        Ok(Some(f)) => f,
        Ok(None) => {
            return Err((StatusCode::NOT_FOUND, format!("no such frame: {frame_id}")));
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("fetching {frame_id} from central store: {e}"),
            ));
        }
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
        // Close the event channel BEFORE awaiting the task. Once we stop
        // draining, any in-flight `events.send` inside step_frame must fail
        // fast instead of blocking on a full channel — otherwise a cancelled
        // step could wedge forever with its VM alive.
        drop(event_rx);
        // Wait for `step_frame` to finish (clean tear-down via hard_kill)
        // whether we cancelled it or it completed naturally.
        let result = step_task.await;
        let body = match result {
            Ok(Ok((id, outcome))) => step_result_ok(&id, &outcome),
            Ok(Err(e)) => step_result_err(e.to_string()),
            Err(e) => step_result_err(format!("panic: {e}")),
        };
        let _ = sse_tx.send(Ok(result_event(&body))).await;
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
    let parent = match state.frames.get_or_fetch(&frame_id).await {
        Ok(Some(f)) => f,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                format!("no such frame: {frame_id}"),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("fetching {frame_id} from central store: {e}"),
            )
                .into_response();
        }
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
                    .send(control_frame(&StepControl::Error(format!(
                        "invalid EvalRequest: {e}"
                    ))))
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

    let cancel_tx = Some(cancel_tx);

    // The two directions run as SEPARATE tasks. A single mux loop had two
    // failure modes this structure removes by construction:
    //
    // - circular backpressure deadlock: the loop blocked in `input_tx.send`
    //   (full) while run_eval blocked in `events.send` (full) — neither side
    //   drained the other, wedging the session and its VM forever;
    // - starvation: `biased` polling of `event_rx` first meant a guest
    //   flooding stdout kept that branch permanently ready, so the inbound
    //   frame carrying `{"kill":true}` was never read — the one affordance
    //   for stopping a runaway command didn't work under exactly the load
    //   that makes it necessary.
    //
    // Outbound: step events → ws_sink (plus pongs for the pings the inbound
    // task forwards). Owns the sink; returns it for the final result frame.
    let (pong_tx, mut pong_rx) = mpsc::channel::<Vec<u8>>(4);
    let outbound = tokio::spawn(async move {
        let mut pong_open = true;
        loop {
            tokio::select! {
                ev = event_rx.recv() => match ev {
                    Some(ev) => {
                        if send_step_event(&mut ws_sink, ev).await.is_err() {
                            // Client unreachable — stop draining; dropping
                            // event_rx (below, on return) makes step_frame's
                            // sends fail fast, and the inbound task sees the
                            // dead connection on its next read and cancels.
                            break;
                        }
                    }
                    None => break, // step finished; result is sent by the caller
                },
                p = pong_rx.recv(), if pong_open => match p {
                    Some(p) => {
                        let _ = ws_sink.send(Message::Pong(p.into())).await;
                    }
                    None => pong_open = false,
                },
            }
        }
        ws_sink
    });

    // Inbound: client frames → input_tx, in its OWN task. The session's main
    // path must never be parked on the client's next frame: for an input-less
    // command (a plain `ls`) there IS no next frame, and when this loop ran
    // inline here the finished result sat waiting behind `ws_stream.next()`
    // forever — the client stared at "snapshotting…" until it gave up. The
    // step finishing (below) is what ends the session; the client talking
    // again is optional.
    let inbound = tokio::spawn(async move {
        let mut cancel_tx = cancel_tx;
        loop {
            match ws_stream.next().await {
                Some(Ok(Message::Binary(b))) => {
                    if input_tx.send(StepInput::Stdin(b.to_vec())).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Text(t))) => {
                    let v: serde_json::Value =
                        serde_json::from_str(t.as_str()).unwrap_or(serde_json::Value::Null);
                    if v.get("kill").and_then(|b| b.as_bool()) == Some(true) {
                        let _ = input_tx.send(StepInput::Kill).await;
                    } else if v.get("stdin_close").and_then(|b| b.as_bool()) == Some(true) {
                        let _ = input_tx.send(StepInput::StdinClose).await;
                    }
                    // Other text frames are ignored as forward-compat.
                }
                Some(Ok(Message::Ping(p))) => {
                    // try_send: pongs are advisory; dropping one under load is
                    // fine, blocking the inbound loop behind the sink is not.
                    let _ = pong_tx.try_send(p.to_vec());
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
        // Drop input_tx so any pending input branch in run_eval observes EOF
        // promptly (pong_tx dies with the task).
        drop(input_tx);
    });

    // Wait for step_frame to finish (hard_kill has run either way). The
    // outbound task ends right after: step_frame's event senders are gone,
    // so its `event_rx.recv()` returns None.
    let result = step_task.await;
    let Ok(mut ws_sink) = outbound.await else {
        return;
    };
    let body = match result {
        Ok(Ok((id, outcome))) => step_result_ok(&id, &outcome),
        Ok(Err(e)) => step_result_err(e.to_string()),
        Err(e) => step_result_err(format!("panic: {e}")),
    };
    let _ = ws_sink
        .send(control_frame(&StepControl::Result(body)))
        .await;
    let _ = ws_sink.send(Message::Close(None)).await;
    // The session is over; don't leave the reader parked on a client that
    // never sends another frame.
    inbound.abort();
}

/// Serialize a [`StepControl`] into a WS text frame.
fn control_frame(c: &StepControl) -> Message {
    Message::Text(
        serde_json::to_string(c)
            .expect("StepControl serializes")
            .into(),
    )
}

/// Send one `StepEvent` over the WS sink.
async fn send_step_event<S>(sink: &mut S, ev: StepEvent) -> Result<(), axum::Error>
where
    S: futures_util::Sink<Message, Error = axum::Error> + Unpin,
{
    let msg = match ev {
        StepEvent::Phase(name) => control_frame(&StepControl::Phase(name.to_owned())),
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
            control_frame(&StepControl::Stderr(b64))
        }
    };
    sink.send(msg).await
}

fn build_event_to_sse(ev: BuildEvent) -> Event {
    match ev {
        BuildEvent::Log { source, line } => Event::default().event("log").data(
            serde_json::to_string(&LogEvent {
                source: source.to_owned(),
                line,
            })
            .expect("LogEvent serializes"),
        ),
        BuildEvent::Phase(name) => Event::default().event("phase").data(
            serde_json::to_string(&PhaseEvent {
                name: name.to_owned(),
            })
            .expect("PhaseEvent serializes"),
        ),
        BuildEvent::Ready => Event::default().event("ready").data(""),
    }
}

fn step_event_to_sse(ev: StepEvent) -> Event {
    match ev {
        StepEvent::Phase(name) => Event::default().event("phase").data(
            serde_json::to_string(&PhaseEvent {
                name: name.to_owned(),
            })
            .expect("PhaseEvent serializes"),
        ),
        StepEvent::Stream { stream, data } => {
            let kind = match stream {
                agent_protocol::Stream::Stdout => "stdout",
                agent_protocol::Stream::Stderr => "stderr",
            };
            let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
            Event::default().event(kind).data(
                serde_json::to_string(&DataEvent { data: b64 }).expect("DataEvent serializes"),
            )
        }
    }
}

fn result_event<T: Serialize>(body: &T) -> Event {
    Event::default()
        .event("result")
        .data(serde_json::to_string(body).expect("result payload serializes"))
}

fn step_result_ok(id: &FrameId, outcome: &StepOutcome) -> StepResult {
    StepResult {
        ok: true,
        frame_id: Some(id.as_str().to_owned()),
        outcome: Some(outcome_to_proto(outcome)),
        error: None,
    }
}

fn step_result_err(error: String) -> StepResult {
    StepResult {
        ok: false,
        frame_id: None,
        outcome: None,
        error: Some(error),
    }
}

fn outcome_to_proto(o: &StepOutcome) -> Outcome {
    match o {
        StepOutcome::Exited(agent_protocol::ExitResult::Code(c)) => {
            Outcome::Exited(Exit::Code(*c as i64))
        }
        StepOutcome::Exited(agent_protocol::ExitResult::Signal(s)) => {
            Outcome::Exited(Exit::Signal(*s as i64))
        }
        StepOutcome::SpawnFailed(e) => Outcome::SpawnFailed(e.to_string()),
        StepOutcome::WaitFailed(e) => Outcome::WaitFailed(e.to_string()),
    }
}
