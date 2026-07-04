//! Typed host↔client wire protocol for the HTTP API (SSE + WebSocket).
//!
//! Before this crate existed the host hand-built every event with `json!()`
//! and the CLI re-probed untyped `serde_json::Value`s at four call sites —
//! which is how a host `{"error": ...}` frame got silently dropped and a
//! dead `Close` arm shipped. Both sides now (de)serialize these types, so a
//! field or shape change is a compile error rather than a silently ignored
//! frame. The serde shapes are wire-compatible with the pre-crate JSON.
//!
//! Transport map:
//! - `POST /frames/{id}/step` (SSE): `phase` / `stdout` / `stderr` events
//!   carry [`PhaseEvent`] / [`DataEvent`]; the terminal `result` event
//!   carries [`StepResult`].
//! - `GET /frames/{id}/step` (WebSocket): outbound text frames are
//!   [`StepControl`] (binary frames are raw guest stdout); the client's
//!   first text frame is an [`EvalRequest`].
//! - `POST /frames/build` (SSE): `phase` / `log` events carry
//!   [`PhaseEvent`] / [`LogEvent`]; the terminal `result` carries
//!   [`BuildResult`].

use serde::{Deserialize, Serialize};

/// One step request: spawn `binary` with `argv` in `cwd` inside the guest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalRequest {
    pub binary: String,
    pub argv: Vec<String>,
    pub cwd: String,
}

/// A text frame on the step WebSocket, server → client. Externally tagged,
/// so the wire shape is a single-key object: `{"phase": "restoring"}`,
/// `{"stderr": "<base64>"}`, `{"error": "..."}`, `{"result": {...}}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepControl {
    /// Coarse lifecycle phase marker (preparing / restoring / attaching /
    /// evaluating / snapshotting).
    Phase(String),
    /// Guest-process stderr bytes, base64-encoded. Stdout rides in binary
    /// frames so an SSH-over-stdout client never sees stderr interleaved.
    Stderr(String),
    /// Pre-step failure (e.g. the first frame wasn't a valid
    /// [`EvalRequest`]). No `result` will follow.
    Error(String),
    /// Terminal event: the step finished (ok or not). Always the last
    /// meaningful frame of a session.
    Result(StepResult),
}

/// Terminal result of a step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepResult {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// What happened to the step's guest process, when the step itself succeeded
/// (`ok: true` — a child frame exists either way).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// The process ran and exited: `{"exited":{"code":0}}` or
    /// `{"exited":{"signal":9}}`.
    Exited(Exit),
    /// The agent's spawn failed before the process ran; the child frame's
    /// state is provably identical to the parent's.
    SpawnFailed(String),
    /// The process spawned and ran, but the agent's `wait()` errored — the
    /// frame WAS (potentially) mutated; only the exit status is unknown.
    WaitFailed(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exit {
    Code(i64),
    Signal(i64),
}

/// `data` payload of an SSE `phase` event: `{"name": "booting"}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseEvent {
    pub name: String,
}

/// `data` payload of an SSE `stdout` / `stderr` step event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataEvent {
    /// base64-encoded bytes.
    pub data: String,
}

/// `data` payload of a build SSE `log` event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEvent {
    /// "nix" or "boot".
    pub source: String,
    pub line: String,
}

/// `data` payload of the build SSE terminal `result` event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildResult {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The serde shapes must stay byte-compatible with the JSON the host
    /// emitted before this crate existed — old CLIs parse new hosts and
    /// vice versa.
    #[test]
    fn step_control_wire_shapes() {
        let phase = serde_json::to_value(StepControl::Phase("restoring".into())).unwrap();
        assert_eq!(phase, serde_json::json!({"phase": "restoring"}));

        let stderr = serde_json::to_value(StepControl::Stderr("aGk=".into())).unwrap();
        assert_eq!(stderr, serde_json::json!({"stderr": "aGk="}));

        let err = serde_json::to_value(StepControl::Error("bad request".into())).unwrap();
        assert_eq!(err, serde_json::json!({"error": "bad request"}));

        let result = serde_json::to_value(StepControl::Result(StepResult {
            ok: true,
            frame_id: Some("frm_x".into()),
            outcome: Some(Outcome::Exited(Exit::Code(0))),
            error: None,
        }))
        .unwrap();
        assert_eq!(
            result,
            serde_json::json!({"result": {"ok": true, "frame_id": "frm_x",
                               "outcome": {"exited": {"code": 0}}}})
        );
    }

    #[test]
    fn outcome_wire_shapes() {
        let sig = serde_json::to_value(Outcome::Exited(Exit::Signal(9))).unwrap();
        assert_eq!(sig, serde_json::json!({"exited": {"signal": 9}}));

        let spawn = serde_json::to_value(Outcome::SpawnFailed("ENOENT".into())).unwrap();
        assert_eq!(spawn, serde_json::json!({"spawn_failed": "ENOENT"}));

        let wait = serde_json::to_value(Outcome::WaitFailed("EINTR".into())).unwrap();
        assert_eq!(wait, serde_json::json!({"wait_failed": "EINTR"}));
    }

    #[test]
    fn result_parses_error_side() {
        let r: StepControl =
            serde_json::from_str(r#"{"result": {"ok": false, "error": "agent closed"}}"#).unwrap();
        match r {
            StepControl::Result(res) => {
                assert!(!res.ok);
                assert_eq!(res.error.as_deref(), Some("agent closed"));
                assert!(res.frame_id.is_none());
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn eval_request_roundtrip() {
        let e = EvalRequest {
            binary: "/bin/sh".into(),
            argv: vec!["-c".into(), "ls".into()],
            cwd: "/".into(),
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: EvalRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.binary, e.binary);
        assert_eq!(back.argv, e.argv);
        assert_eq!(back.cwd, e.cwd);
    }
}
