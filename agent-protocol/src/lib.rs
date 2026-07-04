//! Sans-IO protocol shared by the agent and host.
//!
//! Messages are serialized with `postcard` and framed with COBS, so every
//! frame on the wire ends in a `0x00` sentinel byte. The crate is sans-io:
//! encoders append to a caller-owned `Vec<u8>`, and decoders are stateful
//! buffers that yield complete messages as bytes are pushed in. That keeps
//! the transport (vsock, pipe, tokio stream, std reader, ...) out of scope.

use serde::{Deserialize, Serialize};
use std::io;

pub use postcard::Error as CodecError;

/// vsock addressing shared by the host and the agent. Defined once here so
/// the two sides can't drift: the agent dials out to (VSOCK_HOST_CID,
/// VSOCK_HOST_PORT); firecracker forwards that to the host's AF_UNIX
/// listener at `<vsock_uds>_<port>`.
pub const VSOCK_HOST_CID: u32 = 2;
pub const VSOCK_HOST_PORT: u32 = 1024;
pub const VSOCK_GUEST_CID: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Stream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitResult {
    Code(i32),
    Signal(i32),
}

/// Messages emitted by the agent and consumed by the host.
///
/// Connection lifecycle (agent-dials-host model): the agent connects out to
/// the host's vsock listener, sends `Hello` as the first message to confirm
/// the byte path works, then waits for `HostMessage::Eval`. The agent emits
/// periodic `Heartbeat`s on the connection; the host ignores them, but the
/// *write* failing is how the agent detects that a snapshot/restore has
/// killed the connection (reads don't wake on the guest kernel's
/// TRANSPORT_RESET, but writes return EPIPE). On a failed heartbeat write
/// the agent reconnects, and the host's `accept()` of the new connection is
/// the "agent is alive post-restore" signal.
#[derive(Debug, Serialize, Deserialize)]
pub enum AgentMessage {
    StreamChunk {
        stream: Stream,
        data: Vec<u8>,
    },
    ProcessExit(ExitResult),
    ProcessErr(#[serde(with = "io_error_serde")] io::Error),
    /// Sent by the agent as the first message after connecting, to confirm
    /// the byte path works (not "born dead" from a TRANSPORT_RESET race).
    Hello,
    /// Periodic liveness write. The host discards it; the agent uses the
    /// write succeeding/failing as its dead-connection detector.
    Heartbeat,
    /// The child was spawned and ran, but `wait()` errored afterwards. Unlike
    /// `ProcessErr` (spawn-time failure, VM state provably unchanged), the
    /// process DID execute and may have mutated the frame — the host must not
    /// report this as "spawn failed". Appended last so older decoders keep
    /// their postcard variant indices.
    ProcessWaitErr(#[serde(with = "io_error_serde")] io::Error),
}

/// Messages emitted by the host and consumed by the agent.
///
/// `Stdin`, `StdinClose`, and `Kill` only apply while an `Eval` is in flight.
/// They are silently dropped if no child is currently running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HostMessage {
    Eval {
        pwd_path: String,
        binary_path: String,
        argv: Vec<String>,
    },
    Stdin(Vec<u8>),
    StdinClose,
    Kill,
}

impl AgentMessage {
    /// Encode and append a single COBS-framed message to `out`.
    pub fn encode_to(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        encode_into(self, out)
    }
}

impl HostMessage {
    /// Encode and append a single COBS-framed message to `out`.
    pub fn encode_to(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        encode_into(self, out)
    }
}

fn encode_into<T: Serialize + ?Sized>(value: &T, out: &mut Vec<u8>) -> Result<(), CodecError> {
    let frame = postcard::to_allocvec_cobs(value)?;
    out.extend_from_slice(&frame);
    Ok(())
}

// ---------------------------------------------------------------------------
// Sans-IO decoders
// ---------------------------------------------------------------------------

/// Buffers bytes from the wire and yields complete messages of type `T`.
/// One implementation for both directions — see the aliases below.
#[derive(Debug)]
pub struct Decoder<T> {
    buf: Vec<u8>,
    /// Bytes already scanned for a sentinel without finding one. Lets each
    /// `next_message` resume the scan where the last one stopped instead of
    /// rescanning the whole partial frame from offset 0 on every push
    /// (O(N²) on a large frame delivered in many reads).
    scanned: usize,
    _marker: std::marker::PhantomData<T>,
}

/// Buffers bytes from the wire and yields complete `AgentMessage`s.
pub type AgentMessageDecoder = Decoder<AgentMessage>;
/// Buffers bytes from the wire and yields complete `HostMessage`s.
pub type HostMessageDecoder = Decoder<HostMessage>;

impl<T> Default for Decoder<T> {
    fn default() -> Self {
        Self {
            buf: Vec::new(),
            scanned: 0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: for<'de> Deserialize<'de>> Decoder<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append more bytes received from the wire.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Try to decode the next complete message. Returns `Ok(None)` when no
    /// full COBS frame has arrived yet.
    pub fn next_message(&mut self) -> Result<Option<T>, CodecError> {
        let Some(rel_idx) = self.buf[self.scanned..].iter().position(|&b| b == 0) else {
            self.scanned = self.buf.len();
            return Ok(None);
        };
        let sentinel_idx = self.scanned + rel_idx;
        // Drain through the sentinel, then drop the trailing 0x00 because
        // postcard's COBS decoder operates on the encoded bytes only.
        let mut frame: Vec<u8> = self.buf.drain(..=sentinel_idx).collect();
        frame.pop();
        self.scanned = 0;
        let msg = postcard::from_bytes_cobs::<T>(&mut frame)?;
        Ok(Some(msg))
    }
}

// ---------------------------------------------------------------------------
// io::Error <-> serde proxy
// ---------------------------------------------------------------------------

mod io_error_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::borrow::Cow;
    use std::io;

    #[derive(Serialize, Deserialize)]
    struct Repr<'a> {
        kind: u8,
        message: Cow<'a, str>,
    }

    pub fn serialize<S: Serializer>(err: &io::Error, ser: S) -> Result<S::Ok, S::Error> {
        Repr {
            kind: kind_to_u8(err.kind()),
            message: Cow::Owned(err.to_string()),
        }
        .serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<io::Error, D::Error> {
        let Repr { kind, message } = Repr::deserialize(de)?;
        Ok(io::Error::new(u8_to_kind(kind), message.into_owned()))
    }

    // Intentionally lossy: unknown kinds map to `Other` on both sides so
    // additions to `io::ErrorKind` cannot break the wire format.
    fn kind_to_u8(kind: io::ErrorKind) -> u8 {
        use io::ErrorKind::*;
        match kind {
            NotFound => 1,
            PermissionDenied => 2,
            ConnectionRefused => 3,
            ConnectionReset => 4,
            ConnectionAborted => 5,
            NotConnected => 6,
            AddrInUse => 7,
            AddrNotAvailable => 8,
            BrokenPipe => 9,
            AlreadyExists => 10,
            WouldBlock => 11,
            InvalidInput => 12,
            InvalidData => 13,
            TimedOut => 14,
            WriteZero => 15,
            Interrupted => 16,
            Unsupported => 17,
            UnexpectedEof => 18,
            OutOfMemory => 19,
            _ => 0,
        }
    }

    fn u8_to_kind(tag: u8) -> io::ErrorKind {
        use io::ErrorKind::*;
        match tag {
            1 => NotFound,
            2 => PermissionDenied,
            3 => ConnectionRefused,
            4 => ConnectionReset,
            5 => ConnectionAborted,
            6 => NotConnected,
            7 => AddrInUse,
            8 => AddrNotAvailable,
            9 => BrokenPipe,
            10 => AlreadyExists,
            11 => WouldBlock,
            12 => InvalidInput,
            13 => InvalidData,
            14 => TimedOut,
            15 => WriteZero,
            16 => Interrupted,
            17 => Unsupported,
            18 => UnexpectedEof,
            19 => OutOfMemory,
            _ => Other,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_agent(msg: AgentMessage) -> AgentMessage {
        let mut buf = Vec::new();
        msg.encode_to(&mut buf).unwrap();
        let mut dec = AgentMessageDecoder::new();
        dec.push(&buf);
        dec.next_message().unwrap().unwrap()
    }

    fn roundtrip_host(msg: HostMessage) -> HostMessage {
        let mut buf = Vec::new();
        msg.encode_to(&mut buf).unwrap();
        let mut dec = HostMessageDecoder::new();
        dec.push(&buf);
        dec.next_message().unwrap().unwrap()
    }

    #[test]
    fn stream_chunk_roundtrip() {
        let msg = AgentMessage::StreamChunk {
            stream: Stream::Stderr,
            data: vec![0, 1, 2, 255, 0xab, 0, 0, 0],
        };
        match roundtrip_agent(msg) {
            AgentMessage::StreamChunk { stream, data } => {
                assert_eq!(stream, Stream::Stderr);
                assert_eq!(data, vec![0, 1, 2, 255, 0xab, 0, 0, 0]);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn hello_roundtrip() {
        match roundtrip_agent(AgentMessage::Hello) {
            AgentMessage::Hello => {}
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn process_exit_roundtrip() {
        for r in [
            ExitResult::Code(0),
            ExitResult::Code(-1),
            ExitResult::Signal(9),
        ] {
            match roundtrip_agent(AgentMessage::ProcessExit(r)) {
                AgentMessage::ProcessExit(got) => assert_eq!(got, r),
                other => panic!("wrong variant: {other:?}"),
            }
        }
    }

    #[test]
    fn process_err_roundtrip() {
        let err = io::Error::new(io::ErrorKind::PermissionDenied, "nope");
        match roundtrip_agent(AgentMessage::ProcessErr(err)) {
            AgentMessage::ProcessErr(got) => {
                assert_eq!(got.kind(), io::ErrorKind::PermissionDenied);
                assert_eq!(got.to_string(), "nope");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn process_wait_err_roundtrip() {
        let err = io::Error::new(io::ErrorKind::Interrupted, "wait interrupted");
        match roundtrip_agent(AgentMessage::ProcessWaitErr(err)) {
            AgentMessage::ProcessWaitErr(got) => {
                assert_eq!(got.kind(), io::ErrorKind::Interrupted);
                assert_eq!(got.to_string(), "wait interrupted");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn eval_roundtrip() {
        let msg = HostMessage::Eval {
            pwd_path: "/work".into(),
            binary_path: "/usr/bin/echo".into(),
            argv: vec!["echo".into(), "hello world".into(), String::new()],
        };
        assert_eq!(roundtrip_host(msg.clone()), msg);
    }

    #[test]
    fn eval_empty_argv_roundtrip() {
        let msg = HostMessage::Eval {
            pwd_path: "/".into(),
            binary_path: "/bin/true".into(),
            argv: vec![],
        };
        assert_eq!(roundtrip_host(msg.clone()), msg);
    }

    #[test]
    fn stdin_kill_close_roundtrip() {
        for msg in [
            HostMessage::Stdin(b"hello\n".to_vec()),
            HostMessage::Stdin(Vec::new()),
            HostMessage::StdinClose,
            HostMessage::Kill,
        ] {
            assert_eq!(roundtrip_host(msg.clone()), msg);
        }
    }

    #[test]
    fn decoder_handles_partial_then_full_frames() {
        let mut buf = Vec::new();
        AgentMessage::StreamChunk {
            stream: Stream::Stdout,
            data: b"hi".to_vec(),
        }
        .encode_to(&mut buf)
        .unwrap();
        AgentMessage::ProcessExit(ExitResult::Code(0))
            .encode_to(&mut buf)
            .unwrap();

        let mut dec = AgentMessageDecoder::new();
        let mut yielded = Vec::new();
        for chunk in buf.chunks(1) {
            dec.push(chunk);
            while let Some(m) = dec.next_message().unwrap() {
                yielded.push(m);
            }
        }
        assert_eq!(yielded.len(), 2);
        match &yielded[0] {
            AgentMessage::StreamChunk { stream, data } => {
                assert_eq!(*stream, Stream::Stdout);
                assert_eq!(data, b"hi");
            }
            other => panic!("wrong variant: {other:?}"),
        }
        assert!(matches!(yielded[1], AgentMessage::ProcessExit(ExitResult::Code(0))));
    }

    #[test]
    fn decoder_yields_none_on_empty() {
        let mut dec = AgentMessageDecoder::new();
        assert!(dec.next_message().unwrap().is_none());
        dec.push(&[0x42]); // partial frame, no sentinel yet
        assert!(dec.next_message().unwrap().is_none());
    }
}
