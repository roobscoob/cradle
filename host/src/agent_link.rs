//! Host-side wrapper around the agent protocol.
//!
//! The agent dials OUT to the host: it connects to firecracker's
//! hybrid-vsock as a guest-initiated connection, which firecracker forwards
//! to an `AF_UNIX` listener the host pre-creates at `<jail>/vsock.sock_<port>`.
//! Unlike the host-initiated path, the guest-initiated path has NO
//! `CONNECT`/`OK` handshake — raw agent-protocol bytes flow the instant the
//! host `accept()`s.
//!
//! `AgentLink` owns one accepted `UnixStream` and bridges `AgentMessage` /
//! `HostMessage` traffic across it. Codec errors are mapped into
//! `io::Error(InvalidData)` so callers only need to handle one error type.

use std::io;

use agent_protocol::{AgentMessage, AgentMessageDecoder, HostMessage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

const READ_BUF: usize = 8 * 1024;

pub struct AgentLink {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    decoder: AgentMessageDecoder,
    read_buf: Vec<u8>,
    send_buf: Vec<u8>,
}

impl AgentLink {
    /// Wrap a guest→host `UnixStream` the host just `accept()`ed off its
    /// `vsock.sock_<port>` listener. Raw agent-protocol bytes flow
    /// immediately — no handshake to perform.
    pub fn from_stream(stream: UnixStream) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            reader,
            writer,
            decoder: AgentMessageDecoder::new(),
            read_buf: vec![0u8; READ_BUF],
            send_buf: Vec::with_capacity(READ_BUF),
        }
    }

    /// Pull the next complete `AgentMessage` from the wire. Returns
    /// `Ok(None)` on a clean peer close with no partial frame buffered.
    pub async fn recv(&mut self) -> io::Result<Option<AgentMessage>> {
        loop {
            if let Some(msg) = self.decoder.next_message().map_err(codec_to_io)? {
                return Ok(Some(msg));
            }
            let n = self.reader.read(&mut self.read_buf).await?;
            if n == 0 {
                return Ok(self.decoder.next_message().map_err(codec_to_io)?);
            }
            self.decoder.push(&self.read_buf[..n]);
        }
    }

    pub async fn send(&mut self, msg: &HostMessage) -> io::Result<()> {
        self.send_buf.clear();
        msg.encode_to(&mut self.send_buf).map_err(codec_to_io)?;
        self.writer.write_all(&self.send_buf).await
    }
}

fn codec_to_io(err: agent_protocol::CodecError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}
