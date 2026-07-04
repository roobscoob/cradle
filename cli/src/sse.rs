//! Minimal SSE client built on `reqwest`.
//!
//! We hand-roll the parser because the wire format is small. The host emits
//! one `data:` line per event, so the parser is essentially "split on
//! `\n\n`, then extract `event:` and `data:` lines."

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use tokio::sync::mpsc;

/// One SSE event as it arrives off the wire.
#[derive(Debug, Clone)]
pub struct Event {
    pub name: String,
    pub data: String,
}

/// Returns:
/// - `None` for pure keep-alive / comment frames (axum emits `: \n\n` every
///   ~15s on idle SSE streams; we silently skip these).
/// - `Some(Ok(_))` for a real, well-formed event.
/// - `Some(Err(_))` for a frame that had `data:` lines but no `event:` line
///   (shouldn't happen from our host, but surface it instead of guessing).
fn parse_event(text: &str) -> Option<Result<Event>> {
    let mut name = String::new();
    let mut data = String::new();
    let mut saw_meaningful = false;
    for line in text.lines() {
        // Per the SSE spec the space after the field colon is OPTIONAL —
        // require only the field name and strip at most one leading space.
        if let Some(rest) = line.strip_prefix("event:") {
            name = rest.strip_prefix(' ').unwrap_or(rest).to_owned();
            saw_meaningful = true;
        } else if let Some(rest) = line.strip_prefix("data:") {
            let rest = rest.strip_prefix(' ').unwrap_or(rest);
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest);
            saw_meaningful = true;
        }
        // Lines starting with `:` (comments / keep-alives) and blank
        // continuation lines are ignored.
    }
    if !saw_meaningful {
        return None;
    }
    if name.is_empty() {
        return Some(Err(anyhow!("SSE event has no `event:` field")));
    }
    Some(Ok(Event { name, data }))
}

/// POST to `url` (multipart-form-style body) and stream the response as
/// parsed SSE events. The returned stream ends when the server closes the
/// connection.
pub async fn post_sse(
    client: &reqwest::Client,
    url: &str,
    body: Option<reqwest::multipart::Form>,
) -> Result<mpsc::UnboundedReceiver<Result<Event>>> {
    let mut req = client.post(url);
    if let Some(form) = body {
        req = req.multipart(form);
    }
    response_to_events(req.send().await.context("POST request failed")?).await
}

async fn response_to_events(
    resp: reqwest::Response,
) -> Result<mpsc::UnboundedReceiver<Result<Event>>> {
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("HTTP {status}: {body}"));
    }

    let (tx, rx) = mpsc::unbounded_channel::<Result<Event>>();
    let mut bytes = resp.bytes_stream();

    tokio::spawn(async move {
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        while let Some(chunk) = bytes.next().await {
            match chunk {
                Ok(b) => {
                    buf.extend_from_slice(&b);
                    // Drain any complete events from the buffer. Events end
                    // at a blank line — LF-LF from our host, but CRLF-CRLF
                    // is equally spec-legal, so accept both (whichever
                    // terminator appears first).
                    loop {
                        let lf = buf.windows(2).position(|w| w == b"\n\n").map(|p| (p, 2));
                        let crlf = buf
                            .windows(4)
                            .position(|w| w == b"\r\n\r\n")
                            .map(|p| (p, 4));
                        let Some((term, tlen)) =
                            [lf, crlf].into_iter().flatten().min_by_key(|&(p, _)| p)
                        else {
                            break;
                        };
                        let raw = buf.drain(..term + tlen).collect::<Vec<u8>>();
                        let text = String::from_utf8_lossy(&raw[..term]).into_owned();
                        // `None` = keep-alive comment; skip silently.
                        let Some(event) = parse_event(&text) else { continue };
                        if tx.send(event).is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(anyhow::Error::from(e)));
                    return;
                }
            }
        }
        if !buf.is_empty() {
            let _ = tx.send(Err(anyhow!(
                "stream ended with {} bytes of unterminated SSE data",
                buf.len()
            )));
        }
    });

    Ok(rx)
}
