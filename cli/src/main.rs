//! cradle CLI: two-phase client.
//!
//! Phase 1 runs the initial `/frames/build` SSE stream as plain stdout —
//! a self-overwriting `[<phase>] <log>` line that ends with a newline once
//! the frame is ready. No alt-screen takeover, so the build log stays in
//! the user's real terminal scrollback after `:q`.
//!
//! Phase 2 enters alt-screen ratatui and runs the REPL: each command the
//! user submits becomes one transcript entry. While a step is running, its
//! `$ <cmd>` header is pinned as a sticky top row; output streams into an
//! embedded vt100 widget; the entry collapses to a finalized form on
//! completion. See the plan file for the full design.

use std::{collections::VecDeque, path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use crossterm::event::{
    Event as CtEvent, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};
use futures_util::StreamExt;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Paragraph, Wrap},
};
use serde_json::Value;
use tokio::sync::mpsc;

mod app;
mod session;
mod sse;
mod tarball;
mod ui;

use app::{App, Entry, FooterLine, Mode, render_row};
use session::{Session, SessionEvent};
use sse::{Event as SseEvent, post_sse};

/// vt100 row width used to interpret a single log line's ANSI styles.
/// Generous so even a long line fits without wrapping inside the parser —
/// ratatui handles visual wrap at render time.
const BUILD_VT_COLS: u16 = 2048;

#[derive(Parser, Debug)]
#[command(name = "cradle", about = "Interactive client for the cradle host")]
struct Args {
    /// Path to a flake directory to upload as the guest image. The directory
    /// is tar+gzipped client-side and POSTed to `/frames/build`. If omitted,
    /// the host's default storeDisk is used.
    #[arg(long)]
    machine: Option<PathBuf>,
    /// Base URL of the cradle host, e.g. `http://localhost:8080`.
    #[arg(long)]
    host: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let client = reqwest::Client::builder()
        .build()
        .context("build reqwest client")?;

    // Fire the initial /frames/build POST *before* entering alt-screen so
    // any connect / tarball errors print as plain stderr the user can read
    // alongside their normal shell history.
    let build_form = build_request_form(args.machine.as_deref())?;
    let build_url = format!("{}/frames/build", args.host.trim_end_matches('/'));
    let initial_sse = post_sse(&client, &build_url, build_form)
        .await
        .context("initial /frames/build POST")?;

    // One alt-screen ratatui terminal for the whole session: a build view
    // first (sticky `=== <phase> ===` bar with tailing logs below), then
    // transition into the REPL with the same terminal once the build
    // succeeds. The guard restores the terminal on every exit path.
    //
    // We deliberately do NOT enable mouse capture: with `alternateScroll`
    // (the default in Windows Terminal, iTerm2, Konsole, Alacritty, etc.)
    // the terminal translates scroll-wheel events to Up/Down keys when in
    // alt-screen, so we get app-scrolling for free while leaving native
    // mouse selection / copy / paste fully functional.
    let _restore_guard = RawGuard;
    let mut terminal = ratatui::init();

    let mut keys = EventStream::new();
    let session_image = run_build_phase(&mut terminal, &mut keys, initial_sse).await?;
    run_repl(&mut terminal, &mut keys, &args.host, session_image).await
}

/// Guarantee `ratatui::restore()` runs on every exit path (normal return,
/// error, panic). Must be created BEFORE `ratatui::init()` so it drops
/// after the terminal does its own cleanup.
struct RawGuard;

impl Drop for RawGuard {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

/// Cap on log lines retained in memory during the build phase. Plenty for
/// real nix builds; the cap just guarantees we don't grow without bound on
/// a runaway log producer.
const MAX_BUILD_LOG_LINES: usize = 5000;

struct BuildState {
    phase: Option<String>,
    /// Pre-parsed log lines with ANSI styles already resolved into ratatui
    /// `Span` styling — `\x1b[1;32m<<< NixOS Stage 1 >>>\x1b[0m` becomes a
    /// bold-green span instead of being shown as literal escape text.
    log_lines: VecDeque<Line<'static>>,
    /// Visual rows scrolled up from the live (bottom) edge. 0 = bottom-anchored.
    scroll: u16,
}

/// Drive the initial `/frames/build` SSE stream inside the alt-screen
/// terminal. Renders a sticky `=== <phase> ===` bar pinned to the top row
/// with the most recent log lines tailing below. Returns the new frame's
/// id on success.
async fn run_build_phase(
    terminal: &mut DefaultTerminal,
    keys: &mut EventStream,
    mut sse_rx: mpsc::UnboundedReceiver<Result<SseEvent>>,
) -> Result<String> {
    let mut state = BuildState {
        phase: None,
        log_lines: VecDeque::new(),
        scroll: 0,
    };

    loop {
        terminal
            .draw(|f| draw_build(f, &mut state))
            .context("draw build frame")?;

        let (_, term_rows) = crossterm::terminal::size().unwrap_or((80, 24));

        tokio::select! {
            biased;

            evt = keys.next() => {
                match evt {
                    Some(Ok(CtEvent::Key(key))) if key.kind == KeyEventKind::Press => {
                        if key.modifiers.contains(KeyModifiers::CONTROL)
                            && matches!(key.code, KeyCode::Char('c'))
                        {
                            return Err(anyhow!("cancelled by user during build"));
                        }
                        // Scroll. Up/Down come from terminals translating
                        // mouse-wheel events in alt-screen mode.
                        let half = term_rows.saturating_sub(2).max(1) / 2;
                        match key.code {
                            KeyCode::Up => {
                                state.scroll = state.scroll.saturating_add(1);
                            }
                            KeyCode::Down => {
                                state.scroll = state.scroll.saturating_sub(1);
                            }
                            KeyCode::PageUp => {
                                state.scroll = state.scroll.saturating_add(half.max(1));
                            }
                            KeyCode::PageDown => {
                                state.scroll = state.scroll.saturating_sub(half.max(1));
                            }
                            KeyCode::Home => {
                                state.scroll = u16::MAX;
                            }
                            KeyCode::End => {
                                state.scroll = 0;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    _ => {}
                }
            }

            item = sse_rx.recv() => {
                let Some(item) = item else {
                    return Err(anyhow!("build SSE stream ended without a result event"));
                };
                let ev = item.context("SSE stream error during build")?;
                match ev.name.as_str() {
                    "phase" => {
                        if let Ok(v) = serde_json::from_str::<Value>(&ev.data) {
                            if let Some(name) = v.get("name").and_then(|s| s.as_str()) {
                                state.phase = Some(name.to_owned());
                            }
                        }
                    }
                    "log" => {
                        if let Ok(v) = serde_json::from_str::<Value>(&ev.data) {
                            if let Some(line) = v.get("line").and_then(|s| s.as_str()) {
                                push_log(&mut state, line);
                            }
                        }
                    }
                    "ready" => {}
                    "result" => {
                        if let Ok(v) = serde_json::from_str::<Value>(&ev.data) {
                            let ok = v.get("ok").and_then(|b| b.as_bool()).unwrap_or(false);
                            if !ok {
                                let err = v
                                    .get("error")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or("?")
                                    .to_owned();
                                return Err(anyhow!("build failed: {err}"));
                            }
                            let frame_id = v
                                .get("frame_id")
                                .and_then(|s| s.as_str())
                                .context("build result missing frame_id")?
                                .to_owned();
                            return Ok(frame_id);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn push_log(state: &mut BuildState, line: &str) {
    while state.log_lines.len() >= MAX_BUILD_LOG_LINES {
        state.log_lines.pop_front();
    }
    state.log_lines.push_back(parse_log_line(line));
}

/// Run a single log line through a 1-row vt100 parser so embedded ANSI
/// escape sequences (SGR colors, bold, italic, etc.) resolve into ratatui
/// `Span` styles instead of being shown as literal escape text. Strips
/// embedded `\r`/`\n` first since vt100 with rows=1 + no scrollback would
/// otherwise lose any content following them.
fn parse_log_line(line: &str) -> Line<'static> {
    let sanitized: String = line.chars().filter(|c| *c != '\r' && *c != '\n').collect();
    // 2 rows, not 1: vt100 0.16.2's `col_wrap` underflows on a 1-row grid
    // when a long line exceeds BUILD_VT_COLS. We only read row 0 anyway.
    let mut parser = vt100::Parser::new(2, BUILD_VT_COLS, 0);
    parser.process(sanitized.as_bytes());
    render_row(parser.screen(), 0, BUILD_VT_COLS)
}

fn draw_build(f: &mut Frame, state: &mut BuildState) {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(area);

    let phase = state.phase.as_deref().unwrap_or("starting");
    let bar = Paragraph::new(Line::from(Span::styled(
        format!("=== {phase} ==="),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )))
    .alignment(Alignment::Center);
    f.render_widget(bar, chunks[0]);

    let log_area = chunks[1];
    if log_area.height == 0 {
        return;
    }

    // Total wrap-rows of all logs (saturated to u32 to avoid u16 overflow on
    // pathological builds; we clamp into u16 at the very end for scroll).
    let total_rows: u32 = state
        .log_lines
        .iter()
        .map(|l| wrap_rows_styled(l, log_area.width) as u32)
        .sum();
    let visible_rows = log_area.height as u32;

    let lines: Vec<Line<'static>> = state.log_lines.iter().cloned().collect();

    if total_rows <= visible_rows {
        // Everything fits — pad the top so logs sit flush against the bottom.
        // Force scroll back to 0 so subsequent scroll-down doesn't have to
        // burn through stale accumulated value before responding.
        state.scroll = 0;
        let padding = (visible_rows - total_rows) as u16;
        let mut padded: Vec<Line> = Vec::with_capacity(padding as usize + lines.len());
        for _ in 0..padding {
            padded.push(Line::raw(""));
        }
        padded.extend(lines);
        let para = Paragraph::new(padded).wrap(Wrap { trim: false });
        f.render_widget(para, log_area);
    } else {
        // Overflowing — clamp the user's scroll to the actual max and write
        // it back so further key presses act on the clamped value (no "type
        // through stale buffer" feel when scrolling past the top).
        let max_scroll_up = total_rows - visible_rows;
        let clamped = (state.scroll as u32).min(max_scroll_up);
        state.scroll = clamped.min(u16::MAX as u32) as u16;
        let scroll_y = ((max_scroll_up - clamped).min(u16::MAX as u32)) as u16;
        let para = Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .scroll((scroll_y, 0));
        f.render_widget(para, log_area);
    }
}

/// Wrap-row cost of a styled `Line`, counted in visual cells. Walks every
/// span's content rather than the line's literal byte length.
fn wrap_rows_styled(line: &Line<'_>, width: u16) -> u16 {
    if width == 0 {
        return 1;
    }
    let n: usize = line.spans.iter().map(|s| s.content.chars().count()).sum();
    if n == 0 {
        return 1;
    }
    let w = width as usize;
    ((n + w - 1) / w).min(u16::MAX as usize) as u16
}

/// Build a multipart form for the upload, or `None` if `--machine` wasn't
/// provided (server falls back to its default storeDisk).
fn build_request_form(
    machine: Option<&std::path::Path>,
) -> Result<Option<reqwest::multipart::Form>> {
    let Some(dir) = machine else { return Ok(None) };
    if !dir.is_dir() {
        return Err(anyhow!("--machine {} is not a directory", dir.display()));
    }
    let bytes = tarball::pack_dir(dir).context("packing flake directory")?;
    let part = reqwest::multipart::Part::bytes(bytes)
        .file_name("flake.tgz")
        .mime_str("application/gzip")
        .context("building multipart part")?;
    let form = reqwest::multipart::Form::new().part("flake", part);
    Ok(Some(form))
}

async fn run_repl(
    terminal: &mut DefaultTerminal,
    keys: &mut EventStream,
    host: &str,
    session_image: String,
) -> Result<()> {
    let mut app = App::new(session_image);
    // Active step session, if any. While `Some`, its `events` receiver
    // is the source of truth for the running entry. Drop to cancel — the
    // Session's Drop impl aborts the WS bridge tasks, the WS closes, and
    // the host hard-kills the VM.
    let mut session: Option<Session> = None;
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<String>();
    // Last terminal size we told the active session about, so we only send
    // a winsize frame when it actually changes.
    let mut last_size: Option<(u16, u16)> = None;

    loop {
        terminal
            .draw(|f| ui::draw(f, &mut app))
            .context("draw frame")?;
        if matches!(app.mode, Mode::Quit) {
            break;
        }

        let (term_cols, term_rows) = crossterm::terminal::size().unwrap_or((80, 24));

        // Push terminal resizes through to the running command's PTY (the
        // bridge TIOCSWINSZ's it → SIGWINCH → the program reflows). Only on
        // change, and only while a session is live.
        if let Some(s) = session.as_ref() {
            if last_size != Some((term_cols, term_rows)) {
                s.send_resize(term_rows, term_cols);
                last_size = Some((term_cols, term_rows));
            }
        }

        tokio::select! {
            biased;

            evt = keys.next() => {
                let Some(evt) = evt else { break; };
                let evt = evt.context("read terminal event")?;
                match handle_event(&mut app, evt, term_rows) {
                    EventAction::None => {}
                    EventAction::SubmitCommand(cmd) => {
                        let _ = cmd_tx.send(cmd);
                    }
                    EventAction::SendInput(bytes) => {
                        if let Some(s) = session.as_ref() {
                            s.send_input(&bytes);
                        }
                    }
                    EventAction::AbortRunning => {
                        // Drop the session — closes the WS, host hard-kills VM.
                        session = None;
                        let duration = running_duration(&app);
                        app.finalize_running(FooterLine::Aborted { duration });
                    }
                    EventAction::Quit => {
                        app.mode = Mode::Quit;
                    }
                }
            }

            Some(ev) = recv_maybe(session.as_mut().map(|s| &mut s.events)) => {
                match ev {
                    Some(ev) => {
                        let is_result = matches!(ev, SessionEvent::Result(_));
                        app.handle_session_event(ev);
                        if is_result {
                            // handle_session_event already finalized the
                            // running entry and flipped mode back to Idle;
                            // drop the now-dead session so its background
                            // tasks can wind down.
                            session = None;
                        }
                    }
                    None => {
                        // Session events channel closed without a Result —
                        // bridge tasks dropped their senders prematurely
                        // (WS error, abort, etc). If we're still Running,
                        // synthesize an error footer.
                        if matches!(app.mode, Mode::Running) {
                            app.finalize_running(FooterLine::Error {
                                message: "session closed without result".into(),
                            });
                        }
                        session = None;
                    }
                }
            }

            Some(cmd) = cmd_rx.recv() => {
                let parent = app.last_frame.as_ref().unwrap_or(&app.session_image).clone();
                app.start_step(cmd.clone(), term_cols, term_rows);
                match session::open(host, &parent, &cmd, term_rows, term_cols) {
                    Ok(s) => {
                        app.mode = Mode::Running;
                        session = Some(s);
                        // The bridge already got this size via its args, so
                        // seed last_size to avoid an immediate redundant
                        // resize frame; only later changes get pushed.
                        last_size = Some((term_cols, term_rows));
                    }
                    Err(e) => {
                        app.finalize_running(FooterLine::Error {
                            message: format!("dispatch: {e}"),
                        });
                    }
                }
            }

            _ = tokio::time::sleep(Duration::from_millis(250)) => {
                app.tick_spinner();
            }
        }
    }

    Ok(())
}

fn running_duration(app: &App) -> Duration {
    match app.entries.last() {
        Some(Entry::Running { started, .. }) => started.elapsed(),
        _ => Duration::default(),
    }
}

/// `select!`-friendly recv that resolves immediately to `None` when there's
/// no receiver — yields the branch right back to the executor instead of
/// blocking forever.
async fn recv_maybe<T>(rx: Option<&mut mpsc::UnboundedReceiver<T>>) -> Option<Option<T>> {
    match rx {
        Some(rx) => Some(rx.recv().await),
        None => None,
    }
}

enum EventAction {
    None,
    SubmitCommand(String),
    /// Forward these bytes to the running command's PTY stdin.
    SendInput(Vec<u8>),
    AbortRunning,
    Quit,
}

fn handle_event(app: &mut App, evt: CtEvent, term_rows: u16) -> EventAction {
    match evt {
        CtEvent::Key(key) if key.kind == KeyEventKind::Press => handle_key(app, key, term_rows),
        _ => EventAction::None,
    }
}

fn handle_key(app: &mut App, key: KeyEvent, term_rows: u16) -> EventAction {
    // Ctrl-C is always interpreted regardless of mode. While a step runs it
    // is the hard-abort hatch (drop the session → host hard-kills the VM),
    // NOT forwarded to the program as SIGINT — keeps a reliable escape from
    // a wedged command. (Forwarding ^C as 0x03 is a possible future toggle.)
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        return match app.mode {
            Mode::Running => EventAction::AbortRunning,
            Mode::Idle if app.input.is_empty() => EventAction::Quit,
            Mode::Idle => {
                app.input.clear();
                app.cursor = 0;
                EventAction::None
            }
            Mode::Quit => EventAction::None,
        };
    }

    // PageUp/PageDown scroll the transcript in ANY mode — reserved for
    // reviewing past output even mid-command. (Programs rarely need them,
    // and it keeps a way to scroll back while something interactive runs.)
    match key.code {
        KeyCode::PageUp => {
            let step = term_rows.saturating_sub(1).max(1) / 2;
            app.scroll = app.scroll.saturating_add(step.max(1));
            return EventAction::None;
        }
        KeyCode::PageDown => {
            let step = term_rows.saturating_sub(1).max(1) / 2;
            app.scroll = app.scroll.saturating_sub(step.max(1));
            return EventAction::None;
        }
        _ => {}
    }

    // While a step runs, forward keystrokes to the command's PTY so
    // interactive programs work (incl. arrow keys, which therefore go to
    // the program rather than scrolling the transcript — use PgUp/PgDn to
    // scroll mid-command). Wheel events arrive as Up/Down and likewise go
    // to the program, matching how a real terminal behaves.
    if matches!(app.mode, Mode::Running) {
        return match key_to_bytes(&key) {
            Some(bytes) => EventAction::SendInput(bytes),
            None => EventAction::None,
        };
    }

    // Idle: Up/Down scroll the transcript (wheel events in alt-screen).
    match key.code {
        KeyCode::Up => {
            app.scroll = app.scroll.saturating_add(1);
            return EventAction::None;
        }
        KeyCode::Down => {
            app.scroll = app.scroll.saturating_sub(1);
            return EventAction::None;
        }
        _ => {}
    }

    // Idle editing keys.
    if !matches!(app.mode, Mode::Idle) {
        return EventAction::None;
    }

    match key.code {
        KeyCode::Enter => {
            let cmd = std::mem::take(&mut app.input);
            app.cursor = 0;
            let trimmed = cmd.trim();
            if trimmed.is_empty() {
                return EventAction::None;
            }
            if trimmed == ":q" || trimmed == ":quit" {
                return EventAction::Quit;
            }
            app.scroll = 0;
            EventAction::SubmitCommand(cmd)
        }
        KeyCode::Backspace => {
            if app.cursor > 0 {
                app.cursor -= 1;
                app.input.remove(app.cursor);
            }
            app.scroll = 0;
            EventAction::None
        }
        KeyCode::Char(c) => {
            app.input.insert(app.cursor, c);
            app.cursor += 1;
            app.scroll = 0;
            EventAction::None
        }
        KeyCode::Left => {
            app.cursor = app.cursor.saturating_sub(1);
            EventAction::None
        }
        KeyCode::Right => {
            if app.cursor < app.input.len() {
                app.cursor += 1;
            }
            EventAction::None
        }
        KeyCode::Home => {
            app.cursor = 0;
            EventAction::None
        }
        KeyCode::End => {
            app.cursor = app.input.len();
            EventAction::None
        }
        _ => EventAction::None,
    }
}

/// Encode a key press into the bytes a terminal would send for it, for
/// forwarding to the running command's PTY. Covers the common cases;
/// returns `None` for keys we don't translate (they're simply not
/// forwarded). Ctrl-C is handled by the caller before this is reached.
fn key_to_bytes(key: &KeyEvent) -> Option<Vec<u8>> {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char(c) => {
            if ctrl {
                // Map Ctrl-<char> to its C0 control byte: ^@=0 … ^_=31.
                let upper = (c as u8).to_ascii_uppercase();
                if (b'@'..=b'_').contains(&upper) {
                    Some(vec![upper - b'@'])
                } else if c == ' ' {
                    Some(vec![0])
                } else {
                    Some(c.to_string().into_bytes())
                }
            } else {
                Some(c.to_string().into_bytes())
            }
        }
        KeyCode::Enter => Some(vec![b'\r']),
        KeyCode::Backspace => Some(vec![0x7f]),
        KeyCode::Tab => Some(vec![b'\t']),
        KeyCode::BackTab => Some(b"\x1b[Z".to_vec()),
        KeyCode::Esc => Some(vec![0x1b]),
        KeyCode::Up => Some(b"\x1b[A".to_vec()),
        KeyCode::Down => Some(b"\x1b[B".to_vec()),
        KeyCode::Right => Some(b"\x1b[C".to_vec()),
        KeyCode::Left => Some(b"\x1b[D".to_vec()),
        KeyCode::Home => Some(b"\x1b[H".to_vec()),
        KeyCode::End => Some(b"\x1b[F".to_vec()),
        KeyCode::Delete => Some(b"\x1b[3~".to_vec()),
        KeyCode::Insert => Some(b"\x1b[2~".to_vec()),
        _ => None,
    }
}

