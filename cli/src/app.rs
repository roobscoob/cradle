//! REPL state for the cradle CLI.
//!
//! After Phase 1 (build) succeeds and the alt-screen TUI takes over, the
//! whole screen is a transcript of *entries*: each entry is one shell command
//! the user submitted, its output, and a dim footer with the resulting frame
//! id and timing. While a step is running, its `$ <cmd>` header is pinned as
//! a sticky row at the top of the screen and its output streams into an
//! embedded `tui-term::PseudoTerminal` widget. When the step completes, the
//! `vt100::Parser` is walked into a `Vec<Line>` and the entry rejoins the
//! flow of the transcript — visually identical to any past entry.
//!
//! See `C:\Users\rose\.claude\plans\mighty-sprouting-hedgehog.md` for the
//! full design.

use std::time::{Duration, Instant};

use client_protocol::{Exit, Outcome};
use ratatui::{
    style::{Color, Modifier, Style},
    text::{Line, Span},
};

use crate::session::{SessionEvent, StepResult};

/// Per-parser scrollback rows — generous to preserve everything for the
/// post-finalize `vt_to_lines` walk.
pub const SCROLLBACK_ROWS: usize = 5000;

pub enum Mode {
    Idle,
    Running,
    Quit,
}

pub enum Entry {
    Finalized {
        cmd: String,
        output: Vec<Line<'static>>,
        footer: FooterLine,
        phases: Vec<PhaseTiming>,
    },
    Running {
        cmd: String,
        vt: vt100::Parser,
        started: Instant,
        produced_output: bool,
        current_phase: Option<String>,
        /// Each `phase` SSE event we've seen, with the instant it arrived.
        /// Used at finalize time to compute per-phase durations.
        phase_log: Vec<(String, Instant)>,
        /// Accumulated stderr bytes from the agent's spawned process — for
        /// the bridge path this is anything `pty-bridge` itself writes to
        /// stderr (e.g. a spawn error). Decoded to dim-red lines at finalize
        /// so users can see what went wrong when a step fails.
        stderr: Vec<u8>,
        spinner_tick: u8,
    },
}

#[derive(Debug, Clone)]
pub struct PhaseTiming {
    pub name: String,
    pub duration: Duration,
}

pub enum FooterLine {
    Ok {
        frame_id: String,
        duration: Duration,
    },
    Exit {
        frame_id: String,
        code: i64,
        duration: Duration,
    },
    Signal {
        frame_id: String,
        signal: i64,
        duration: Duration,
    },
    Error {
        message: String,
    },
    Aborted {
        duration: Duration,
    },
}

pub struct App {
    pub mode: Mode,
    pub entries: Vec<Entry>,
    pub session_image: String,
    pub last_frame: Option<String>,
    pub input: String,
    /// BYTE offset into `input` (always on a char boundary — see the cursor
    /// handling in main.rs). Not a char count: `String::insert`/`remove`
    /// take byte indices, and conflating the two panics on the first
    /// multi-byte character.
    pub cursor: usize,
    /// Rows scrolled away from the bottom. 0 means anchored to the live
    /// edge. usize, not u16: a long session's transcript exceeds 65,535
    /// rows well within normal use (~14 full-scrollback commands).
    pub scroll: usize,
}

impl App {
    pub fn new(session_image: String) -> Self {
        Self {
            mode: Mode::Idle,
            entries: Vec::new(),
            session_image,
            last_frame: None,
            input: String::new(),
            cursor: 0,
            scroll: 0,
        }
    }

    /// Push a fresh `Entry::Running` and snap the view back to the bottom.
    /// The caller wires the SSE rx and flips `mode = Running` after the step
    /// request returns. The vt100 grid is sized to the live terminal so the
    /// guest sees a real-sized TTY; the renderer resizes it again on each
    /// draw to track resize events.
    pub fn start_step(&mut self, cmd: String, term_cols: u16, term_rows: u16) {
        // Floor at 2×2 — vt100 0.16.2's `col_wrap` underflows on a 1-row
        // grid and on a <2-col grid. See the same floor in
        // [crate::ui::build_transcript].
        let rows = term_rows.max(2);
        let cols = term_cols.max(2);
        // Seed `phase_log` with a "starting" entry timestamped at submit
        // time, so the gap between Enter being pressed and the host's
        // first `phase` event (WS connect + initial server-side setup)
        // shows up in the final breakdown like any other phase.
        let now = Instant::now();
        self.entries.push(Entry::Running {
            cmd,
            vt: vt100::Parser::new(rows, cols, SCROLLBACK_ROWS),
            started: now,
            produced_output: false,
            current_phase: Some("starting".to_owned()),
            phase_log: vec![("starting".to_owned(), now)],
            stderr: Vec::new(),
            spinner_tick: 0,
        });
        self.scroll = 0;
    }

    /// Drive the running entry from a single [SessionEvent] produced by
    /// [crate::session]. Functionally the WebSocket equivalent of
    /// [Self::handle_sse]; the Result variant flips mode back to Idle by
    /// replacing the running entry with a finalized one.
    pub fn handle_session_event(&mut self, ev: SessionEvent) {
        match ev {
            SessionEvent::Phase(name) => {
                if let Some(Entry::Running {
                    current_phase,
                    phase_log,
                    ..
                }) = self.entries.last_mut()
                {
                    *current_phase = Some(name.clone());
                    phase_log.push((name, Instant::now()));
                }
            }
            SessionEvent::Stdout(bytes) => {
                if let Some(Entry::Running {
                    vt,
                    produced_output,
                    ..
                }) = self.entries.last_mut()
                {
                    vt.process(&bytes);
                    *produced_output = true;
                }
            }
            SessionEvent::Stderr(bytes) => {
                if let Some(Entry::Running { stderr, .. }) = self.entries.last_mut() {
                    stderr.extend_from_slice(&bytes);
                }
            }
            SessionEvent::Result(StepResult::Ok { frame_id, outcome }) => {
                self.last_frame = Some(frame_id.clone());
                let duration = self.running_duration().unwrap_or_default();
                // The host's outcome is the pty-bridge process exit — and
                // the bridge intentionally doesn't propagate the command's
                // exit code (we dropped exit-code fidelity when we moved
                // off SSH). So in practice this is ~always Ok; the
                // `outcome_to_footer` path stays for the rare spawn/wait
                // failure cases the host reports.
                let footer = match &outcome {
                    None => FooterLine::Ok { frame_id, duration },
                    Some(o) => outcome_to_footer(o, frame_id, duration),
                };
                self.finalize_running(footer);
            }
            SessionEvent::Result(StepResult::Err(msg)) => {
                self.finalize_running(FooterLine::Error { message: msg });
            }
        }
        // Any incoming event represents activity; snap the view back to
        // the live edge so the user sees what's happening.
        self.scroll = 0;
    }

    /// Finalize the in-flight entry with the supplied footer, drop the SSE
    /// receiver, return to Idle. Called for clean completion, errors, and
    /// transport failures.
    pub fn finalize_running(&mut self, footer: FooterLine) {
        let Some(last) = self.entries.last_mut() else {
            return;
        };
        if matches!(last, Entry::Running { .. }) {
            // SAFETY: matched the variant above.
            let owned = std::mem::replace(
                last,
                Entry::Finalized {
                    cmd: String::new(),
                    output: Vec::new(),
                    footer: FooterLine::Error {
                        message: "<placeholder>".into(),
                    },
                    phases: Vec::new(),
                },
            );
            if let Entry::Running {
                cmd,
                vt,
                phase_log,
                stderr,
                ..
            } = owned
            {
                let mut output = vt_to_lines(vt);
                // Only surface the spawned process's stderr when the step
                // didn't cleanly succeed. For the bridge path this is
                // normally empty; on failure it carries pty-bridge's own
                // diagnostics (e.g. a spawn error), which is exactly when
                // you want to see it.
                if !matches!(footer, FooterLine::Ok { .. }) {
                    output.extend(stderr_to_lines(&stderr));
                }
                let phases = phase_log_to_timings(phase_log);
                *last = Entry::Finalized {
                    cmd,
                    output,
                    footer,
                    phases,
                };
            }
        }
        self.mode = Mode::Idle;
    }

    /// Advance the spinner glyph on the periodic redraw tick.
    pub fn tick_spinner(&mut self) {
        if let Some(Entry::Running { spinner_tick, .. }) = self.entries.last_mut() {
            *spinner_tick = spinner_tick.wrapping_add(1);
        }
    }

    fn running_duration(&self) -> Option<Duration> {
        match self.entries.last() {
            Some(Entry::Running { started, .. }) => Some(started.elapsed()),
            _ => None,
        }
    }
}

/// Decode the buffered stderr bytes (the spawned process's stderr — for
/// the bridge path, anything `pty-bridge` writes to its own stderr) into
/// styled lines for appending to a finalized entry. UTF-8 lossy so
/// non-text bytes don't blow up; styled dim red so they read as
/// "diagnostic, not part of the program's real output".
fn stderr_to_lines(bytes: &[u8]) -> Vec<Line<'static>> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(bytes);
    let style = Style::default()
        .fg(Color::Red)
        .add_modifier(Modifier::DIM);
    text.split_inclusive('\n')
        .map(|chunk| chunk.trim_end_matches(['\r', '\n']).to_owned())
        .filter(|s| !s.is_empty())
        .map(|s| Line::from(Span::styled(format!("stderr: {s}"), style)))
        .collect()
}

/// Convert the timestamped phase log captured during a step into per-phase
/// durations. Each entry's duration is the gap to the next entry's start;
/// the final entry runs until `Instant::now()` (i.e. finalize time).
fn phase_log_to_timings(log: Vec<(String, Instant)>) -> Vec<PhaseTiming> {
    let now = Instant::now();
    let mut out = Vec::with_capacity(log.len());
    for i in 0..log.len() {
        let (name, start) = &log[i];
        let end = log.get(i + 1).map(|(_, t)| *t).unwrap_or(now);
        out.push(PhaseTiming {
            name: name.clone(),
            duration: end.saturating_duration_since(*start),
        });
    }
    out
}

fn outcome_to_footer(o: &Outcome, frame_id: String, duration: Duration) -> FooterLine {
    match o {
        Outcome::Exited(Exit::Code(0)) => FooterLine::Ok { frame_id, duration },
        Outcome::Exited(Exit::Code(code)) => FooterLine::Exit {
            frame_id,
            code: *code,
            duration,
        },
        Outcome::Exited(Exit::Signal(signal)) => FooterLine::Signal {
            frame_id,
            signal: *signal,
            duration,
        },
        Outcome::SpawnFailed(err) => FooterLine::Error {
            message: format!("spawn failed: {err}"),
        },
        // The command ran (the frame is real and is now `last_frame`) but
        // its exit status was lost to a wait() error on the agent.
        Outcome::WaitFailed(err) => FooterLine::Error {
            message: format!("ran, but exit status unknown (wait failed: {err})"),
        },
    }
}

/// Walk the parser's full scrollback + visible screen, emitting one styled
/// `Line` per row in chronological order. Called once per step at finalize
/// time; the parser is moved in because we mutate `set_scrollback` while
/// walking.
pub fn vt_to_lines(mut parser: vt100::Parser) -> Vec<Line<'static>> {
    let (rows, cols) = parser.screen().size();
    let mut lines: Vec<Line<'static>> = Vec::new();

    // Walk scrollback most-recent → oldest by increasing `set_scrollback(N)`,
    // reading row 0 each time (the row revealed at the top when the view is
    // shifted up by N). Stop once the parser caps below our requested offset.
    let mut scrollback_rows: Vec<Line<'static>> = Vec::new();
    let mut offset: usize = 1;
    loop {
        parser.screen_mut().set_scrollback(offset);
        if parser.screen().scrollback() != offset {
            break;
        }
        scrollback_rows.push(render_row(parser.screen(), 0, cols));
        offset += 1;
        if offset > SCROLLBACK_ROWS + rows as usize {
            break;
        }
    }
    scrollback_rows.reverse();
    lines.extend(scrollback_rows);

    parser.screen_mut().set_scrollback(0);
    let screen = parser.screen();
    if let Some(last_nonblank) = last_nonblank_row(screen, rows, cols) {
        for row in 0..=last_nonblank {
            lines.push(render_row(screen, row, cols));
        }
    }

    lines
}

/// The index of the last row in the (rows × cols) viewport with any
/// non-blank cell, or `None` if the entire viewport is blank. Used by the
/// live render to trim trailing blank rows of the running entry's body —
/// vt100 keeps a fixed grid, but a step that has only printed one line of
/// `\r`-overwriting progress shouldn't paint 19 blank rows below it.
pub fn last_nonblank_row(screen: &vt100::Screen, rows: u16, cols: u16) -> Option<u16> {
    let mut last: Option<u16> = None;
    for row in 0..rows {
        let mut nonblank = false;
        for col in 0..cols {
            if let Some(cell) = screen.cell(row, col) {
                if !cell.contents().chars().all(|c| c == ' ' || c == '\0') {
                    nonblank = true;
                    break;
                }
            }
        }
        if nonblank {
            last = Some(row);
        }
    }
    last
}

/// Render one row of the vt100 screen at the given scrollback offset (0 =
/// visible). Groups adjacent same-style cells into a single `Span`; skips
/// wide-character continuation cells (the leading wide cell paints both
/// columns in any terminal that respects East Asian Width).
///
/// Public so the live-render path in [crate::ui] can reuse it for the
/// running entry's body.
pub fn render_row(screen: &vt100::Screen, row: u16, cols: u16) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut buf = String::new();
    let mut cur_style: Option<Style> = None;
    for col in 0..cols {
        let Some(cell) = screen.cell(row, col) else { continue };
        if cell.is_wide_continuation() {
            continue;
        }
        let style = cell_style(cell);
        let contents = cell.contents();
        let s = if contents.is_empty() { " " } else { contents };
        match cur_style {
            Some(prev) if prev == style => buf.push_str(s),
            _ => {
                if let Some(prev) = cur_style.take() {
                    spans.push(Span::styled(std::mem::take(&mut buf), prev));
                }
                cur_style = Some(style);
                buf.push_str(s);
            }
        }
    }
    if let Some(prev) = cur_style.take() {
        // Trim trailing blanks that share the default style — they're just
        // unused cells in the right margin and clutter the line otherwise.
        if prev == Style::default() {
            let trimmed: String = buf.trim_end().to_owned();
            if !trimmed.is_empty() {
                spans.push(Span::styled(trimmed, prev));
            }
        } else {
            spans.push(Span::styled(buf, prev));
        }
    }
    Line::from(spans)
}

fn cell_style(cell: &vt100::Cell) -> Style {
    let mut style = Style::default();
    if let Some(c) = vt_color_to_ratatui(cell.fgcolor()) {
        style = style.fg(c);
    }
    if let Some(c) = vt_color_to_ratatui(cell.bgcolor()) {
        style = style.bg(c);
    }
    let mut mods = Modifier::empty();
    if cell.bold() {
        mods |= Modifier::BOLD;
    }
    if cell.italic() {
        mods |= Modifier::ITALIC;
    }
    if cell.underline() {
        mods |= Modifier::UNDERLINED;
    }
    if cell.inverse() {
        mods |= Modifier::REVERSED;
    }
    if !mods.is_empty() {
        style = style.add_modifier(mods);
    }
    style
}

fn vt_color_to_ratatui(c: vt100::Color) -> Option<Color> {
    match c {
        vt100::Color::Default => None,
        vt100::Color::Idx(i) => Some(ansi_indexed_to_ratatui(i)),
        vt100::Color::Rgb(r, g, b) => Some(Color::Rgb(r, g, b)),
    }
}

fn ansi_indexed_to_ratatui(i: u8) -> Color {
    match i {
        0 => Color::Black,
        1 => Color::Red,
        2 => Color::Green,
        3 => Color::Yellow,
        4 => Color::Blue,
        5 => Color::Magenta,
        6 => Color::Cyan,
        7 => Color::Gray,
        8 => Color::DarkGray,
        9 => Color::LightRed,
        10 => Color::LightGreen,
        11 => Color::LightYellow,
        12 => Color::LightBlue,
        13 => Color::LightMagenta,
        14 => Color::LightCyan,
        15 => Color::White,
        other => Color::Indexed(other),
    }
}
