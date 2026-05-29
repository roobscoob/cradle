//! Render the REPL: alt-screen ratatui as one continuous transcript stream.
//!
//! Idle and Running render identically — there's no layout switch on mode.
//! The whole screen is the transcript: each entry's `$ <cmd>` header is in
//! the flow, then its body, then its footer; entries are separated by a
//! blank line; when Idle, the active `$ <input>` prompt is the final row.
//! The running entry (if any) is the last entry and renders the same way
//! as a finalized one — header in flow, then either a spinner row or the
//! embedded vt100 body.
//!
//! "Sticky" matches CSS `position: sticky`: as the transcript scrolls, if
//! the topmost visible row belongs to entry E and E's header has already
//! scrolled off above the viewport, we overlay E's header on that top row.
//! When E's body fully scrolls past, the owning entry of the top row
//! changes, the previous header is released, and the next entry's header
//! takes over — exactly like a section header in a scrolling list.
//!
//! See `C:\Users\rose\.claude\plans\mighty-sprouting-hedgehog.md`.

use std::fmt::Write;

use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Clear, Paragraph},
};

use crate::app::{App, Entry, FooterLine, Mode, PhaseTiming, last_nonblank_row, render_row};

const SPINNER_GLYPHS: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

pub fn draw(f: &mut Frame, app: &mut App) {
    render_transcript(f, f.area(), app);
}

fn render_transcript(f: &mut Frame, area: Rect, app: &mut App) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    // Build the full transcript as a flat Vec<Line>, plus parallel metadata
    // for sticky: `owners[r]` = which entry row r belongs to (None for
    // separators / the input prompt), and `header_rows[e]` = the logical
    // row index of entry e's header in `lines`. Both finalized and running
    // entries produce lines via the same path so they layout and scroll
    // identically.
    let Transcript {
        lines,
        owners,
        headers,
        header_rows,
    } = build_transcript(app, area.width, area.height);

    let total = lines.len() as u16;
    let visible = area.height;
    // Bottom-anchored: scroll_y is the number of lines hidden above the
    // visible window. user `app.scroll` lifts the window further off the
    // bottom (capped at total). Write the clamped value back so scrolling
    // past the top doesn't accumulate stale offset that has to be burned
    // through before scroll-down responds.
    let max_scroll_y = total.saturating_sub(visible);
    app.scroll = app.scroll.min(max_scroll_y);
    let user_scroll = app.scroll;
    let scroll_y = max_scroll_y.saturating_sub(user_scroll);

    let para = Paragraph::new(lines).scroll((scroll_y, 0));
    f.render_widget(para, area);

    // CSS-sticky overlay. The topmost visible logical row is `scroll_y`.
    // If it belongs to entry E and E's header row is strictly above the
    // viewport, overlay E's header on the top row — same as
    // `position: sticky; top: 0` for a section header in a scrolling list.
    // When the body of E fully scrolls past, owners[scroll_y] no longer
    // points to E, and the overlay automatically releases.
    if let Some(Some(owner)) = owners.get(scroll_y as usize).copied() {
        if header_rows[owner] < scroll_y {
            let top = Rect {
                x: area.x,
                y: area.y,
                width: area.width,
                height: 1,
            };
            // Wipe the row first — Paragraph only writes cells with text, so
            // without Clear the transcript row underneath shows through past
            // the end of the command (and its fg/bg patches through the
            // header's unset style fields).
            f.render_widget(Clear, top);
            f.render_widget(Paragraph::new(headers[owner].clone()), top);
        }
    }

    // Cursor on the active prompt — only if Idle AND the user hasn't scrolled
    // away (otherwise the cursor would land off-screen).
    if matches!(app.mode, Mode::Idle) && user_scroll == 0 && total > 0 {
        let last_logical_row = total - 1;
        let on_screen_row = last_logical_row.saturating_sub(scroll_y);
        if on_screen_row < visible {
            let col = 2 + app.cursor as u16; // "$ " prefix
            let x = area.x + col.min(area.width.saturating_sub(1));
            let y = area.y + on_screen_row;
            f.set_cursor_position((x, y));
        }
    }
}

struct Transcript {
    lines: Vec<Line<'static>>,
    owners: Vec<Option<usize>>,
    headers: Vec<Line<'static>>,
    header_rows: Vec<u16>,
}

fn build_transcript(app: &mut App, cols: u16, area_height: u16) -> Transcript {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut owners: Vec<Option<usize>> = Vec::new();
    let mut headers: Vec<Line<'static>> = Vec::new();
    let mut header_rows: Vec<u16> = Vec::new();

    let entries_len = app.entries.len();
    for (i, entry) in app.entries.iter_mut().enumerate() {
        let is_last = i + 1 == entries_len;

        let (cmd, body): (&str, Vec<Line<'static>>) = match entry {
            Entry::Finalized {
                cmd,
                output,
                footer,
                phases,
            } => {
                let mut body: Vec<Line<'static>> = output.iter().cloned().collect();
                body.push(footer_line(footer));
                if !phases.is_empty() {
                    body.push(breakdown_line(phases));
                }
                (cmd.as_str(), body)
            }
            Entry::Running {
                cmd,
                vt,
                produced_output,
                current_phase,
                spinner_tick,
                ..
            } => {
                let body = if *produced_output {
                    // Size the vt100 grid to the live terminal so the guest
                    // sees a real-sized TTY and resize events flow through.
                    // Then render only up to the last non-blank row — vt100
                    // keeps a fixed grid, but a step that has only printed
                    // one line of `\r`-overwriting progress shouldn't paint
                    // a screenful of blank rows below it.
                    //
                    // Floor at 2×2: vt100 0.16.2 has an underflow in
                    // `col_wrap` on a 1-row grid (`prev_pos.row -= scrolled`
                    // with prev_pos.row = 0 and scrolled = 1), and a similar
                    // hazard in `col_wrap`'s `self.size.cols - width` when
                    // cols < 2. A 1-cell terminal isn't a meaningful PTY
                    // anyway.
                    let rows = area_height.max(2);
                    let cols = cols.max(2);
                    let cur = vt.screen().size();
                    if cur != (rows, cols) {
                        vt.screen_mut().set_size(rows, cols);
                    }
                    let screen = vt.screen();
                    let mut lines: Vec<Line<'static>> = match last_nonblank_row(screen, rows, cols)
                    {
                        Some(last) => (0..=last)
                            .map(|row| render_row(screen, row, cols))
                            .collect(),
                        None => Vec::new(),
                    };
                    // After the command exits the host moves to the
                    // "snapshotting" phase before the entry finalizes.
                    // Show a phase spinner below the output during that
                    // window so the ~couple-second wait reads as
                    // "working", not "frozen". During the command itself
                    // (phase "evaluating") the streaming output is its own
                    // liveness signal, so no spinner then.
                    if current_phase.as_deref() == Some("snapshotting") {
                        lines.push(spinner_line("snapshotting", *spinner_tick));
                    }
                    lines
                } else {
                    let phase = current_phase.as_deref().unwrap_or("starting");
                    vec![spinner_line(phase, *spinner_tick)]
                };
                (cmd.as_str(), body)
            }
        };

        let header = header_line(cmd);
        header_rows.push(lines.len() as u16);
        headers.push(header.clone());
        lines.push(header);
        owners.push(Some(i));
        for l in body {
            lines.push(l);
            owners.push(Some(i));
        }
        if !is_last {
            // Separator is not part of any entry, so it never triggers
            // sticky — and the next entry's header on the row below it
            // remains the visible owner.
            lines.push(Line::default());
            owners.push(None);
        }
    }

    if matches!(app.mode, Mode::Idle) {
        lines.push(prompt_line(&app.input));
        owners.push(None);
    }

    Transcript {
        lines,
        owners,
        headers,
        header_rows,
    }
}

fn header_line(cmd: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled("$ ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::styled(
            cmd.to_owned(),
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
    ])
}

fn prompt_line(input: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled("$ ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::raw(input.to_owned()),
    ])
}

fn spinner_line(phase: &str, tick: u8) -> Line<'static> {
    let glyph = SPINNER_GLYPHS[(tick as usize) % SPINNER_GLYPHS.len()];
    let style = Style::default().fg(Color::Cyan).add_modifier(Modifier::DIM);
    Line::from(vec![
        Span::styled(glyph.to_owned(), style),
        Span::styled(format!(" {phase}"), style),
    ])
}

fn breakdown_line(phases: &[PhaseTiming]) -> Line<'static> {
    let style = Style::default().fg(Color::DarkGray).add_modifier(Modifier::DIM);
    let text = phases
        .iter()
        .map(|p| format!("{} {}ms", p.name, p.duration.as_millis()))
        .collect::<Vec<_>>()
        .join(" · ");
    Line::from(Span::styled(text, style))
}

fn footer_line(footer: &FooterLine) -> Line<'static> {
    let mut text = String::new();
    let style = match footer {
        FooterLine::Ok { frame_id, duration } => {
            let _ = write!(
                text,
                "image {} finished in {:.2}s",
                frame_id,
                duration.as_secs_f64()
            );
            Style::default().fg(Color::DarkGray)
        }
        FooterLine::Exit {
            frame_id,
            code,
            duration,
        } => {
            let _ = write!(
                text,
                "image {} exited {} in {:.2}s",
                frame_id,
                code,
                duration.as_secs_f64()
            );
            Style::default().fg(Color::Red).add_modifier(Modifier::DIM)
        }
        FooterLine::Signal {
            frame_id,
            signal,
            duration,
        } => {
            let _ = write!(
                text,
                "image {} signal {} in {:.2}s",
                frame_id,
                signal,
                duration.as_secs_f64()
            );
            Style::default().fg(Color::Red).add_modifier(Modifier::DIM)
        }
        FooterLine::Error { message } => {
            let _ = write!(text, "error: {}", message);
            Style::default().fg(Color::Red)
        }
        FooterLine::Aborted { duration } => {
            let _ = write!(text, "aborted in {:.2}s", duration.as_secs_f64());
            Style::default().fg(Color::Red).add_modifier(Modifier::DIM)
        }
    };
    Line::from(Span::styled(text, style))
}
