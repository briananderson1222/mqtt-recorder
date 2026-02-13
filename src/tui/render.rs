//! Terminal rendering and management

use crate::tui::{state::TuiState, types::AuditSeverity};
use crossterm::{
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Padding, Paragraph, Wrap},
};
use std::{
    io::{stdout, Stdout},
    sync::atomic::Ordering,
    time::Duration,
};

/// Shared context for drawing operations
struct DrawContext<'a> {
    state: &'a TuiState,
    dim: Style,
    bright: Style,
    // colors
    source_color: Color,
    broker_color: Color,
    mirror_color: Color,
    record_color: Color,
    file_color: Color,
    playback_color: Color,
    // computed values
    received: u64,
    mirrored: u64,
    replayed: u64,
    published: u64,
    recorded: u64,
    // flags
    source_on: bool,
    source_connected: bool,
    mirror_on: bool,
    record_on: bool,
    playback_on: bool,
    playback_finished: bool,
    playback_looping: bool,
    broker_clients: usize,
}

/// Helper to format duration
fn fmt_dur(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

/// Render the Source panel (left column)
fn draw_source_section(frame: &mut Frame, area: Rect, ctx: &DrawContext) {
    let source_host = ctx.state.source_host.as_deref().unwrap_or("none");
    let source_url = format!("mqtt://{}:{}", source_host, ctx.state.source_port);
    let first_connected = ctx.state.get_first_connected_since();
    let source_elapsed = ctx.state.get_source_enabled_elapsed();

    let source_health = if !ctx.source_on {
        Span::styled("○", ctx.dim)
    } else if ctx.source_connected {
        Span::styled("●", Style::default().fg(Color::Green))
    } else {
        Span::styled("◐", Style::default().fg(Color::Yellow))
    };

    let source_timing: Vec<Span> = if let Some(dt) = first_connected {
        let date_str = dt.format("%m/%d %H:%M").to_string();
        let elapsed_str = source_elapsed
            .map(|d| format!(" (+{})", fmt_dur(d)))
            .unwrap_or_default();
        vec![
            Span::styled(date_str, ctx.dim),
            Span::styled(elapsed_str, Style::default().fg(Color::Green)),
            Span::raw(" "),
        ]
    } else {
        vec![]
    };

    let source_block = Block::default()
        .title(Line::from(vec![
            " Source ".into(),
            source_health,
            " ".into(),
        ]))
        .title(Line::from(source_timing).alignment(Alignment::Right))
        .title_bottom(Line::from(Span::styled(
            format!(" {} ", source_url),
            ctx.dim,
        )))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(ctx.source_color))
        .padding(Padding::horizontal(1));
    let si = source_block.inner(area);
    frame.render_widget(source_block, area);

    let conn_status = if !ctx.source_on {
        Line::from(Span::styled("Disabled", ctx.dim))
    } else if ctx.source_connected {
        Line::from(Span::styled("Connected", Style::default().fg(Color::Green)))
    } else {
        Line::from(Span::styled(
            "Connecting...",
            Style::default().fg(Color::Yellow),
        ))
    };
    let rate_span = ctx.state.get_source_rate().map(|r| {
        Span::styled(
            format!(" ({:.1}/s)", r),
            Style::default().fg(Color::DarkGray),
        )
    });
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(
                [
                    vec![
                        "Received: ".into(),
                        Span::styled(format!("{}", ctx.received), ctx.bright),
                    ],
                    rate_span.into_iter().collect(),
                ]
                .concat(),
            ),
            conn_status,
        ])
        .wrap(Wrap { trim: false }),
        si,
    );
}

/// Render the Broker panel (right column)
fn draw_broker_section(frame: &mut Frame, area: Rect, ctx: &DrawContext) {
    let broker_url = format!("mqtt://localhost:{}", ctx.state.broker_port);
    let broker_health = Span::styled("●", Style::default().fg(ctx.broker_color));
    let broker_uptime = ctx.state.get_broker_uptime();
    let broker_started = ctx.state.get_broker_started_at();
    let broker_date_str = broker_started.format("%m/%d %H:%M").to_string();

    let broker_block = Block::default()
        .title(Line::from(vec![
            " Broker ".into(),
            broker_health,
            " ".into(),
        ]))
        .title(
            Line::from(vec![
                Span::styled(broker_date_str, ctx.dim),
                Span::styled(
                    format!(" (+{}) ", fmt_dur(broker_uptime)),
                    Style::default().fg(Color::Green),
                ),
            ])
            .alignment(Alignment::Right),
        )
        .title_bottom(Line::from(Span::styled(
            format!(" {} ", broker_url),
            ctx.dim,
        )))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(ctx.broker_color))
        .padding(Padding::horizontal(1));
    let bi = broker_block.inner(area);
    frame.render_widget(broker_block, area);

    let clients_style = if ctx.broker_clients > 0 {
        ctx.bright
    } else {
        ctx.dim
    };
    let broker_rate = ctx.state.get_broker_rate();
    let rate_span = if broker_rate > 0.0 {
        Span::styled(
            format!(" ({:.1}/s)", broker_rate),
            Style::default().fg(Color::DarkGray),
        )
    } else {
        Span::raw("")
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                "Published: ".into(),
                Span::styled(format!("{}", ctx.published), ctx.bright),
                rate_span,
            ]),
            Line::from(vec![
                "Clients: ".into(),
                Span::styled(format!("{}", ctx.broker_clients), clients_style),
            ]),
        ])
        .wrap(Wrap { trim: false }),
        bi,
    );
}

/// Render the Mirror path row
fn draw_mirror_section(frame: &mut Frame, area: Rect, ctx: &DrawContext) {
    let mirror_elapsed = ctx.state.get_mirror_enabled_elapsed();
    let m_arrow_l = if ctx.mirror_on && ctx.source_on {
        "→"
    } else {
        " "
    };
    let m_arrow_r = if ctx.mirror_on { "→" } else { " " };
    let mirror_elapsed_spans: Vec<Span> = if let Some(d) = mirror_elapsed {
        vec![Span::styled(
            format!(" +{} ", fmt_dur(d)),
            Style::default().fg(Color::Yellow),
        )]
    } else {
        vec![]
    };

    let mirror_block = Block::default()
        .title(Line::from(vec![
            Span::styled(m_arrow_l, Style::default().fg(ctx.mirror_color)),
            " Mirror ".into(),
            if ctx.mirror_on {
                Span::styled("●", Style::default().fg(ctx.mirror_color).bold())
            } else {
                Span::styled("○", ctx.dim)
            },
            " ".into(),
            Span::styled(m_arrow_r, Style::default().fg(ctx.mirror_color)),
        ]))
        .title(Line::from(mirror_elapsed_spans).alignment(Alignment::Right))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(ctx.mirror_color))
        .padding(Padding::horizontal(1));
    let mi = mirror_block.inner(area);
    frame.render_widget(mirror_block, area);

    let vfy_matched = ctx.state.get_verify_matched();
    let vfy_mismatched = ctx.state.get_verify_mismatched();
    let vfy_missing = ctx.state.get_verify_missing();
    let vfy_active = vfy_matched + vfy_mismatched + vfy_missing > 0;
    let mut mirror_lines = vec![Line::from(vec![
        "Mirrored: ".into(),
        Span::styled(format!("{}", ctx.mirrored), ctx.bright),
    ])];
    if vfy_active {
        let mismatch_style = if vfy_mismatched > 0 || vfy_missing > 0 {
            Style::default().fg(Color::Red)
        } else {
            Style::default().fg(Color::Green)
        };
        mirror_lines.push(Line::from(vec![
            Span::styled("Verify: ", Style::default().fg(Color::LightCyan)),
            Span::styled(
                format!("{}✓ ", vfy_matched),
                Style::default().fg(Color::Green),
            ),
            Span::styled(format!("{}✗ ", vfy_mismatched), mismatch_style),
            Span::styled(format!("{}? ", vfy_missing), mismatch_style),
        ]));
    }
    frame.render_widget(Paragraph::new(mirror_lines), mi);
}

/// Render the Record/File/Playback row
fn draw_record_file_playback(frame: &mut Frame, area: Rect, ctx: &DrawContext) {
    let path2 = Layout::horizontal([
        Constraint::Length(18), // Record
        Constraint::Min(10),    // File (flexible)
        Constraint::Length(18), // Playback
    ])
    .split(area);

    let rec_duration = ctx.state.get_recording_duration();
    let play_duration = ctx.state.get_playback_duration();

    draw_record_panel(frame, path2[0], ctx, rec_duration);
    draw_file_panel(frame, path2[1], ctx);
    draw_playback_panel(frame, path2[2], ctx, play_duration);
}

/// Render the Record panel showing recording status and count.
fn draw_record_panel(frame: &mut Frame, area: Rect, ctx: &DrawContext, duration: Option<Duration>) {
    let r_arrow = if ctx.record_on && ctx.source_on {
        "→"
    } else {
        " "
    };
    let record_block = Block::default()
        .title(Line::from(vec![
            Span::styled(r_arrow, Style::default().fg(ctx.record_color)),
            " Record ".into(),
            if ctx.record_on {
                Span::styled("●", Style::default().fg(ctx.record_color).bold())
            } else {
                Span::styled("○", ctx.dim)
            },
            " ".into(),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(ctx.record_color))
        .padding(Padding::horizontal(1));
    let ri = record_block.inner(area);
    frame.render_widget(record_block, area);

    let mut rec_lines = vec![Line::from(vec![
        "Rec: ".into(),
        Span::styled(format!("{}", ctx.recorded), ctx.bright),
    ])];
    if let Some(d) = duration {
        rec_lines.push(Line::from(Span::styled(
            fmt_dur(d),
            Style::default().fg(Color::Green),
        )));
    }
    frame.render_widget(Paragraph::new(rec_lines), ri);
}

/// Render the File panel showing playlist or single file.
fn draw_file_panel(frame: &mut Frame, area: Rect, ctx: &DrawContext) {
    let f_arrow_l = if ctx.record_on { "←" } else { " " };
    let f_arrow_r = if ctx.playback_on { "→" } else { " " };
    let file_block = Block::default()
        .title(Line::from(vec![
            Span::styled(f_arrow_l, Style::default().fg(ctx.file_color)),
            " File ".into(),
            Span::styled(f_arrow_r, Style::default().fg(ctx.file_color)),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(ctx.file_color))
        .padding(Padding::horizontal(1));
    let fi = file_block.inner(area);
    frame.render_widget(file_block, area);

    let all_files = ctx.state.get_all_files();
    let selected_idx = ctx.state.get_selected_index();
    let active_idx = ctx.state.get_playlist_index();
    let file_lines: Vec<Line> = if all_files.is_empty() {
        let f = ctx
            .state
            .get_file_path()
            .unwrap_or_else(|| "none".to_string());
        let count_str = ctx
            .state
            .get_file_line_count(&f)
            .map(|c| format!(" ({})", c))
            .unwrap_or_default();
        vec![Line::from(vec![
            Span::styled(f.clone(), Style::default().fg(Color::White)),
            Span::styled(count_str, ctx.dim),
        ])]
    } else {
        all_files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let is_selected = i == selected_idx;
                let is_active = i == active_idx;
                let prefix = match (is_selected, is_active) {
                    (true, true) => "▶●",
                    (true, false) => "▶ ",
                    (false, true) => " ●",
                    (false, false) => "  ",
                };
                let style = if is_active {
                    Style::default().fg(Color::Green).bold()
                } else if is_selected {
                    Style::default().fg(Color::White)
                } else {
                    ctx.dim
                };
                let count_str = ctx
                    .state
                    .get_file_line_count(f)
                    .map(|c| format!(" ({})", c))
                    .unwrap_or_default();
                Line::from(vec![
                    Span::styled(format!("{}{}", prefix, f), style),
                    Span::styled(count_str, ctx.dim),
                ])
            })
            .collect()
    };
    frame.render_widget(Paragraph::new(file_lines).wrap(Wrap { trim: false }), fi);
}

/// Render the Playback panel showing playback status and count.
fn draw_playback_panel(
    frame: &mut Frame,
    area: Rect,
    ctx: &DrawContext,
    duration: Option<Duration>,
) {
    let p_arrow = if ctx.playback_on { "→" } else { " " };
    let pb_label = if ctx.playback_finished {
        ("◇ done ", ctx.dim)
    } else if ctx.playback_on && ctx.playback_looping {
        ("◆ loop ", Style::default().fg(ctx.playback_color).bold())
    } else if ctx.playback_on {
        ("◆ once ", Style::default().fg(ctx.playback_color).bold())
    } else {
        ("○ off ", ctx.dim)
    };
    let playback_block = Block::default()
        .title(Line::from(vec![
            " Playback ".into(),
            Span::styled(pb_label.0, pb_label.1),
            " ".into(),
            Span::styled(p_arrow, Style::default().fg(ctx.playback_color)),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if ctx.playback_finished {
            Color::DarkGray
        } else {
            ctx.playback_color
        }))
        .padding(Padding::horizontal(1));
    let pi = playback_block.inner(area);
    frame.render_widget(playback_block, area);

    let mut play_lines = vec![Line::from(vec![
        "Replayed: ".into(),
        Span::styled(format!("{}", ctx.replayed), ctx.bright),
    ])];
    if let Some(d) = duration {
        play_lines.push(Line::from(Span::styled(
            fmt_dur(d),
            Style::default().fg(Color::Magenta),
        )));
    }
    frame.render_widget(Paragraph::new(play_lines), pi);
}

/// Render the audit log section
fn draw_audit_log(frame: &mut Frame, area: Rect, ctx: &DrawContext, log_scroll: usize) {
    let audit_entries = ctx.state.get_audit_log();
    let audit_on = ctx.state.is_audit_enabled();
    let audit_file_on = ctx.state.is_audit_file_enabled();
    let scroll_indicator = if log_scroll > 0 {
        format!(" Log ↑{} ", log_scroll)
    } else {
        " Log ".to_string()
    };
    let mut title_spans: Vec<Span> = vec![Span::raw(scroll_indicator)];
    if audit_on {
        title_spans.push(Span::styled("●", Style::default().fg(Color::Green).bold()));
    } else {
        title_spans.push(Span::styled("○", ctx.dim));
    }
    title_spans.push(Span::raw(" "));
    if let Some(path) = ctx.state.get_audit_file_path() {
        if audit_file_on {
            title_spans.push(Span::styled(
                format!("{} ", path),
                Style::default().fg(Color::Green),
            ));
        } else {
            title_spans.push(Span::styled(format!("{} ", path), ctx.dim));
        }
    }
    let audit_color = if audit_on {
        Color::DarkGray
    } else {
        Color::Red
    };
    let audit_block = Block::default()
        .title(Line::from(title_spans).alignment(Alignment::Center))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(audit_color));
    let audit_inner = audit_block.inner(area);
    frame.render_widget(audit_block, area);
    let visible = audit_inner.height as usize;
    let total = audit_entries.len();
    let max_scroll = total.saturating_sub(visible);
    let clamped_scroll = log_scroll.min(max_scroll);
    let start = clamped_scroll;
    let end = (start + visible).min(total);
    let log_lines: Vec<Line> = audit_entries[..total]
        .iter()
        .rev()
        .skip(start)
        .take(end - start)
        .map(|e| {
            let severity_style = match e.severity {
                AuditSeverity::Error => Style::default().fg(Color::Red),
                AuditSeverity::Warn => Style::default().fg(Color::Yellow),
                AuditSeverity::Info => Style::default().fg(e.area.color()),
            };
            Line::from(vec![
                Span::styled(&e.timestamp, ctx.dim),
                Span::styled(
                    format!(" [{}] ", e.area.label()),
                    Style::default().fg(e.area.color()),
                ),
                Span::styled(&e.message, severity_style),
            ])
        })
        .collect();
    frame.render_widget(
        Paragraph::new(log_lines).wrap(Wrap { trim: false }),
        audit_inner,
    );
}

/// Render the controls bar
fn draw_controls(
    frame: &mut Frame,
    area: Rect,
    ctx: &DrawContext,
    input_mode: bool,
    input_target_audit: bool,
    input_buffer: &str,
) {
    let controls = if input_mode && input_target_audit {
        Line::from(vec![
            " Audit log: ".into(),
            Span::styled(input_buffer, Style::default().fg(Color::White)),
            Span::styled("█", Style::default().fg(Color::White)),
            "  [Enter] Save  [Esc] Cancel".into(),
        ])
    } else if input_mode {
        Line::from(vec![
            " New file: ".into(),
            Span::styled(input_buffer, Style::default().fg(Color::White)),
            Span::styled("█", Style::default().fg(Color::White)),
            Span::styled(".csv", ctx.dim),
            "  [Enter] Save  [Esc] Cancel".into(),
        ])
    } else {
        let loop_on = ctx.state.loop_enabled.load(Ordering::Relaxed);
        let audit_on = ctx.state.is_audit_enabled();
        let audit_color = if audit_on {
            Color::Green
        } else {
            Color::DarkGray
        };
        let loop_ind = if loop_on { "●" } else { "○" };
        let src_ind = if ctx.source_on { "●" } else { "○" };
        let mir_ind = if ctx.mirror_on { "●" } else { "○" };
        let rec_ind = if ctx.record_on { "●" } else { "○" };
        let ply_ind = if ctx.playback_on { "●" } else { "○" };
        let aud_ind = if audit_on { "●" } else { "○" };
        Line::from(vec![
            Span::styled(" [s]", Style::default().fg(ctx.source_color)),
            Span::styled(
                format!(" Source {} ", src_ind),
                Style::default().fg(ctx.source_color),
            ),
            Span::styled("[m]", Style::default().fg(ctx.mirror_color)),
            Span::styled(
                format!(" Mirror {} ", mir_ind),
                Style::default().fg(ctx.mirror_color),
            ),
            Span::styled("[r]", Style::default().fg(ctx.record_color)),
            Span::styled(
                format!(" Record {} ", rec_ind),
                Style::default().fg(ctx.record_color),
            ),
            Span::styled("[p]", Style::default().fg(ctx.playback_color)),
            Span::styled(
                format!(" Play {} ", ply_ind),
                Style::default().fg(ctx.playback_color),
            ),
            Span::styled("[l]", Style::default().fg(ctx.playback_color)),
            Span::styled(
                format!(" Loop {} ", loop_ind),
                Style::default().fg(ctx.playback_color),
            ),
            Span::styled("[f]", Style::default().fg(Color::White)),
            " New File  ".into(),
            Span::styled("[a]", Style::default().fg(audit_color)),
            Span::styled(
                format!(" Audit {} ", aud_ind),
                Style::default().fg(audit_color),
            ),
            Span::styled("[A]", Style::default().fg(audit_color)),
            " Log File  ".into(),
            Span::styled("[↑↓]", ctx.dim),
            " Select  ".into(),
            Span::styled("[j/k]", ctx.dim),
            " Scroll  ".into(),
            Span::styled("[q]", Style::default().fg(Color::Red)),
            " Quit".into(),
        ])
    };
    frame.render_widget(Paragraph::new(controls).wrap(Wrap { trim: false }), area);
}

/// Terminal wrapper for cleanup on drop
pub struct Terminal {
    terminal: ratatui::Terminal<CrosstermBackend<Stdout>>,
}

impl Terminal {
    /// Creates a new TUI terminal, entering raw mode and the alternate screen.
    pub fn new() -> std::io::Result<Self> {
        enable_raw_mode()?;
        stdout().execute(EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout());
        let terminal = ratatui::Terminal::new(backend)?;
        Ok(Self { terminal })
    }

    /// Draws the TUI frame with the current state.
    pub fn draw(
        &mut self,
        state: &TuiState,
        input_mode: bool,
        input_target_audit: bool,
        input_buffer: &str,
        log_scroll: usize,
    ) -> std::io::Result<()> {
        self.terminal.draw(|frame| {
            let area = frame.area();
            let error = state.get_error();
            let has_error = error.is_some();
            let loop_on = state.loop_enabled.load(Ordering::Relaxed);

            // Create shared context
            let ctx = DrawContext {
                state,
                dim: Style::default().fg(Color::DarkGray),
                bright: Style::default().fg(Color::White).bold(),
                received: state.get_received_count(),
                mirrored: state.get_mirrored_count(),
                replayed: state.get_replayed_count(),
                published: state.get_published_count(),
                recorded: state.get_recorded_count(),
                source_on: state.is_source_enabled(),
                source_connected: state.is_source_connected(),
                mirror_on: state.is_mirroring(),
                record_on: state.is_recording(),
                playback_on: loop_on && !state.is_recording(),
                playback_finished: state.is_playback_finished(),
                playback_looping: state.is_playback_looping(),
                broker_clients: state.get_broker_connections(),
                source_color: if !state.is_source_enabled() {
                    Color::DarkGray
                } else if has_error {
                    Color::Red
                } else if state.is_source_connected() {
                    Color::Green
                } else {
                    Color::Yellow
                },
                broker_color: Color::Green,
                mirror_color: if has_error {
                    Color::Red
                } else if state.is_mirroring() && state.is_source_connected() {
                    Color::Yellow
                } else {
                    Color::DarkGray
                },
                record_color: if state.is_recording() {
                    Color::Green
                } else {
                    Color::DarkGray
                },
                file_color: if state.is_recording() {
                    Color::Green
                } else if loop_on && !state.is_recording() {
                    Color::Magenta
                } else {
                    Color::DarkGray
                },
                playback_color: if loop_on && !state.is_recording() {
                    Color::Magenta
                } else {
                    Color::DarkGray
                },
            };

            // Outer frame
            let outer = Block::default()
                .title(" mqtt-recorder ")
                .title_bottom(
                    Line::from(Span::styled(
                        format!(" {} ", state.session_id),
                        Style::default().fg(Color::DarkGray),
                    ))
                    .alignment(Alignment::Right),
                )
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan));
            let inner = outer.inner(area);
            frame.render_widget(outer, area);

            // Main layout: [Source | Middle | Broker] + audit log + error + controls
            let vert = Layout::vertical([
                Constraint::Min(8),
                Constraint::Length(6), // Audit log
                Constraint::Length(1),
                Constraint::Length(2), // Controls (wraps)
            ])
            .split(inner);

            let cols = Layout::horizontal([
                Constraint::Percentage(28), // Source
                Constraint::Min(20),        // Middle
                Constraint::Percentage(28), // Broker
            ])
            .split(vert[0]);

            // Render sections
            draw_source_section(frame, cols[0], &ctx);
            draw_broker_section(frame, cols[2], &ctx);

            // Middle: two paths
            let mid = Layout::vertical([
                Constraint::Length(3), // Mirror path
                Constraint::Length(1), // separator
                Constraint::Min(3),    // Record/File/Playback path
            ])
            .split(cols[1]);

            draw_mirror_section(frame, mid[0], &ctx);

            // Separator
            frame.render_widget(Paragraph::new("").style(ctx.dim), mid[1]);

            draw_record_file_playback(frame, mid[2], &ctx);
            draw_audit_log(frame, vert[1], &ctx, log_scroll);

            // Error
            if let Some(ref err) = error {
                frame.render_widget(
                    Paragraph::new(Line::from(vec![
                        Span::styled(" ⚠ ", Style::default().fg(Color::Red).bold()),
                        Span::styled(err.as_str(), Style::default().fg(Color::Red)),
                    ])),
                    vert[2],
                );
            }

            draw_controls(
                frame,
                vert[3],
                &ctx,
                input_mode,
                input_target_audit,
                input_buffer,
            );
        })?;
        Ok(())
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = stdout().execute(LeaveAlternateScreen);
    }
}
