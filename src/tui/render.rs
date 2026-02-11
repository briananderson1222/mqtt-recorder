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

/// Terminal wrapper for cleanup on drop
pub struct Terminal {
    terminal: ratatui::Terminal<CrosstermBackend<Stdout>>,
}

impl Terminal {
    pub fn new() -> std::io::Result<Self> {
        enable_raw_mode()?;
        stdout().execute(EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout());
        let terminal = ratatui::Terminal::new(backend)?;
        Ok(Self { terminal })
    }

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
            let received = state.get_received_count();
            let mirrored = state.get_mirrored_count();
            let replayed = state.get_replayed_count();
            let published = state.get_published_count();
            let recorded = state.get_recorded_count();
            let error = state.get_error();

            let source_host = state.source_host.as_deref().unwrap_or("none");
            let source_url = format!("mqtt://{}:{}", source_host, state.source_port);
            let broker_url = format!("mqtt://localhost:{}", state.broker_port);

            let source_on = state.is_source_enabled();
            let source_connected = state.is_source_connected();
            let mirror_on = state.is_mirroring();
            let loop_on = state.loop_enabled.load(Ordering::Relaxed);
            let record_on = state.is_recording();
            let playback_on = loop_on && !record_on;
            let playback_finished = state.is_playback_finished();
            let playback_looping = state.is_playback_looping();
            let broker_clients = state.get_broker_connections();

            // Get timing info
            let _conn_duration = state.get_connection_duration();
            let source_elapsed = state.get_source_enabled_elapsed();
            let mirror_elapsed = state.get_mirror_enabled_elapsed();
            let rec_duration = state.get_recording_duration();
            let play_duration = state.get_playback_duration();
            let first_connected = state.get_first_connected_since();

            let has_error = error.is_some();
            let dim = Style::default().fg(Color::DarkGray);
            let bright = Style::default().fg(Color::White).bold();

            // Helper to format duration
            let fmt_dur = |d: Duration| -> String {
                let secs = d.as_secs();
                if secs >= 3600 {
                    format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
                } else if secs >= 60 {
                    format!("{}m{}s", secs / 60, secs % 60)
                } else {
                    format!("{}s", secs)
                }
            };

            let source_color = if !source_on {
                Color::DarkGray
            } else if has_error {
                Color::Red
            } else if source_connected {
                Color::Green
            } else {
                Color::Yellow
            };
            let broker_color = Color::Green; // Broker is always local, always up
            let mirror_color = if has_error {
                Color::Red
            } else if mirror_on && source_connected {
                Color::Yellow
            } else {
                Color::DarkGray
            };
            let record_color = if record_on {
                Color::Green
            } else {
                Color::DarkGray
            };
            let file_color = if record_on {
                Color::Green
            } else if playback_on {
                Color::Magenta
            } else {
                Color::DarkGray
            };
            let playback_color = if playback_on {
                Color::Magenta
            } else {
                Color::DarkGray
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

            // === SOURCE (left, full height) ===
            let source_health = if !source_on {
                Span::styled("○", dim)
            } else if source_connected {
                Span::styled("●", Style::default().fg(Color::Green))
            } else {
                Span::styled("◐", Style::default().fg(Color::Yellow))
            };
            // Timing for top-right: first connected date + elapsed since enabled
            let source_timing: Vec<Span> = if let Some(dt) = first_connected {
                let date_str = dt.format("%m/%d %H:%M").to_string();
                let elapsed_str = source_elapsed
                    .map(|d| format!(" (+{})", fmt_dur(d)))
                    .unwrap_or_default();
                vec![
                    Span::styled(date_str, dim),
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
                .title_bottom(Line::from(Span::styled(format!(" {} ", source_url), dim)))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(source_color))
                .padding(Padding::horizontal(1));
            let si = source_block.inner(cols[0]);
            frame.render_widget(source_block, cols[0]);

            // Source content
            let conn_status = if !source_on {
                Line::from(Span::styled("Disabled", dim))
            } else if source_connected {
                Line::from(Span::styled("Connected", Style::default().fg(Color::Green)))
            } else {
                Line::from(Span::styled(
                    "Connecting...",
                    Style::default().fg(Color::Yellow),
                ))
            };
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(vec![
                        "Received: ".into(),
                        Span::styled(format!("{}", received), bright),
                    ]),
                    conn_status,
                ])
                .wrap(Wrap { trim: false }),
                si,
            );

            // === BROKER (right, full height) ===
            let broker_health = Span::styled("●", Style::default().fg(broker_color));
            let broker_uptime = state.get_broker_uptime();
            let broker_started = state.get_broker_started_at();
            let broker_date_str = broker_started.format("%m/%d %H:%M").to_string();
            let broker_block = Block::default()
                .title(Line::from(vec![
                    " Broker ".into(),
                    broker_health,
                    " ".into(),
                ]))
                .title(
                    Line::from(vec![
                        Span::styled(broker_date_str, dim),
                        Span::styled(
                            format!(" (+{}) ", fmt_dur(broker_uptime)),
                            Style::default().fg(Color::Green),
                        ),
                    ])
                    .alignment(Alignment::Right),
                )
                .title_bottom(Line::from(Span::styled(format!(" {} ", broker_url), dim)))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(broker_color))
                .padding(Padding::horizontal(1));
            let bi = broker_block.inner(cols[2]);
            frame.render_widget(broker_block, cols[2]);

            let clients_style = if broker_clients > 0 { bright } else { dim };
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(vec![
                        "Published: ".into(),
                        Span::styled(format!("{}", published), bright),
                    ]),
                    Line::from(vec![
                        "Clients: ".into(),
                        Span::styled(format!("{}", broker_clients), clients_style),
                    ]),
                ])
                .wrap(Wrap { trim: false }),
                bi,
            );

            // === MIDDLE: two paths ===
            let mid = Layout::vertical([
                Constraint::Length(3), // Mirror path
                Constraint::Length(1), // separator
                Constraint::Min(3),    // Record/File/Playback path
            ])
            .split(cols[1]);

            // --- Path 1: Mirror (Source → Mirror → Broker) ---
            let m_arrow_l = if mirror_on && source_on { "→" } else { " " };
            let m_arrow_r = if mirror_on { "→" } else { " " };
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
                    Span::styled(m_arrow_l, Style::default().fg(mirror_color)),
                    " Mirror ".into(),
                    if mirror_on {
                        Span::styled("●", Style::default().fg(mirror_color).bold())
                    } else {
                        Span::styled("○", dim)
                    },
                    " ".into(),
                    Span::styled(m_arrow_r, Style::default().fg(mirror_color)),
                ]))
                .title(Line::from(mirror_elapsed_spans).alignment(Alignment::Right))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(mirror_color))
                .padding(Padding::horizontal(1));
            let mi = mirror_block.inner(mid[0]);
            frame.render_widget(mirror_block, mid[0]);
            let vfy_matched = state.get_verify_matched();
            let vfy_mismatched = state.get_verify_mismatched();
            let vfy_missing = state.get_verify_missing();
            let vfy_active = vfy_matched + vfy_mismatched + vfy_missing > 0;
            let mut mirror_lines = vec![Line::from(vec![
                "Mirrored: ".into(),
                Span::styled(format!("{}", mirrored), bright),
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

            // --- Separator ---
            frame.render_widget(Paragraph::new("").style(dim), mid[1]);

            // --- Path 2: Record → File → Playback (Source → ... → Broker) ---
            let path2 = Layout::horizontal([
                Constraint::Length(18), // Record
                Constraint::Min(10),    // File (flexible)
                Constraint::Length(18), // Playback
            ])
            .split(mid[2]);

            // Record (Source → Record)
            let r_arrow = if record_on && source_on { "→" } else { " " };
            let record_block = Block::default()
                .title(Line::from(vec![
                    Span::styled(r_arrow, Style::default().fg(record_color)),
                    " Record ".into(),
                    if record_on {
                        Span::styled("●", Style::default().fg(record_color).bold())
                    } else {
                        Span::styled("○", dim)
                    },
                    " ".into(),
                ]))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(record_color))
                .padding(Padding::horizontal(1));
            let ri = record_block.inner(path2[0]);
            frame.render_widget(record_block, path2[0]);

            // Record content with duration
            let mut rec_lines = vec![Line::from(vec![
                "Rec: ".into(),
                Span::styled(format!("{}", recorded), bright),
            ])];
            if let Some(d) = rec_duration {
                rec_lines.push(Line::from(Span::styled(
                    fmt_dur(d),
                    Style::default().fg(Color::Green),
                )));
            }
            frame.render_widget(Paragraph::new(rec_lines), ri);

            // File
            let f_arrow_l = if record_on { "←" } else { " " };
            let f_arrow_r = if playback_on { "→" } else { " " };
            let file_block = Block::default()
                .title(Line::from(vec![
                    Span::styled(f_arrow_l, Style::default().fg(file_color)),
                    " File ".into(),
                    Span::styled(f_arrow_r, Style::default().fg(file_color)),
                ]))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(file_color))
                .padding(Padding::horizontal(1));
            let fi = file_block.inner(path2[1]);
            frame.render_widget(file_block, path2[1]);

            // Show playlist with selection
            let all_files = state.get_all_files();
            let selected_idx = state.get_selected_index();
            let active_idx = state.get_playlist_index();
            let file_lines: Vec<Line> = if all_files.is_empty() {
                let f = state.get_file_path().unwrap_or_else(|| "none".to_string());
                let count_str = state
                    .get_file_line_count(&f)
                    .map(|c| format!(" ({})", c))
                    .unwrap_or_default();
                vec![Line::from(vec![
                    Span::styled(f.clone(), Style::default().fg(Color::White)),
                    Span::styled(count_str, dim),
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
                            dim
                        };
                        let count_str = state
                            .get_file_line_count(f)
                            .map(|c| format!(" ({})", c))
                            .unwrap_or_default();
                        Line::from(vec![
                            Span::styled(format!("{}{}", prefix, f), style),
                            Span::styled(count_str, dim),
                        ])
                    })
                    .collect()
            };
            frame.render_widget(Paragraph::new(file_lines).wrap(Wrap { trim: false }), fi);

            // Playback (Playback → Broker)
            let p_arrow = if playback_on { "→" } else { " " };
            let pb_label = if playback_finished {
                ("◇ done ", dim)
            } else if playback_on && playback_looping {
                ("◆ loop ", Style::default().fg(playback_color).bold())
            } else if playback_on {
                ("◆ once ", Style::default().fg(playback_color).bold())
            } else {
                ("○ off ", dim)
            };
            let playback_block = Block::default()
                .title(Line::from(vec![
                    " Playback ".into(),
                    Span::styled(pb_label.0, pb_label.1),
                    " ".into(),
                    Span::styled(p_arrow, Style::default().fg(playback_color)),
                ]))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(if playback_finished {
                    Color::DarkGray
                } else {
                    playback_color
                }))
                .padding(Padding::horizontal(1));
            let pi = playback_block.inner(path2[2]);
            frame.render_widget(playback_block, path2[2]);

            // Playback content with duration
            let mut play_lines = vec![Line::from(vec![
                "Replayed: ".into(),
                Span::styled(format!("{}", replayed), bright),
            ])];
            if let Some(d) = play_duration {
                play_lines.push(Line::from(Span::styled(
                    fmt_dur(d),
                    Style::default().fg(Color::Magenta),
                )));
            }
            frame.render_widget(Paragraph::new(play_lines), pi);

            // === AUDIT LOG ===
            let audit_entries = state.get_audit_log();
            let audit_on = state.is_audit_enabled();
            let audit_file_on = state.is_audit_file_enabled();
            let scroll_indicator = if log_scroll > 0 {
                format!(" Log ↑{} ", log_scroll)
            } else {
                " Log ".to_string()
            };
            let mut title_spans: Vec<Span> = vec![Span::raw(scroll_indicator)];
            if audit_on {
                title_spans.push(Span::styled("●", Style::default().fg(Color::Green).bold()));
            } else {
                title_spans.push(Span::styled("○", dim));
            }
            title_spans.push(Span::raw(" "));
            if let Some(path) = state.get_audit_file_path() {
                if audit_file_on {
                    title_spans.push(Span::styled(
                        format!("{} ", path),
                        Style::default().fg(Color::Green),
                    ));
                } else {
                    title_spans.push(Span::styled(format!("{} ", path), dim));
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
            let audit_inner = audit_block.inner(vert[1]);
            frame.render_widget(audit_block, vert[1]);
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
                        Span::styled(&e.timestamp, dim),
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

            // === ERROR ===
            if let Some(ref err) = error {
                frame.render_widget(
                    Paragraph::new(Line::from(vec![
                        Span::styled(" ⚠ ", Style::default().fg(Color::Red).bold()),
                        Span::styled(err.as_str(), Style::default().fg(Color::Red)),
                    ])),
                    vert[2],
                );
            }

            // === CONTROLS ===
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
                    Span::styled(".csv", dim),
                    "  [Enter] Save  [Esc] Cancel".into(),
                ])
            } else {
                let audit_on = state.is_audit_enabled();
                let audit_color = if audit_on {
                    Color::Green
                } else {
                    Color::DarkGray
                };
                let loop_ind = if loop_on { "●" } else { "○" };
                let src_ind = if source_on { "●" } else { "○" };
                let mir_ind = if mirror_on { "●" } else { "○" };
                let rec_ind = if record_on { "●" } else { "○" };
                let ply_ind = if playback_on { "●" } else { "○" };
                let aud_ind = if audit_on { "●" } else { "○" };
                Line::from(vec![
                    Span::styled(" [s]", Style::default().fg(source_color)),
                    Span::styled(
                        format!(" Source {} ", src_ind),
                        Style::default().fg(source_color),
                    ),
                    Span::styled("[m]", Style::default().fg(mirror_color)),
                    Span::styled(
                        format!(" Mirror {} ", mir_ind),
                        Style::default().fg(mirror_color),
                    ),
                    Span::styled("[r]", Style::default().fg(record_color)),
                    Span::styled(
                        format!(" Record {} ", rec_ind),
                        Style::default().fg(record_color),
                    ),
                    Span::styled("[p]", Style::default().fg(playback_color)),
                    Span::styled(
                        format!(" Play {} ", ply_ind),
                        Style::default().fg(playback_color),
                    ),
                    Span::styled("[l]", Style::default().fg(playback_color)),
                    Span::styled(
                        format!(" Loop {} ", loop_ind),
                        Style::default().fg(playback_color),
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
                    Span::styled("[↑↓]", dim),
                    " Select  ".into(),
                    Span::styled("[j/k]", dim),
                    " Scroll  ".into(),
                    Span::styled("[q]", Style::default().fg(Color::Red)),
                    " Quit".into(),
                ])
            };
            frame.render_widget(Paragraph::new(controls).wrap(Wrap { trim: false }), vert[3]);
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
