//! TUI input handling and main event loop

use crate::tui::{
    render::Terminal,
    state::TuiState,
    types::{AuditArea, AuditSeverity},
};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::sync::broadcast;

/// Run the TUI event loop. Returns when quit is requested or shutdown signal received.
pub async fn run_tui(
    state: Arc<TuiState>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> std::io::Result<()> {
    let mut terminal = Terminal::new()?;

    // Log session start
    {
        let mut info = format!(
            "Session {} started — broker localhost:{}",
            state.session_id, state.broker_port
        );
        if let Some(ref host) = state.source_host {
            info.push_str(&format!(", source {}:{}", host, state.source_port));
        }
        if let Some(file) = state.get_file_path() {
            info.push_str(&format!(", file {}", file));
        }
        state.push_audit(AuditArea::System, AuditSeverity::Info, info);
    }
    let mut input_mode = false;
    let mut input_target_audit = false; // false = file, true = audit log path
    let mut input_buffer = String::new();
    let mut log_scroll: usize = 0;
    let mut log_pinned_bottom = true;

    loop {
        if log_pinned_bottom {
            log_scroll = 0;
        }
        terminal.draw(
            &state,
            input_mode,
            input_target_audit,
            &input_buffer,
            log_scroll,
        )?;

        // Poll for events with timeout
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    if input_mode {
                        // Handle input mode keys
                        match key.code {
                            KeyCode::Enter => {
                                if !input_buffer.is_empty() {
                                    if input_target_audit {
                                        let prev = state.get_audit_file_path();
                                        if let Some(ref p) = prev {
                                            if *p != input_buffer {
                                                // Write transition message to OLD file before switching
                                                state.push_audit(
                                                    AuditArea::System,
                                                    AuditSeverity::Info,
                                                    format!(
                                                        "Audit log file switching to {}",
                                                        input_buffer
                                                    ),
                                                );
                                            }
                                        }
                                        state.set_audit_file_path(input_buffer.clone());
                                        state.enable_audit_file();
                                        let msg = match prev {
                                            Some(p) if p != input_buffer => format!(
                                                "Audit log file set to {} (was {})",
                                                input_buffer, p
                                            ),
                                            _ => format!("Audit log file set to {}", input_buffer),
                                        };
                                        state.push_audit(
                                            AuditArea::System,
                                            AuditSeverity::Info,
                                            msg,
                                        );
                                    } else {
                                        state.set_new_file(input_buffer.clone());
                                    }
                                }
                                input_buffer.clear();
                                input_mode = false;
                            }
                            KeyCode::Esc => {
                                input_buffer.clear();
                                input_mode = false;
                            }
                            KeyCode::Backspace => {
                                input_buffer.pop();
                            }
                            KeyCode::Char(c) => {
                                input_buffer.push(c);
                            }
                            _ => {}
                        }
                    } else {
                        // Normal mode keys
                        match key.code {
                            KeyCode::Char('c')
                                if key
                                    .modifiers
                                    .contains(crossterm::event::KeyModifiers::CONTROL) =>
                            {
                                state.request_quit();
                                let _ = shutdown_tx.send(());
                                break;
                            }
                            KeyCode::Char('q') => {
                                state.request_quit();
                                let _ = shutdown_tx.send(());
                                break;
                            }
                            KeyCode::Char('m') => {
                                let enabling = !state.is_mirroring();
                                if !enabling {
                                    // Capture session stats before toggling
                                    let (m_session, p_session) = state.get_mirror_session_counts();
                                    let elapsed = state.get_mirror_enabled_elapsed();
                                    let dur_str = elapsed
                                        .map(|d| format!(" in {}s", d.as_secs()))
                                        .unwrap_or_default();
                                    state.push_audit(AuditArea::Mirror, AuditSeverity::Info, format!(
                                        "Mirror OFF ({} mirrored, {} published this session{}, {} total)",
                                        m_session, p_session, dur_str, state.get_mirrored_count()
                                    ));
                                }
                                state.set_mirroring(enabling);
                                state.set_mirror_enabled_at(enabling);
                                if enabling {
                                    state.push_audit(
                                        AuditArea::Mirror,
                                        AuditSeverity::Info,
                                        format!(
                                            "Mirror ON (total: {} mirrored, {} published)",
                                            state.get_mirrored_count(),
                                            state.get_published_count()
                                        ),
                                    );
                                }
                            }
                            KeyCode::Char('r') => {
                                let enabling = !state.is_recording();
                                if enabling {
                                    // Record and Playback are mutually exclusive
                                    if state.loop_enabled.load(Ordering::Relaxed) {
                                        let session = state.get_playback_session_count();
                                        let total = state.get_replayed_count();
                                        state.push_audit(AuditArea::Playback, AuditSeverity::Info, format!(
                                            "Playback OFF ({} this session, {} total) — recording enabled", session, total
                                        ));
                                    }
                                    state.loop_enabled.store(false, Ordering::Relaxed);
                                    state.stop_playback_session();
                                    if state.get_file_path().is_none() {
                                        state.set_new_file(state.generate_default_filename());
                                    }
                                    state.start_recording_session();
                                    let file = state.get_active_file().unwrap_or_default();
                                    state.push_audit(
                                        AuditArea::Record,
                                        AuditSeverity::Info,
                                        format!("Recording ON → {}", file),
                                    );
                                } else {
                                    let session = state.get_recording_session_count();
                                    let total = state.get_recorded_count();
                                    let dur = state.get_recording_duration();
                                    let dur_str = dur
                                        .map(|d| format!(" in {}s", d.as_secs()))
                                        .unwrap_or_default();
                                    state.stop_recording_session();
                                    state.push_audit(
                                        AuditArea::Record,
                                        AuditSeverity::Info,
                                        format!(
                                            "Recording OFF ({} this session{}, {} total)",
                                            session, dur_str, total
                                        ),
                                    );
                                }
                                state.set_recording(enabling);
                            }
                            KeyCode::Char('s') => {
                                let enabling = !state.is_source_enabled();
                                if !enabling {
                                    let elapsed = state.get_source_enabled_elapsed();
                                    let dur_str = elapsed
                                        .map(|d| format!(" in {}s", d.as_secs()))
                                        .unwrap_or_default();
                                    state.push_audit(
                                        AuditArea::Source,
                                        AuditSeverity::Info,
                                        format!(
                                            "Source disabled (received: {}{}, connected: {})",
                                            state.get_received_count(),
                                            dur_str,
                                            if state.is_source_connected() {
                                                "yes"
                                            } else {
                                                "no"
                                            }
                                        ),
                                    );
                                }
                                state.set_source_enabled(enabling);
                                if enabling {
                                    state.reset_source_enabled_at();
                                    state.push_audit(
                                        AuditArea::Source,
                                        AuditSeverity::Info,
                                        format!(
                                            "Source enabled (received: {})",
                                            state.get_received_count()
                                        ),
                                    );
                                }
                            }
                            KeyCode::Char('f') => {
                                input_mode = true;
                                input_target_audit = false;
                                input_buffer.clear();
                            }
                            KeyCode::Char('A') => {
                                input_mode = true;
                                input_target_audit = true;
                                input_buffer = state.get_audit_file_path().unwrap_or_default();
                            }
                            KeyCode::Char('p') => {
                                // Only allow playback if there's an active file
                                if state.get_active_file().is_some() {
                                    let is_finished = state.is_playback_finished();
                                    let is_on = state.loop_enabled.load(Ordering::Relaxed);

                                    if is_finished || !is_on {
                                        // Starting playback (or restarting after done)
                                        if is_on && is_finished {
                                            // Restart: clear finished, reset session
                                            state.start_playback_session();
                                            let file = state.get_active_file().unwrap_or_default();
                                            state.push_audit(
                                                AuditArea::Playback,
                                                AuditSeverity::Info,
                                                format!("Playback restarted → {}", file),
                                            );
                                        } else {
                                            // Fresh start
                                            if state.is_recording() {
                                                let session = state.get_recording_session_count();
                                                let total = state.get_recorded_count();
                                                let dur = state.get_recording_duration();
                                                let dur_str = dur
                                                    .map(|d| format!(" in {}s", d.as_secs()))
                                                    .unwrap_or_default();
                                                state.stop_recording_session();
                                                state.push_audit(AuditArea::Record, AuditSeverity::Info, format!(
                                                    "Recording OFF ({} this session{}, {} total) — playback enabled", session, dur_str, total
                                                ));
                                            }
                                            state.set_recording(false);
                                            state.start_playback_session();
                                            let mode = if state.is_playback_looping() {
                                                "loop"
                                            } else {
                                                "once"
                                            };
                                            let file = state.get_active_file().unwrap_or_default();
                                            state.push_audit(
                                                AuditArea::Playback,
                                                AuditSeverity::Info,
                                                format!("Playback ON ({}) → {}", mode, file),
                                            );
                                            state.loop_enabled.store(true, Ordering::Relaxed);
                                        }
                                    } else {
                                        // Stopping playback
                                        let session = state.get_playback_session_count();
                                        let total = state.get_replayed_count();
                                        let dur = state.get_playback_duration();
                                        let dur_str = dur
                                            .map(|d| format!(" in {}s", d.as_secs()))
                                            .unwrap_or_default();
                                        state.stop_playback_session();
                                        state.push_audit(
                                            AuditArea::Playback,
                                            AuditSeverity::Info,
                                            format!(
                                                "Playback OFF ({} this session, {} total{})",
                                                session, total, dur_str
                                            ),
                                        );
                                        state.loop_enabled.store(false, Ordering::Relaxed);
                                    }
                                }
                            }
                            KeyCode::Char('l') => {
                                // Toggle loop mode for playback
                                let was_looping = state.playback_looping.load(Ordering::Relaxed);
                                state
                                    .playback_looping
                                    .store(!was_looping, Ordering::Relaxed);
                                state.push_audit(
                                    AuditArea::Playback,
                                    AuditSeverity::Info,
                                    format!(
                                        "Playback mode: {}",
                                        if !was_looping { "loop" } else { "once" }
                                    ),
                                );
                            }
                            KeyCode::Up => {
                                state.navigate_selection(-1);
                            }
                            KeyCode::Down => {
                                state.navigate_selection(1);
                            }
                            KeyCode::Enter => {
                                state.confirm_selection();
                            }
                            KeyCode::Char('j') => {
                                let total = state.get_audit_log().len();
                                if total > 0 && log_scroll < total.saturating_sub(1) {
                                    log_scroll += 1;
                                    log_pinned_bottom = false;
                                }
                            }
                            KeyCode::Char('k') => {
                                log_scroll = log_scroll.saturating_sub(1);
                                if log_scroll == 0 {
                                    log_pinned_bottom = true;
                                }
                            }
                            KeyCode::Char('a') => {
                                state.toggle_audit();
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        // Check for external shutdown signal or quit request
        if shutdown_rx.try_recv().is_ok() || state.is_quit_requested() {
            break;
        }
    }

    Ok(())
}
