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

/// Actions that can result from handling normal mode keys
#[derive(Debug, PartialEq)]
enum KeyAction {
    None,
    Quit,
    EnterInputMode { target_audit: bool, buffer: String },
}

/// Handle keys when in input mode. Returns true if input mode should be exited.
fn handle_input_mode_key(
    key: KeyCode,
    state: &Arc<TuiState>,
    input_buffer: &mut String,
    input_target_audit: bool,
) -> bool {
    match key {
        KeyCode::Enter => {
            if !input_buffer.is_empty() {
                if input_target_audit {
                    let prev = state.get_audit_file_path();
                    if let Some(ref p) = prev {
                        if *p != *input_buffer {
                            // Write transition message to OLD file before switching
                            state.push_audit(
                                AuditArea::System,
                                AuditSeverity::Info,
                                format!("Audit log file switching to {}", input_buffer),
                            );
                        }
                    }
                    state.set_audit_file_path(input_buffer.clone());
                    state.enable_audit_file();
                    let msg = match prev {
                        Some(p) if p != *input_buffer => {
                            format!("Audit log file set to {} (was {})", input_buffer, p)
                        }
                        _ => format!("Audit log file set to {}", input_buffer),
                    };
                    state.push_audit(AuditArea::System, AuditSeverity::Info, msg);
                } else {
                    state.set_new_file(input_buffer.clone());
                }
            }
            input_buffer.clear();
            true
        }
        KeyCode::Esc => {
            input_buffer.clear();
            true
        }
        KeyCode::Backspace => {
            input_buffer.pop();
            false
        }
        KeyCode::Char(c) => {
            input_buffer.push(c);
            false
        }
        _ => false,
    }
}

/// Handle keys in normal mode. Returns the action to take.
fn handle_normal_key(
    key: crossterm::event::KeyEvent,
    state: &Arc<TuiState>,
    shutdown_tx: &broadcast::Sender<()>,
    log_scroll: &mut usize,
    log_pinned_bottom: &mut bool,
) -> KeyAction {
    match key.code {
        KeyCode::Char('c')
            if key
                .modifiers
                .contains(crossterm::event::KeyModifiers::CONTROL) =>
        {
            state.request_quit();
            let _ = shutdown_tx.send(());
            KeyAction::Quit
        }
        KeyCode::Char('q') => {
            state.request_quit();
            let _ = shutdown_tx.send(());
            KeyAction::Quit
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
                state.push_audit(
                    AuditArea::Mirror,
                    AuditSeverity::Info,
                    format!(
                        "Mirror OFF ({} mirrored, {} published this session{}, {} total)",
                        m_session,
                        p_session,
                        dur_str,
                        state.get_mirrored_count()
                    ),
                );
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
            KeyAction::None
        }
        KeyCode::Char('r') => {
            let enabling = !state.is_recording();
            if enabling {
                // Record and Playback are mutually exclusive
                if state.loop_enabled.load(Ordering::Relaxed) {
                    let session = state.get_playback_session_count();
                    let total = state.get_replayed_count();
                    state.push_audit(
                        AuditArea::Playback,
                        AuditSeverity::Info,
                        format!(
                            "Playback OFF ({} this session, {} total) — recording enabled",
                            session, total
                        ),
                    );
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
            KeyAction::None
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
                    format!("Source enabled (received: {})", state.get_received_count()),
                );
            }
            KeyAction::None
        }
        KeyCode::Char('f') => KeyAction::EnterInputMode {
            target_audit: false,
            buffer: String::new(),
        },
        KeyCode::Char('A') => KeyAction::EnterInputMode {
            target_audit: true,
            buffer: state.get_audit_file_path().unwrap_or_default(),
        },
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
            KeyAction::None
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
            KeyAction::None
        }
        KeyCode::Up => {
            state.navigate_selection(-1);
            KeyAction::None
        }
        KeyCode::Down => {
            state.navigate_selection(1);
            KeyAction::None
        }
        KeyCode::Enter => {
            state.confirm_selection();
            KeyAction::None
        }
        KeyCode::Char('j') => {
            let total = state.get_audit_log().len();
            if total > 0 && *log_scroll < total.saturating_sub(1) {
                *log_scroll += 1;
                *log_pinned_bottom = false;
            }
            KeyAction::None
        }
        KeyCode::Char('k') => {
            *log_scroll = log_scroll.saturating_sub(1);
            if *log_scroll == 0 {
                *log_pinned_bottom = true;
            }
            KeyAction::None
        }
        KeyCode::Char('a') => {
            state.toggle_audit();
            KeyAction::None
        }
        _ => KeyAction::None,
    }
}

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
                        if handle_input_mode_key(
                            key.code,
                            &state,
                            &mut input_buffer,
                            input_target_audit,
                        ) {
                            input_mode = false;
                        }
                    } else {
                        // Handle normal mode keys
                        match handle_normal_key(
                            key,
                            &state,
                            &shutdown_tx,
                            &mut log_scroll,
                            &mut log_pinned_bottom,
                        ) {
                            KeyAction::None => {}
                            KeyAction::Quit => break,
                            KeyAction::EnterInputMode {
                                target_audit,
                                buffer,
                            } => {
                                input_mode = true;
                                input_target_audit = target_audit;
                                input_buffer = buffer;
                            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::state::{TuiConfig, TuiState};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::sync::Arc;
    use tokio::sync::broadcast;

    fn create_test_state() -> Arc<TuiState> {
        Arc::new(TuiState::new(TuiConfig {
            broker_port: 1883,
            file_path: None,
            source_host: None,
            source_port: 1883,
            initial_record: None,
            initial_mirror: true,
            playlist: vec![],
            audit_enabled: true,
            health_check_interval: 60,
        }))
    }

    #[test]
    fn test_handle_input_mode_key_char_input() {
        let state = create_test_state();
        let mut buffer = String::new();

        let result = handle_input_mode_key(KeyCode::Char('a'), &state, &mut buffer, false);

        assert_eq!(buffer, "a");
        assert!(!result);
    }

    #[test]
    fn test_handle_input_mode_key_backspace() {
        let state = create_test_state();
        let mut buffer = "test".to_string();

        let result = handle_input_mode_key(KeyCode::Backspace, &state, &mut buffer, false);

        assert_eq!(buffer, "tes");
        assert!(!result);
    }

    #[test]
    fn test_handle_input_mode_key_esc() {
        let state = create_test_state();
        let mut buffer = "test".to_string();

        let result = handle_input_mode_key(KeyCode::Esc, &state, &mut buffer, false);

        assert_eq!(buffer, "");
        assert!(result);
    }

    #[test]
    fn test_handle_input_mode_key_enter_non_empty_buffer_file() {
        let state = create_test_state();
        let mut buffer = "test.csv".to_string();

        let result = handle_input_mode_key(KeyCode::Enter, &state, &mut buffer, false);

        assert_eq!(buffer, "");
        assert!(result);
        assert_eq!(state.get_file_path(), Some("test.csv".to_string()));
    }

    #[test]
    fn test_handle_input_mode_key_enter_empty_buffer() {
        let state = create_test_state();
        let mut buffer = String::new();

        let result = handle_input_mode_key(KeyCode::Enter, &state, &mut buffer, false);

        assert_eq!(buffer, "");
        assert!(result);
    }

    #[test]
    fn test_handle_input_mode_key_enter_audit_target() {
        let state = create_test_state();
        let mut buffer = "audit.log".to_string();

        let result = handle_input_mode_key(KeyCode::Enter, &state, &mut buffer, true);

        assert_eq!(buffer, "");
        assert!(result);
        assert_eq!(state.get_audit_file_path(), Some("audit.log".to_string()));
    }

    #[test]
    fn test_handle_normal_key_quit() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 0;
        let mut log_pinned_bottom = true;
        let key = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(result, KeyAction::Quit);
        assert!(state.is_quit_requested());
    }

    #[test]
    fn test_handle_normal_key_enter_file_mode() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 0;
        let mut log_pinned_bottom = true;
        let key = KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE);

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(
            result,
            KeyAction::EnterInputMode {
                target_audit: false,
                buffer: String::new()
            }
        );
    }

    #[test]
    fn test_handle_normal_key_enter_audit_mode() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 0;
        let mut log_pinned_bottom = true;
        let key = KeyEvent::new(KeyCode::Char('A'), KeyModifiers::NONE);

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(
            result,
            KeyAction::EnterInputMode {
                target_audit: true,
                buffer: String::new()
            }
        );
    }

    #[test]
    fn test_handle_normal_key_toggle_mirroring() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 0;
        let mut log_pinned_bottom = true;
        let key = KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE);
        let initial_mirroring = state.is_mirroring();

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(result, KeyAction::None);
        assert_eq!(state.is_mirroring(), !initial_mirroring);
    }

    #[test]
    fn test_handle_normal_key_toggle_recording() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 0;
        let mut log_pinned_bottom = true;
        let key = KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE);
        let initial_recording = state.is_recording();

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(result, KeyAction::None);
        assert_eq!(state.is_recording(), !initial_recording);
    }

    #[test]
    fn test_handle_normal_key_toggle_source() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 0;
        let mut log_pinned_bottom = true;
        let key = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE);
        let initial_source = state.is_source_enabled();

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(result, KeyAction::None);
        assert_eq!(state.is_source_enabled(), !initial_source);
    }

    #[test]
    fn test_handle_normal_key_scroll_down() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 0;
        let mut log_pinned_bottom = true;
        let key = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE);

        // Add multiple audit entries to enable scrolling
        state.push_audit(
            crate::tui::types::AuditArea::System,
            crate::tui::types::AuditSeverity::Info,
            "test1".to_string(),
        );
        state.push_audit(
            crate::tui::types::AuditArea::System,
            crate::tui::types::AuditSeverity::Info,
            "test2".to_string(),
        );

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(result, KeyAction::None);
        assert_eq!(log_scroll, 1);
        assert!(!log_pinned_bottom);
    }

    #[test]
    fn test_handle_normal_key_scroll_up() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 5;
        let mut log_pinned_bottom = false;
        let key = KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE);

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(result, KeyAction::None);
        assert_eq!(log_scroll, 4);
    }

    #[test]
    fn test_handle_normal_key_toggle_looping() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 0;
        let mut log_pinned_bottom = true;
        let key = KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE);
        let initial_looping = state.is_playback_looping();

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(result, KeyAction::None);
        assert_eq!(state.is_playback_looping(), !initial_looping);
    }

    #[test]
    fn test_handle_normal_key_toggle_audit() {
        let state = create_test_state();
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut log_scroll = 0;
        let mut log_pinned_bottom = true;
        let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE);
        let initial_audit = state.is_audit_enabled();

        let result = handle_normal_key(
            key,
            &state,
            &shutdown_tx,
            &mut log_scroll,
            &mut log_pinned_bottom,
        );

        assert_eq!(result, KeyAction::None);
        assert_eq!(state.is_audit_enabled(), !initial_audit);
    }
}
