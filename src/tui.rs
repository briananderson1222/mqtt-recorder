//! Interactive TUI module for mqtt-recorder

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Padding, Paragraph, Wrap},
};
use std::{
    io::{stdout, Stdout},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::broadcast;

/// Application mode (for compatibility, but mirroring/recording are now separate flags)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppMode {
    Record,
    Replay,
    Mirror,
    Passthrough,
}

impl std::fmt::Display for AppMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppMode::Record => write!(f, "record"),
            AppMode::Replay => write!(f, "replay"),
            AppMode::Mirror => write!(f, "mirror"),
            AppMode::Passthrough => write!(f, "passthrough"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum AuditArea {
    Source,
    Broker,
    Record,
    Playback,
    Mirror,
    System,
}

#[derive(Clone, Copy, PartialEq)]
pub enum AuditSeverity {
    Info,
    Warn,
    Error,
}

#[derive(Clone)]
pub struct AuditEntry {
    pub timestamp: String,
    pub area: AuditArea,
    pub severity: AuditSeverity,
    pub message: String,
}

impl AuditArea {
    pub fn label(&self) -> &'static str {
        match self {
            AuditArea::Source => "SRC",
            AuditArea::Broker => "BRK",
            AuditArea::Record => "REC",
            AuditArea::Playback => "PLY",
            AuditArea::Mirror => "MIR",
            AuditArea::System => "SYS",
        }
    }

    pub fn color(&self) -> Color {
        match self {
            AuditArea::Source => Color::Cyan,
            AuditArea::Broker => Color::Green,
            AuditArea::Record => Color::Green,
            AuditArea::Playback => Color::Magenta,
            AuditArea::Mirror => Color::Yellow,
            AuditArea::System => Color::White,
        }
    }
}

/// Shared state for the TUI
pub struct TuiState {
    pub received_count: AtomicU64,
    pub mirrored_count: AtomicU64,
    pub replayed_count: AtomicU64,
    pub published_count: AtomicU64,
    pub recorded_count: AtomicU64,
    pub broker_port: u16,
    pub source_host: Option<String>,
    pub source_port: u16,
    file_path: std::sync::Mutex<Option<String>>,
    new_file_path: std::sync::Mutex<Option<String>>,
    playlist: std::sync::Mutex<Vec<String>>,
    playlist_index: std::sync::atomic::AtomicUsize,
    selected_index: std::sync::atomic::AtomicUsize,
    pub loop_enabled: AtomicBool,
    pub recording_enabled: AtomicBool,
    pub mirroring_enabled: AtomicBool,
    pub source_enabled: AtomicBool,
    pub source_connected: AtomicBool,
    #[allow(dead_code)] // Read via AtomicBool::swap() in set_source_connected
    source_ever_connected: AtomicBool,
    pub quit_requested: AtomicBool,
    last_error: std::sync::Mutex<Option<String>>,
    broker_connections: AtomicUsize,
    // Timing fields
    connected_at: std::sync::Mutex<Option<Instant>>,
    broker_started_at: Instant,
    recording_started: std::sync::Mutex<Option<Instant>>,
    recording_start_count: std::sync::Mutex<u64>,
    playback_started: std::sync::Mutex<Option<Instant>>,
    audit_log: std::sync::Mutex<Vec<AuditEntry>>,
    audit_file: std::sync::Mutex<Option<std::fs::File>>,
    audit_file_path: std::sync::Mutex<Option<String>>,
    audit_file_enabled: AtomicBool,
    audit_enabled: AtomicBool,
}

impl TuiState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        _mode: AppMode,
        broker_port: u16,
        file_path: Option<String>,
        source_host: Option<String>,
        source_port: u16,
        initial_record: Option<bool>,
        initial_mirror: bool,
        playlist: Vec<String>,
        audit_enabled: bool,
    ) -> Self {
        // Default recording to ON if file path provided, unless explicitly set
        let recording = initial_record.unwrap_or(file_path.is_some());
        Self {
            received_count: AtomicU64::new(0),
            mirrored_count: AtomicU64::new(0),
            replayed_count: AtomicU64::new(0),
            published_count: AtomicU64::new(0),
            recorded_count: AtomicU64::new(0),
            broker_port,
            source_host,
            source_port,
            file_path: std::sync::Mutex::new(file_path),
            new_file_path: std::sync::Mutex::new(None),
            playlist: std::sync::Mutex::new(playlist),
            playlist_index: std::sync::atomic::AtomicUsize::new(0),
            selected_index: std::sync::atomic::AtomicUsize::new(0),
            loop_enabled: AtomicBool::new(false),
            recording_enabled: AtomicBool::new(recording),
            mirroring_enabled: AtomicBool::new(initial_mirror),
            source_enabled: AtomicBool::new(true),
            source_connected: AtomicBool::new(false),
            source_ever_connected: AtomicBool::new(false),
            quit_requested: AtomicBool::new(false),
            last_error: std::sync::Mutex::new(None),
            broker_connections: AtomicUsize::new(0),
            connected_at: std::sync::Mutex::new(None),
            broker_started_at: Instant::now(),
            recording_started: std::sync::Mutex::new(None),
            recording_start_count: std::sync::Mutex::new(0),
            playback_started: std::sync::Mutex::new(None),
            audit_log: std::sync::Mutex::new(Vec::new()),
            audit_file: std::sync::Mutex::new(None),
            audit_file_path: std::sync::Mutex::new(None),
            audit_file_enabled: AtomicBool::new(false),
            audit_enabled: AtomicBool::new(audit_enabled),
        }
    }

    // === Connection timing ===
    pub fn set_source_connected(&self, connected: bool) {
        let was_connected = self.source_connected.swap(connected, Ordering::Relaxed);
        if connected && !was_connected {
            let is_reconnect = self.source_ever_connected.swap(true, Ordering::Relaxed);
            let label = if is_reconnect { "Re-connected" } else { "Connected" };
            let severity = if is_reconnect { AuditSeverity::Warn } else { AuditSeverity::Info };
            self.push_audit(AuditArea::Source, severity, format!(
                "{} to {}:{}", label, self.source_host.as_deref().unwrap_or("?"), self.source_port
            ));
            // Clear error on reconnect
            if let Ok(mut guard) = self.last_error.lock() {
                *guard = None;
            }
            if let Ok(mut guard) = self.connected_at.lock() {
                *guard = Some(Instant::now());
            }
        } else if !connected && was_connected {
            let uptime = self.connected_at.lock().ok()
                .and_then(|g| g.map(|t| t.elapsed()))
                .map(|d| {
                    let secs = d.as_secs();
                    if secs >= 3600 { format!("{}h{}m{}s", secs / 3600, (secs % 3600) / 60, secs % 60) }
                    else if secs >= 60 { format!("{}m{}s", secs / 60, secs % 60) }
                    else { format!("{}s", secs) }
                }).unwrap_or_else(|| "?".into());
            self.push_audit(AuditArea::Source, AuditSeverity::Warn, format!(
                "Disconnected (received {} msgs, uptime {})", self.received_count.load(Ordering::Relaxed), uptime
            ));
        }
    }

    pub fn is_source_connected(&self) -> bool {
        self.source_connected.load(Ordering::Relaxed)
    }

    pub fn get_connection_duration(&self) -> Option<Duration> {
        if self.source_connected.load(Ordering::Relaxed) {
            if let Ok(guard) = self.connected_at.lock() {
                return guard.map(|t| t.elapsed());
            }
        }
        None
    }

    /// Get the time when connection was established
    pub fn get_connected_since(&self) -> Option<chrono::DateTime<chrono::Local>> {
        self.get_connection_duration().map(|d| chrono::Local::now() - chrono::Duration::from_std(d).unwrap_or_default())
    }

    // === Broker timing ===
    pub fn get_broker_uptime(&self) -> Duration {
        self.broker_started_at.elapsed()
    }

    pub fn get_broker_started_at(&self) -> chrono::DateTime<chrono::Local> {
        chrono::Local::now() - chrono::Duration::from_std(self.get_broker_uptime()).unwrap_or_default()
    }

    // === Recording timing ===
    pub fn start_recording_session(&self) {
        if let Ok(mut guard) = self.recording_started.lock() {
            *guard = Some(Instant::now());
        }
        if let Ok(mut guard) = self.recording_start_count.lock() {
            *guard = self.recorded_count.load(Ordering::Relaxed);
        }
    }

    pub fn stop_recording_session(&self) {
        if let Ok(mut guard) = self.recording_started.lock() {
            *guard = None;
        }
    }

    pub fn get_recording_duration(&self) -> Option<Duration> {
        if self.recording_enabled.load(Ordering::Relaxed) {
            if let Ok(guard) = self.recording_started.lock() {
                return guard.map(|t| t.elapsed());
            }
        }
        None
    }

    // === Playback timing ===
    pub fn start_playback_session(&self) {
        if let Ok(mut guard) = self.playback_started.lock() {
            *guard = Some(Instant::now());
        }
    }

    pub fn stop_playback_session(&self) {
        if let Ok(mut guard) = self.playback_started.lock() {
            *guard = None;
        }
    }

    pub fn get_playback_duration(&self) -> Option<Duration> {
        if self.loop_enabled.load(Ordering::Relaxed) {
            if let Ok(guard) = self.playback_started.lock() {
                return guard.map(|t| t.elapsed());
            }
        }
        None
    }

    /// Add a file to the playlist (used when switching files)
    pub fn add_to_playlist(&self, path: String) {
        if let Ok(mut guard) = self.playlist.lock() {
            if !guard.contains(&path) {
                guard.push(path);
            }
        }
    }

    /// Get all files (current + playlist)
    pub fn get_all_files(&self) -> Vec<String> {
        let mut files = Vec::new();
        if let Some(current) = self.get_file_path() {
            files.push(current);
        }
        if let Ok(guard) = self.playlist.lock() {
            for f in guard.iter() {
                if !files.contains(f) {
                    files.push(f.clone());
                }
            }
        }
        files
    }

    /// Get playlist index (active file)
    pub fn get_playlist_index(&self) -> usize {
        self.playlist_index.load(Ordering::Relaxed)
    }

    /// Get selected index (cursor position for navigation)
    pub fn get_selected_index(&self) -> usize {
        self.selected_index.load(Ordering::Relaxed)
    }

    /// Navigate selection (delta: -1 for up, +1 for down)
    pub fn navigate_selection(&self, delta: isize) {
        let files = self.get_all_files();
        if files.is_empty() {
            return;
        }
        let current = self.selected_index.load(Ordering::Relaxed);
        let new_index = if delta < 0 {
            if current == 0 {
                files.len() - 1
            } else {
                current - 1
            }
        } else {
            (current + 1) % files.len()
        };
        self.selected_index.store(new_index, Ordering::Relaxed);
    }

    /// Confirm selection - sets playlist_index to selected_index
    pub fn confirm_selection(&self) {
        let selected = self.selected_index.load(Ordering::Relaxed);
        self.playlist_index.store(selected, Ordering::Relaxed);
    }

    /// Get the currently selected file from playlist
    #[allow(dead_code)]
    pub fn get_selected_file(&self) -> Option<String> {
        let files = self.get_all_files();
        let index = self.selected_index.load(Ordering::Relaxed);
        files.get(index).cloned()
    }

    /// Get the active file (confirmed selection)
    #[allow(dead_code)]
    pub fn get_active_file(&self) -> Option<String> {
        let files = self.get_all_files();
        let index = self.playlist_index.load(Ordering::Relaxed);
        files.get(index).cloned()
    }

    pub fn set_error(&self, error: String) {
        self.push_audit(AuditArea::System, AuditSeverity::Error, error.clone());
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = Some(error);
        }
    }

    pub fn push_audit(&self, area: AuditArea, severity: AuditSeverity, message: String) {
        if !self.audit_enabled.load(Ordering::Relaxed) {
            return;
        }
        let ts = chrono::Local::now().format("%H:%M:%S").to_string();
        let sev = match severity {
            AuditSeverity::Info => "INFO",
            AuditSeverity::Warn => "WARN",
            AuditSeverity::Error => "ERR ",
        };
        let flat = format!("{} [{}] [{}] {}", ts, area.label(), sev, message);
        let entry = AuditEntry { timestamp: ts, area, severity, message };
        if let Ok(mut log) = self.audit_log.lock() {
            log.push(entry);
            if log.len() > 200 {
                log.remove(0);
            }
        }
        // Write to file if enabled
        if self.audit_file_enabled.load(Ordering::Relaxed) {
            if let Ok(mut guard) = self.audit_file.lock() {
                if let Some(ref mut file) = *guard {
                    use std::io::Write;
                    let _ = writeln!(file, "{}", flat);
                }
            }
        }
    }

    pub fn set_audit_file_path(&self, path: String) {
        if let Ok(mut guard) = self.audit_file_path.lock() {
            *guard = Some(path);
        }
    }

    pub fn enable_audit_file(&self) {
        let path = self.audit_file_path.lock().ok().and_then(|g| g.clone());
        if let Some(path) = path {
            use std::fs::OpenOptions;
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(file) => {
                    if let Ok(mut guard) = self.audit_file.lock() {
                        *guard = Some(file);
                    }
                    self.audit_file_enabled.store(true, Ordering::Relaxed);
                }
                Err(e) => {
                    self.set_error(format!("Failed to open audit log file: {}", e));
                }
            }
        }
    }

    pub fn toggle_audit(&self) -> bool {
        let was_enabled = self.audit_enabled.load(Ordering::Relaxed);
        if was_enabled {
            // Push final entry before disabling
            self.push_audit(AuditArea::System, AuditSeverity::Info, "Auditing disabled".into());
            self.audit_enabled.store(false, Ordering::Relaxed);
            // Close audit file if open
            self.audit_file_enabled.store(false, Ordering::Relaxed);
            if let Ok(mut guard) = self.audit_file.lock() {
                *guard = None;
            }
            false
        } else {
            self.audit_enabled.store(true, Ordering::Relaxed);
            // Re-open audit file if path configured
            self.enable_audit_file();
            self.push_audit(AuditArea::System, AuditSeverity::Info, "Auditing enabled".into());
            true
        }
    }

    pub fn get_audit_log(&self) -> Vec<AuditEntry> {
        self.audit_log.lock().ok().map(|g| g.clone()).unwrap_or_default()
    }

    pub fn set_broker_connections(&self, count: usize) {
        let old = self.broker_connections.swap(count, Ordering::Relaxed);
        if old != count {
            self.push_audit(AuditArea::Broker, AuditSeverity::Info, format!(
                "Connections: {} → {}", old, count
            ));
        }
    }

    pub fn get_broker_connections(&self) -> usize {
        self.broker_connections.load(Ordering::Relaxed)
    }

    pub fn get_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|g| g.clone())
    }

    /// Get the current file path
    pub fn get_file_path(&self) -> Option<String> {
        self.file_path.lock().ok().and_then(|g| g.clone())
    }

    /// Generate a default filename based on source host and current timestamp
    pub fn generate_default_filename(&self) -> String {
        let host = self.source_host.as_deref().unwrap_or("recording");
        // Sanitize host for filename (replace dots and colons)
        let safe_host: String = host
            .chars()
            .map(|c| if c == '.' || c == ':' { '-' } else { c })
            .collect();
        let timestamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
        format!("{}-{}.csv", safe_host, timestamp)
    }

    /// Set a new file path - appends .csv if not present, adds old file to playlist
    pub fn set_new_file(&self, path: String) {
        let path = if path.ends_with(".csv") {
            path
        } else {
            format!("{}.csv", path)
        };
        // Add old file to playlist before switching
        if let Some(old_path) = self.get_file_path() {
            self.add_to_playlist(old_path);
        }
        if let Ok(mut guard) = self.new_file_path.lock() {
            *guard = Some(path.clone());
        }
        if let Ok(mut guard) = self.file_path.lock() {
            *guard = Some(path);
        }
    }

    /// Take the new file path if one was set (clears it)
    pub fn take_new_file(&self) -> Option<String> {
        if let Ok(mut guard) = self.new_file_path.lock() {
            guard.take()
        } else {
            None
        }
    }

    pub fn is_source_enabled(&self) -> bool {
        self.source_enabled.load(Ordering::Relaxed)
    }

    pub fn set_source_enabled(&self, enabled: bool) {
        self.source_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn is_recording(&self) -> bool {
        self.recording_enabled.load(Ordering::Relaxed)
    }

    pub fn set_recording(&self, enabled: bool) {
        self.recording_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn is_mirroring(&self) -> bool {
        self.mirroring_enabled.load(Ordering::Relaxed)
    }

    pub fn set_mirroring(&self, enabled: bool) {
        self.mirroring_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn increment_received(&self) {
        self.received_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_mirrored(&self) {
        self.mirrored_count.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn increment_replayed(&self) {
        self.replayed_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_published(&self) {
        self.published_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_recorded(&self) {
        self.recorded_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_received_count(&self) -> u64 {
        self.received_count.load(Ordering::Relaxed)
    }

    pub fn get_mirrored_count(&self) -> u64 {
        self.mirrored_count.load(Ordering::Relaxed)
    }

    pub fn get_replayed_count(&self) -> u64 {
        self.replayed_count.load(Ordering::Relaxed)
    }

    pub fn get_published_count(&self) -> u64 {
        self.published_count.load(Ordering::Relaxed)
    }

    pub fn get_recorded_count(&self) -> u64 {
        self.recorded_count.load(Ordering::Relaxed)
    }

    pub fn request_quit(&self) {
        self.push_audit(AuditArea::System, AuditSeverity::Info, format!(
            "Quit requested — received {}, mirrored {}, recorded {}, replayed {}",
            self.get_received_count(),
            self.get_mirrored_count(),
            self.get_recorded_count(),
            self.get_replayed_count(),
        ));
        self.quit_requested.store(true, Ordering::Relaxed);
    }

    pub fn is_quit_requested(&self) -> bool {
        self.quit_requested.load(Ordering::Relaxed)
    }
}

/// Check if interactive mode should be enabled
pub fn should_enable_interactive(no_interactive_flag: bool) -> bool {
    if no_interactive_flag {
        return false;
    }
    if std::env::var("CI").is_ok() {
        return false;
    }
    atty::is(atty::Stream::Stdout)
}

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
            let broker_clients = state.get_broker_connections();

            // Get timing info
            let conn_duration = state.get_connection_duration();
            let rec_duration = state.get_recording_duration();
            let play_duration = state.get_playback_duration();

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
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan));
            let inner = outer.inner(area);
            frame.render_widget(outer, area);

            // Main layout: [Source | Middle | Broker] + audit log + error + controls
            let vert = Layout::vertical([
                Constraint::Min(8),
                Constraint::Length(6), // Audit log
                Constraint::Length(1),
                Constraint::Length(1),
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
            let source_block = Block::default()
                .title(Line::from(vec![
                    " Source ".into(),
                    source_health,
                    " ".into(),
                ]))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(source_color))
                .padding(Padding::horizontal(1));
            let si = source_block.inner(cols[0]);
            frame.render_widget(source_block, cols[0]);

            // Source content with connection duration
            let conn_status = if let Some(d) = conn_duration {
                let since = state.get_connected_since();
                let date_str = since
                    .map(|dt| dt.format("%m/%d %H:%M").to_string())
                    .unwrap_or_default();
                Line::from(vec![
                    Span::styled(format!("{} ", date_str), dim),
                    Span::styled(fmt_dur(d), Style::default().fg(Color::Green)),
                ])
            } else if source_on {
                Line::from(Span::styled(
                    "Connecting...",
                    Style::default().fg(Color::Yellow),
                ))
            } else {
                Line::from(Span::styled("Disabled", dim))
            };
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(vec![
                        "Received: ".into(),
                        Span::styled(format!("{}", received), bright),
                    ]),
                    conn_status,
                    Line::from(Span::styled(&source_url, dim)),
                ])
                .wrap(Wrap { trim: false }),
                si,
            );

            // === BROKER (right, full height) ===
            let broker_health = Span::styled("●", Style::default().fg(broker_color));
            let broker_block = Block::default()
                .title(Line::from(vec![
                    " Broker ".into(),
                    broker_health,
                    " ".into(),
                ]))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(broker_color))
                .padding(Padding::horizontal(1));
            let bi = broker_block.inner(cols[2]);
            frame.render_widget(broker_block, cols[2]);

            // Broker uptime
            let broker_uptime = state.get_broker_uptime();
            let broker_started = state.get_broker_started_at();
            let broker_date_str = broker_started.format("%m/%d %H:%M").to_string();
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
                    Line::from(vec![
                        Span::styled(format!("{} ", broker_date_str), dim),
                        Span::styled(fmt_dur(broker_uptime), Style::default().fg(Color::Green)),
                    ]),
                    Line::from(Span::styled(&broker_url, dim)),
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
            let mirror_block = Block::default()
                .title(Line::from(vec![
                    Span::styled(m_arrow_l, Style::default().fg(mirror_color)),
                    " Mirror ".into(),
                    if mirror_on {
                        Span::styled("● ON ", Style::default().fg(mirror_color).bold())
                    } else {
                        Span::styled("○ off ", dim)
                    },
                    Span::styled(m_arrow_r, Style::default().fg(mirror_color)),
                ]))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(mirror_color))
                .padding(Padding::horizontal(1));
            let mi = mirror_block.inner(mid[0]);
            frame.render_widget(mirror_block, mid[0]);
            frame.render_widget(
                Paragraph::new(Line::from(vec![
                    "Mirrored: ".into(),
                    Span::styled(format!("{}", mirrored), bright),
                ])),
                mi,
            );

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
                vec![Line::from(Span::styled(
                    f,
                    Style::default().fg(Color::White),
                ))]
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
                        Line::from(Span::styled(format!("{}{}", prefix, f), style))
                    })
                    .collect()
            };
            frame.render_widget(Paragraph::new(file_lines).wrap(Wrap { trim: false }), fi);

            // Playback (Playback → Broker)
            let p_arrow = if playback_on { "→" } else { " " };
            let pb_label = if playback_on { "◆ on " } else { "○ off " };
            let playback_block = Block::default()
                .title(Line::from(vec![
                    " Playback ".into(),
                    if playback_on {
                        Span::styled(pb_label, Style::default().fg(playback_color).bold())
                    } else {
                        Span::styled(pb_label, dim)
                    },
                    Span::styled(p_arrow, Style::default().fg(playback_color)),
                ]))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(playback_color))
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
            let scroll_indicator = if log_scroll > 0 {
                format!(" Log ↑{} ", log_scroll)
            } else {
                " Log ".to_string()
            };
            let audit_block = Block::default()
                .title(scroll_indicator)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray));
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
                        Span::styled(format!(" [{}] ", e.area.label()), Style::default().fg(e.area.color())),
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
            let controls = if input_mode {
                Line::from(vec![
                    " New file: ".into(),
                    Span::styled(input_buffer, Style::default().fg(Color::White)),
                    Span::styled("█", Style::default().fg(Color::White)),
                    Span::styled(".csv", dim),
                    "  [Enter] Save  [Esc] Cancel".into(),
                ])
            } else {
                Line::from(vec![
                    Span::styled(" [s]", Style::default().fg(Color::Cyan)),
                    " Source  ".into(),
                    Span::styled("[m]", Style::default().fg(Color::Yellow)),
                    " Mirror  ".into(),
                    Span::styled("[r]", Style::default().fg(Color::Green)),
                    " Record  ".into(),
                    Span::styled("[p]", Style::default().fg(Color::Magenta)),
                    " Playback  ".into(),
                    Span::styled("[f]", Style::default().fg(Color::White)),
                    " File  ".into(),
                    Span::styled("[a]", Style::default().fg(Color::DarkGray)),
                    " Audit  ".into(),
                    Span::styled("[↑↓]", dim),
                    " Select  ".into(),
                    Span::styled("[j/k]", dim),
                    " Scroll  ".into(),
                    Span::styled("[q]", Style::default().fg(Color::Red)),
                    " Quit".into(),
                ])
            };
            frame.render_widget(Paragraph::new(controls), vert[3]);
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

/// Run the TUI event loop. Returns when quit is requested or shutdown signal received.
pub async fn run_tui(
    state: Arc<TuiState>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> std::io::Result<()> {
    let mut terminal = Terminal::new()?;

    // Log session start
    {
        let mut info = format!("Session started — broker :{}", state.broker_port);
        if let Some(ref host) = state.source_host {
            info.push_str(&format!(", source {}:{}", host, state.source_port));
        }
        if let Some(file) = state.get_file_path() {
            info.push_str(&format!(", file {}", file));
        }
        state.push_audit(AuditArea::System, AuditSeverity::Info, info);
    }
    let mut input_mode = false;
    let mut input_buffer = String::new();
    let mut log_scroll: usize = 0;
    let mut log_pinned_bottom = true;

    loop {
        if log_pinned_bottom {
            log_scroll = 0;
        }
        terminal.draw(&state, input_mode, &input_buffer, log_scroll)?;

        // Poll for events with timeout
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    if input_mode {
                        // Handle input mode keys
                        match key.code {
                            KeyCode::Enter => {
                                if !input_buffer.is_empty() {
                                    state.set_new_file(input_buffer.clone());
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
                                state.set_mirroring(enabling);
                                state.push_audit(AuditArea::Mirror, AuditSeverity::Info, format!(
                                    "Mirror {} (mirrored: {}, published: {})",
                                    if enabling { "ON" } else { "OFF" },
                                    state.get_mirrored_count(), state.get_published_count()
                                ));
                            }
                            KeyCode::Char('r') => {
                                let enabling = !state.is_recording();
                                if enabling {
                                    // Record and Playback are mutually exclusive
                                    if state.loop_enabled.load(Ordering::Relaxed) {
                                        let replayed = state.get_replayed_count();
                                        state.push_audit(AuditArea::Playback, AuditSeverity::Info, format!(
                                            "Playback OFF ({} replayed) — recording enabled", replayed
                                        ));
                                    }
                                    state.loop_enabled.store(false, Ordering::Relaxed);
                                    state.stop_playback_session();
                                    if state.get_file_path().is_none() {
                                        state.set_new_file(state.generate_default_filename());
                                    }
                                    state.start_recording_session();
                                    let file = state.get_active_file().unwrap_or_default();
                                    state.push_audit(AuditArea::Record, AuditSeverity::Info, format!(
                                        "Recording ON → {}", file
                                    ));
                                } else {
                                    let recorded = state.get_recorded_count();
                                    let dur = state.get_recording_duration();
                                    let dur_str = dur.map(|d| format!(" in {}s", d.as_secs())).unwrap_or_default();
                                    state.stop_recording_session();
                                    state.push_audit(AuditArea::Record, AuditSeverity::Info, format!(
                                        "Recording OFF ({} msgs{})", recorded, dur_str
                                    ));
                                }
                                state.set_recording(enabling);
                            }
                            KeyCode::Char('s') => {
                                let enabling = !state.is_source_enabled();
                                state.set_source_enabled(enabling);
                                state.push_audit(AuditArea::Source, AuditSeverity::Info, format!(
                                    "Source {} (received: {})",
                                    if enabling { "enabled" } else { "disabled" },
                                    state.get_received_count()
                                ));
                            }
                            KeyCode::Char('f') => {
                                input_mode = true;
                                input_buffer.clear();
                            }
                            KeyCode::Char('p') => {
                                // Only allow playback if there's an active file
                                if state.get_active_file().is_some() {
                                    let enabling = !state.loop_enabled.load(Ordering::Relaxed);
                                    if enabling {
                                        // Playback and Record are mutually exclusive
                                        if state.is_recording() {
                                            let recorded = state.get_recorded_count();
                                            let dur = state.get_recording_duration();
                                            let dur_str = dur.map(|d| format!(" in {}s", d.as_secs())).unwrap_or_default();
                                            state.stop_recording_session();
                                            state.push_audit(AuditArea::Record, AuditSeverity::Info, format!(
                                                "Recording OFF ({} msgs{}) — playback enabled", recorded, dur_str
                                            ));
                                        }
                                        state.set_recording(false);
                                        state.start_playback_session();
                                        let file = state.get_active_file().unwrap_or_default();
                                        state.push_audit(AuditArea::Playback, AuditSeverity::Info, format!(
                                            "Playback ON → {}", file
                                        ));
                                    } else {
                                        let replayed = state.get_replayed_count();
                                        let dur = state.get_playback_duration();
                                        let dur_str = dur.map(|d| format!(" in {}s", d.as_secs())).unwrap_or_default();
                                        state.stop_playback_session();
                                        state.push_audit(AuditArea::Playback, AuditSeverity::Info, format!(
                                            "Playback OFF ({} replayed{})", replayed, dur_str
                                        ));
                                    }
                                    state.loop_enabled.store(enabling, Ordering::Relaxed);
                                }
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

        // Check for external shutdown signal
        if shutdown_rx.try_recv().is_ok() {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_mode_display() {
        assert_eq!(AppMode::Record.to_string(), "record");
        assert_eq!(AppMode::Replay.to_string(), "replay");
        assert_eq!(AppMode::Mirror.to_string(), "mirror");
        assert_eq!(AppMode::Passthrough.to_string(), "passthrough");
    }

    #[test]
    fn test_app_mode_equality() {
        assert_eq!(AppMode::Record, AppMode::Record);
        assert_eq!(AppMode::Replay, AppMode::Replay);
        assert_ne!(AppMode::Record, AppMode::Replay);
    }

    #[test]
    fn test_app_mode_clone() {
        let mode = AppMode::Record;
        let cloned = mode;
        assert_eq!(mode, cloned);
    }

    #[test]
    fn test_app_mode_debug() {
        let mode = AppMode::Record;
        let debug_str = format!("{:?}", mode);
        assert!(debug_str.contains("Record"));
    }

    #[test]
    fn test_tui_state_new() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            Some("test.csv".to_string()),
            Some("broker.local".to_string()),
            1883,
            None,
            true,
            vec![],
            true,
        );
        assert_eq!(state.broker_port, 1883);
        assert_eq!(state.get_file_path(), Some("test.csv".to_string()));
        assert_eq!(state.source_host, Some("broker.local".to_string()));
        assert_eq!(state.get_received_count(), 0);
        assert_eq!(state.get_mirrored_count(), 0);
        assert_eq!(state.get_published_count(), 0);
        assert_eq!(state.get_recorded_count(), 0);
        assert!(!state.loop_enabled.load(Ordering::Relaxed));
        assert!(!state.is_quit_requested());
        // Recording enabled by default when file path provided
        assert!(state.is_recording());
        assert!(state.is_mirroring());
        assert!(state.is_source_enabled());
    }

    #[test]
    fn test_tui_state_initial_flags() {
        // Test explicit initial states
        let state = TuiState::new(
            AppMode::Mirror,
            1883,
            Some("test.csv".to_string()),
            None,
            1883,
            Some(false),
            false,
            vec![],
            true,
        );
        assert!(!state.is_recording()); // explicitly disabled
        assert!(!state.is_mirroring()); // explicitly disabled
    }

    #[test]
    fn test_tui_state_recording_toggle() {
        let state = TuiState::new(
            AppMode::Mirror,
            1883,
            Some("test.csv".to_string()),
            None,
            1883,
            None,
            true,
            vec![],
            true,
        );

        assert!(state.is_recording());
        state.set_recording(false);
        assert!(!state.is_recording());
        state.set_recording(true);
        assert!(state.is_recording());
    }

    #[test]
    fn test_tui_state_mirroring_toggle() {
        let state = TuiState::new(AppMode::Mirror, 1883, None, None, 1883, None, true, vec![], true);

        assert!(state.is_mirroring());
        state.set_mirroring(false);
        assert!(!state.is_mirroring());
        state.set_mirroring(true);
        assert!(state.is_mirroring());
    }

    #[test]
    fn test_tui_state_source_toggle() {
        let state = TuiState::new(AppMode::Mirror, 1883, None, None, 1883, None, true, vec![], true);

        assert!(state.is_source_enabled());
        state.set_source_enabled(false);
        assert!(!state.is_source_enabled());
        state.set_source_enabled(true);
        assert!(state.is_source_enabled());
    }

    #[test]
    fn test_tui_state_counters() {
        let state = TuiState::new(AppMode::Record, 1883, None, None, 1883, None, true, vec![], true);

        assert_eq!(state.get_received_count(), 0);
        state.increment_received();
        assert_eq!(state.get_received_count(), 1);
        state.increment_received();
        state.increment_received();
        assert_eq!(state.get_received_count(), 3);

        assert_eq!(state.get_mirrored_count(), 0);
        state.increment_mirrored();
        assert_eq!(state.get_mirrored_count(), 1);

        assert_eq!(state.get_published_count(), 0);
        state.increment_published();
        state.increment_published();
        assert_eq!(state.get_published_count(), 2);

        assert_eq!(state.get_recorded_count(), 0);
        state.increment_recorded();
        assert_eq!(state.get_recorded_count(), 1);
    }

    #[test]
    fn test_tui_state_quit_requested() {
        let state = TuiState::new(AppMode::Record, 1883, None, None, 1883, None, true, vec![], true);
        assert!(!state.is_quit_requested());
        state.request_quit();
        assert!(state.is_quit_requested());
    }

    #[test]
    fn test_tui_state_loop_enabled() {
        let state = TuiState::new(AppMode::Record, 1883, None, None, 1883, None, true, vec![], true);
        assert!(!state.loop_enabled.load(Ordering::Relaxed));

        state.loop_enabled.store(true, Ordering::Relaxed);
        assert!(state.loop_enabled.load(Ordering::Relaxed));
    }

    #[test]
    fn test_should_enable_interactive_no_interactive_flag() {
        assert!(!should_enable_interactive(true));
    }

    #[test]
    fn test_tui_state_none_file_path() {
        let state = TuiState::new(AppMode::Record, 1883, None, None, 1883, None, true, vec![], true);
        assert!(state.get_file_path().is_none());
    }

    #[test]
    fn test_tui_state_set_new_file_updates_display_path() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            Some("original.csv".to_string()),
            None,
            1883,
            None,
            true,
            vec![],
            true,
        );
        assert_eq!(state.get_file_path(), Some("original.csv".to_string()));

        state.set_new_file("new_file.csv".to_string());

        // Display path should update immediately
        assert_eq!(state.get_file_path(), Some("new_file.csv".to_string()));
        // New file path should be available for mirror to pick up
        assert_eq!(state.take_new_file(), Some("new_file.csv".to_string()));
        // After take, it should be None
        assert_eq!(state.take_new_file(), None);
    }

    #[test]
    fn test_tui_state_set_new_file_from_none() {
        let state = TuiState::new(AppMode::Record, 1883, None, None, 1883, None, true, vec![], true);
        assert!(state.get_file_path().is_none());

        state.set_new_file("first_file.csv".to_string());

        assert_eq!(state.get_file_path(), Some("first_file.csv".to_string()));
    }

    #[test]
    fn test_tui_state_set_new_file_appends_csv() {
        let state = TuiState::new(AppMode::Record, 1883, None, None, 1883, None, true, vec![], true);

        state.set_new_file("myfile".to_string());
        assert_eq!(state.get_file_path(), Some("myfile.csv".to_string()));

        state.set_new_file("another.csv".to_string());
        assert_eq!(state.get_file_path(), Some("another.csv".to_string()));
    }

    #[test]
    fn test_tui_state_generate_default_filename() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            Some("broker.example.com".to_string()),
            1883,
            None,
            true,
            vec![],
            true,
        );
        let filename = state.generate_default_filename();

        assert!(filename.starts_with("broker-example-com-"));
        assert!(filename.ends_with(".csv"));
    }

    #[test]
    fn test_tui_state_generate_default_filename_no_host() {
        let state = TuiState::new(AppMode::Record, 1883, None, None, 1883, None, true, vec![], true);
        let filename = state.generate_default_filename();

        assert!(filename.starts_with("recording-"));
        assert!(filename.ends_with(".csv"));
    }

    #[test]
    fn test_tui_state_selection_separate_from_active() {
        let state = TuiState::new(
            AppMode::Replay,
            1883,
            Some("file1.csv".to_string()),
            None,
            1883,
            None,
            true,
            vec!["file2.csv".to_string(), "file3.csv".to_string()],
            true,
        );

        // Initially both at 0
        assert_eq!(state.get_selected_index(), 0);
        assert_eq!(state.get_playlist_index(), 0);

        // Navigate selection without confirming
        state.navigate_selection(1);
        assert_eq!(state.get_selected_index(), 1);
        assert_eq!(state.get_playlist_index(), 0); // Active unchanged

        state.navigate_selection(1);
        assert_eq!(state.get_selected_index(), 2);
        assert_eq!(state.get_playlist_index(), 0); // Active still unchanged

        // Confirm selection
        state.confirm_selection();
        assert_eq!(state.get_selected_index(), 2);
        assert_eq!(state.get_playlist_index(), 2); // Now active matches
    }

    #[test]
    fn test_tui_state_selection_wraps_around() {
        let state = TuiState::new(
            AppMode::Replay,
            1883,
            Some("a.csv".to_string()),
            None,
            1883,
            None,
            true,
            vec!["b.csv".to_string()],
            true,
        );
        assert_eq!(state.get_selected_index(), 0);
        state.navigate_selection(1);
        assert_eq!(state.get_selected_index(), 1);
        state.navigate_selection(1);
        assert_eq!(state.get_selected_index(), 0); // Wrapped

        state.navigate_selection(-1);
        assert_eq!(state.get_selected_index(), 1); // Wrapped backwards
    }
}
