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

/// Generate a default CSV filename from an optional host and current timestamp.
pub fn generate_default_filename(host: Option<&str>) -> String {
    let safe_host: String = host.unwrap_or("recording")
        .chars()
        .map(|c| if c == '.' || c == ':' { '-' } else { c })
        .collect();
    let timestamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
    format!("{}-{}.csv", safe_host, timestamp)
}

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
    pub session_id: String,
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
    first_connected_at: std::sync::Mutex<Option<Instant>>,
    connected_at: std::sync::Mutex<Option<Instant>>,
    source_enabled_at: std::sync::Mutex<Option<Instant>>,
    mirror_enabled_at: std::sync::Mutex<Option<Instant>>,
    mirror_start_count: std::sync::Mutex<(u64, u64)>, // (mirrored, published) at session start
    broker_started_at: Instant,
    recording_started: std::sync::Mutex<Option<Instant>>,
    recording_start_count: std::sync::Mutex<u64>,
    playback_started: std::sync::Mutex<Option<Instant>>,
    playback_start_count: std::sync::Mutex<u64>,
    // Playback mode
    pub playback_looping: AtomicBool,
    pub playback_finished: AtomicBool,
    // Audit
    audit_log: std::sync::Mutex<Vec<AuditEntry>>,
    audit_file: std::sync::Mutex<Option<std::fs::File>>,
    audit_file_path: std::sync::Mutex<Option<String>>,
    audit_file_enabled: AtomicBool,
    audit_enabled: AtomicBool,
    file_line_cache: std::sync::Mutex<std::collections::HashMap<String, (usize, Instant)>>,
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
        let session_id = format!("{:08x}", std::process::id() as u64 ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().subsec_nanos() as u64);
        Self {
            session_id,
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
            first_connected_at: std::sync::Mutex::new(None),
            connected_at: std::sync::Mutex::new(None),
            source_enabled_at: std::sync::Mutex::new(Some(Instant::now())),
            mirror_enabled_at: std::sync::Mutex::new(if initial_mirror { Some(Instant::now()) } else { None }),
            mirror_start_count: std::sync::Mutex::new((0, 0)),
            broker_started_at: Instant::now(),
            recording_started: std::sync::Mutex::new(None),
            recording_start_count: std::sync::Mutex::new(0),
            playback_started: std::sync::Mutex::new(None),
            playback_start_count: std::sync::Mutex::new(0),
            playback_looping: AtomicBool::new(false),
            playback_finished: AtomicBool::new(false),
            audit_log: std::sync::Mutex::new(Vec::new()),
            audit_file: std::sync::Mutex::new(None),
            audit_file_path: std::sync::Mutex::new(None),
            audit_file_enabled: AtomicBool::new(false),
            audit_enabled: AtomicBool::new(audit_enabled),
            file_line_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
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
            if let Ok(mut guard) = self.last_error.lock() {
                *guard = None;
            }
            if let Ok(mut guard) = self.connected_at.lock() {
                *guard = Some(Instant::now());
            }
            // Track first-ever connection
            if let Ok(mut guard) = self.first_connected_at.lock() {
                if guard.is_none() {
                    *guard = Some(Instant::now());
                }
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
    #[allow(dead_code)]
    pub fn get_connected_since(&self) -> Option<chrono::DateTime<chrono::Local>> {
        self.get_connection_duration().map(|d| chrono::Local::now() - chrono::Duration::from_std(d).unwrap_or_default())
    }

    /// Get the time of first-ever connection (never resets)
    pub fn get_first_connected_since(&self) -> Option<chrono::DateTime<chrono::Local>> {
        self.first_connected_at.lock().ok()
            .and_then(|g| g.map(|t| t.elapsed()))
            .map(|d| chrono::Local::now() - chrono::Duration::from_std(d).unwrap_or_default())
    }

    /// Get elapsed since source was last enabled (resets on toggle)
    pub fn get_source_enabled_elapsed(&self) -> Option<Duration> {
        if self.source_enabled.load(Ordering::Relaxed) {
            self.source_enabled_at.lock().ok().and_then(|g| g.map(|t| t.elapsed()))
        } else {
            None
        }
    }

    /// Reset source enabled timer
    pub fn reset_source_enabled_at(&self) {
        if let Ok(mut guard) = self.source_enabled_at.lock() {
            *guard = Some(Instant::now());
        }
    }

    /// Get elapsed since mirror was last enabled (resets on toggle)
    pub fn get_mirror_enabled_elapsed(&self) -> Option<Duration> {
        if self.mirroring_enabled.load(Ordering::Relaxed) {
            self.mirror_enabled_at.lock().ok().and_then(|g| g.map(|t| t.elapsed()))
        } else {
            None
        }
    }

    /// Reset mirror enabled timer
    pub fn set_mirror_enabled_at(&self, enabled: bool) {
        if let Ok(mut guard) = self.mirror_enabled_at.lock() {
            *guard = if enabled { Some(Instant::now()) } else { None };
        }
        if enabled {
            if let Ok(mut guard) = self.mirror_start_count.lock() {
                *guard = (self.mirrored_count.load(Ordering::Relaxed), self.published_count.load(Ordering::Relaxed));
            }
        }
    }

    /// Get mirror session counts: (mirrored_this_session, published_this_session)
    pub fn get_mirror_session_counts(&self) -> (u64, u64) {
        let m_total = self.mirrored_count.load(Ordering::Relaxed);
        let p_total = self.published_count.load(Ordering::Relaxed);
        let (m_start, p_start) = self.mirror_start_count.lock().ok().map(|g| *g).unwrap_or((0, 0));
        (m_total.saturating_sub(m_start), p_total.saturating_sub(p_start))
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

    /// Get the number of messages recorded in the current session
    pub fn get_recording_session_count(&self) -> u64 {
        let total = self.recorded_count.load(Ordering::Relaxed);
        let start = self.recording_start_count.lock().ok().map(|g| *g).unwrap_or(0);
        total.saturating_sub(start)
    }

    // === Playback timing ===
    pub fn start_playback_session(&self) {
        if let Ok(mut guard) = self.playback_started.lock() {
            *guard = Some(Instant::now());
        }
        if let Ok(mut guard) = self.playback_start_count.lock() {
            *guard = self.replayed_count.load(Ordering::Relaxed);
        }
        self.playback_finished.store(false, Ordering::Relaxed);
    }

    pub fn stop_playback_session(&self) {
        if let Ok(mut guard) = self.playback_started.lock() {
            *guard = None;
        }
    }

    /// Get the number of messages replayed in the current session
    pub fn get_playback_session_count(&self) -> u64 {
        let total = self.replayed_count.load(Ordering::Relaxed);
        let start = self.playback_start_count.lock().ok().map(|g| *g).unwrap_or(0);
        total.saturating_sub(start)
    }

    pub fn get_playback_duration(&self) -> Option<Duration> {
        if self.loop_enabled.load(Ordering::Relaxed) {
            if let Ok(guard) = self.playback_started.lock() {
                return guard.map(|t| t.elapsed());
            }
        }
        None
    }

    #[allow(dead_code)]
    pub fn is_playback_active(&self) -> bool {
        self.loop_enabled.load(Ordering::Relaxed) && !self.is_recording()
    }

    pub fn is_playback_looping(&self) -> bool {
        self.playback_looping.load(Ordering::Relaxed)
    }

    pub fn is_playback_finished(&self) -> bool {
        self.playback_finished.load(Ordering::Relaxed)
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
        let flat = format!("{} [{}] [{}] [{}] {}", ts, self.session_id, area.label(), sev, message);
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

    pub fn get_audit_file_path(&self) -> Option<String> {
        self.audit_file_path.lock().ok().and_then(|g| g.clone())
    }

    pub fn is_audit_enabled(&self) -> bool {
        self.audit_enabled.load(Ordering::Relaxed)
    }

    pub fn is_audit_file_enabled(&self) -> bool {
        self.audit_file_enabled.load(Ordering::Relaxed)
    }

    pub fn enable_audit_file(&self) {
        let path = self.audit_file_path.lock().ok().and_then(|g| g.clone());
        if let Some(path) = path {
            // Preload existing file content into TUI log
            self.preload_audit_file(&path);
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

    fn preload_audit_file(&self, path: &str) {
        use std::io::BufRead;
        let file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(_) => return,
        };
        let reader = std::io::BufReader::new(file);
        if let Ok(mut log) = self.audit_log.lock() {
            for line in reader.lines().map_while(Result::ok) {
                // Parse: "HH:MM:SS [session_id] [AREA] [SEV] message"
                let parts: Vec<&str> = line.splitn(5, ' ').collect();
                if parts.len() >= 5 {
                    let timestamp = parts[0].to_string();
                    let area_str = parts[2].trim_matches(|c| c == '[' || c == ']');
                    let sev_str = parts[3].trim_matches(|c| c == '[' || c == ']').trim();
                    let message = parts[4].to_string();
                    let area = match area_str {
                        "SRC" => AuditArea::Source,
                        "BRK" => AuditArea::Broker,
                        "REC" => AuditArea::Record,
                        "PLY" => AuditArea::Playback,
                        "MIR" => AuditArea::Mirror,
                        _ => AuditArea::System,
                    };
                    let severity = match sev_str {
                        "WARN" => AuditSeverity::Warn,
                        "ERR" => AuditSeverity::Error,
                        _ => AuditSeverity::Info,
                    };
                    log.push(AuditEntry { timestamp, area, severity, message });
                }
            }
            // Trim to last 200 entries
            while log.len() > 200 {
                log.remove(0);
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

    /// Get line count for a file, cached for 2 seconds.
    pub fn get_file_line_count(&self, path: &str) -> Option<usize> {
        if let Ok(mut cache) = self.file_line_cache.lock() {
            if let Some((count, checked)) = cache.get(path) {
                if checked.elapsed() < Duration::from_secs(2) {
                    return Some(*count);
                }
            }
            // Count lines (subtract 1 for CSV header)
            if let Ok(file) = std::fs::File::open(path) {
                use std::io::BufRead;
                let count = std::io::BufReader::new(file).lines().count().saturating_sub(1);
                cache.insert(path.to_string(), (count, Instant::now()));
                return Some(count);
            }
        }
        None
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
        generate_default_filename(self.source_host.as_deref())
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
        let uptime = self.get_broker_uptime();
        let secs = uptime.as_secs();
        let uptime_str = if secs >= 3600 {
            format!("{}h{}m{}s", secs / 3600, (secs % 3600) / 60, secs % 60)
        } else if secs >= 60 {
            format!("{}m{}s", secs / 60, secs % 60)
        } else {
            format!("{}s", secs)
        };
        self.push_audit(AuditArea::System, AuditSeverity::Info, format!(
            "Session {} shutting down — uptime {}, received {}, mirrored {}, published {}, recorded {}, replayed {}",
            self.session_id,
            uptime_str,
            self.get_received_count(),
            self.get_mirrored_count(),
            self.get_published_count(),
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
                .title_bottom(Line::from(Span::styled(
                    format!(" {} ", state.session_id),
                    Style::default().fg(Color::DarkGray),
                )).alignment(Alignment::Right))
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
                let elapsed_str = source_elapsed.map(|d| format!(" (+{})", fmt_dur(d))).unwrap_or_default();
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
                Line::from(Span::styled("Connecting...", Style::default().fg(Color::Yellow)))
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
                .title(Line::from(vec![
                    Span::styled(broker_date_str, dim),
                    Span::styled(format!(" (+{}) ", fmt_dur(broker_uptime)), Style::default().fg(Color::Green)),
                ]).alignment(Alignment::Right))
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
                vec![Span::styled(format!(" +{} ", fmt_dur(d)), Style::default().fg(Color::Yellow))]
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
                let count_str = state.get_file_line_count(&f)
                    .map(|c| format!(" ({})", c)).unwrap_or_default();
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
                        let count_str = state.get_file_line_count(f)
                            .map(|c| format!(" ({})", c)).unwrap_or_default();
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
                .border_style(Style::default().fg(if playback_finished { Color::DarkGray } else { playback_color }))
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
                    title_spans.push(Span::styled(format!("{} ", path), Style::default().fg(Color::Green)));
                } else {
                    title_spans.push(Span::styled(format!("{} ", path), dim));
                }
            }
            let audit_color = if audit_on { Color::DarkGray } else { Color::Red };
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
                let audit_color = if audit_on { Color::Green } else { Color::DarkGray };
                let loop_ind = if loop_on { "●" } else { "○" };
                let src_ind = if source_on { "●" } else { "○" };
                let mir_ind = if mirror_on { "●" } else { "○" };
                let rec_ind = if record_on { "●" } else { "○" };
                let ply_ind = if playback_on { "●" } else { "○" };
                let aud_ind = if audit_on { "●" } else { "○" };
                Line::from(vec![
                    Span::styled(" [s]", Style::default().fg(source_color)),
                    Span::styled(format!(" Source {} ", src_ind), Style::default().fg(source_color)),
                    Span::styled("[m]", Style::default().fg(mirror_color)),
                    Span::styled(format!(" Mirror {} ", mir_ind), Style::default().fg(mirror_color)),
                    Span::styled("[r]", Style::default().fg(record_color)),
                    Span::styled(format!(" Record {} ", rec_ind), Style::default().fg(record_color)),
                    Span::styled("[p]", Style::default().fg(playback_color)),
                    Span::styled(format!(" Play {} ", ply_ind), Style::default().fg(playback_color)),
                    Span::styled("[l]", Style::default().fg(playback_color)),
                    Span::styled(format!(" Loop {} ", loop_ind), Style::default().fg(playback_color)),
                    Span::styled("[f]", Style::default().fg(Color::White)),
                    " New File  ".into(),
                    Span::styled("[a]", Style::default().fg(audit_color)),
                    Span::styled(format!(" Audit {} ", aud_ind), Style::default().fg(audit_color)),
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

/// Run the TUI event loop. Returns when quit is requested or shutdown signal received.
pub async fn run_tui(
    state: Arc<TuiState>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> std::io::Result<()> {
    let mut terminal = Terminal::new()?;

    // Log session start
    {
        let mut info = format!("Session {} started — broker localhost:{}", state.session_id, state.broker_port);
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
        terminal.draw(&state, input_mode, input_target_audit, &input_buffer, log_scroll)?;

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
                                                state.push_audit(AuditArea::System, AuditSeverity::Info,
                                                    format!("Audit log file switching to {}", input_buffer));
                                            }
                                        }
                                        state.set_audit_file_path(input_buffer.clone());
                                        state.enable_audit_file();
                                        let msg = match prev {
                                            Some(p) if p != input_buffer => format!("Audit log file set to {} (was {})", input_buffer, p),
                                            _ => format!("Audit log file set to {}", input_buffer),
                                        };
                                        state.push_audit(AuditArea::System, AuditSeverity::Info, msg);
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
                                    let dur_str = elapsed.map(|d| format!(" in {}s", d.as_secs())).unwrap_or_default();
                                    state.push_audit(AuditArea::Mirror, AuditSeverity::Info, format!(
                                        "Mirror OFF ({} mirrored, {} published this session{}, {} total)",
                                        m_session, p_session, dur_str, state.get_mirrored_count()
                                    ));
                                }
                                state.set_mirroring(enabling);
                                state.set_mirror_enabled_at(enabling);
                                if enabling {
                                    state.push_audit(AuditArea::Mirror, AuditSeverity::Info, format!(
                                        "Mirror ON (total: {} mirrored, {} published)",
                                        state.get_mirrored_count(), state.get_published_count()
                                    ));
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
                                    state.push_audit(AuditArea::Record, AuditSeverity::Info, format!(
                                        "Recording ON → {}", file
                                    ));
                                } else {
                                    let session = state.get_recording_session_count();
                                    let total = state.get_recorded_count();
                                    let dur = state.get_recording_duration();
                                    let dur_str = dur.map(|d| format!(" in {}s", d.as_secs())).unwrap_or_default();
                                    state.stop_recording_session();
                                    state.push_audit(AuditArea::Record, AuditSeverity::Info, format!(
                                        "Recording OFF ({} this session{}, {} total)", session, dur_str, total
                                    ));
                                }
                                state.set_recording(enabling);
                            }
                            KeyCode::Char('s') => {
                                let enabling = !state.is_source_enabled();
                                if !enabling {
                                    let elapsed = state.get_source_enabled_elapsed();
                                    let dur_str = elapsed.map(|d| format!(" in {}s", d.as_secs())).unwrap_or_default();
                                    state.push_audit(AuditArea::Source, AuditSeverity::Info, format!(
                                        "Source disabled (received: {}{}, connected: {})",
                                        state.get_received_count(), dur_str,
                                        if state.is_source_connected() { "yes" } else { "no" }
                                    ));
                                }
                                state.set_source_enabled(enabling);
                                if enabling {
                                    state.reset_source_enabled_at();
                                    state.push_audit(AuditArea::Source, AuditSeverity::Info, format!(
                                        "Source enabled (received: {})",
                                        state.get_received_count()
                                    ));
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
                                            state.push_audit(AuditArea::Playback, AuditSeverity::Info, format!(
                                                "Playback restarted → {}", file
                                            ));
                                        } else {
                                            // Fresh start
                                            if state.is_recording() {
                                                let session = state.get_recording_session_count();
                                                let total = state.get_recorded_count();
                                                let dur = state.get_recording_duration();
                                                let dur_str = dur.map(|d| format!(" in {}s", d.as_secs())).unwrap_or_default();
                                                state.stop_recording_session();
                                                state.push_audit(AuditArea::Record, AuditSeverity::Info, format!(
                                                    "Recording OFF ({} this session{}, {} total) — playback enabled", session, dur_str, total
                                                ));
                                            }
                                            state.set_recording(false);
                                            state.start_playback_session();
                                            let mode = if state.is_playback_looping() { "loop" } else { "once" };
                                            let file = state.get_active_file().unwrap_or_default();
                                            state.push_audit(AuditArea::Playback, AuditSeverity::Info, format!(
                                                "Playback ON ({}) → {}", mode, file
                                            ));
                                            state.loop_enabled.store(true, Ordering::Relaxed);
                                        }
                                    } else {
                                        // Stopping playback
                                        let session = state.get_playback_session_count();
                                        let total = state.get_replayed_count();
                                        let dur = state.get_playback_duration();
                                        let dur_str = dur.map(|d| format!(" in {}s", d.as_secs())).unwrap_or_default();
                                        state.stop_playback_session();
                                        state.push_audit(AuditArea::Playback, AuditSeverity::Info, format!(
                                            "Playback OFF ({} this session, {} total{})", session, total, dur_str
                                        ));
                                        state.loop_enabled.store(false, Ordering::Relaxed);
                                    }
                                }
                            }
                            KeyCode::Char('l') => {
                                // Toggle loop mode for playback
                                let was_looping = state.playback_looping.load(Ordering::Relaxed);
                                state.playback_looping.store(!was_looping, Ordering::Relaxed);
                                state.push_audit(AuditArea::Playback, AuditSeverity::Info, format!(
                                    "Playback mode: {}", if !was_looping { "loop" } else { "once" }
                                ));
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

    // === generate_default_filename free function tests ===

    #[test]
    fn test_generate_default_filename_with_host() {
        let filename = super::generate_default_filename(Some("broker.example.com"));
        assert!(filename.starts_with("broker-example-com-"));
        assert!(filename.ends_with(".csv"));
    }

    #[test]
    fn test_generate_default_filename_no_host() {
        let filename = super::generate_default_filename(None);
        assert!(filename.starts_with("recording-"));
        assert!(filename.ends_with(".csv"));
    }

    #[test]
    fn test_generate_default_filename_sanitizes_colons() {
        let filename = super::generate_default_filename(Some("host:8883"));
        assert!(filename.starts_with("host-8883-"));
    }

    #[test]
    fn test_generate_default_filename_matches_tui_state() {
        let state = TuiState::new(
            AppMode::Record, 1883, None,
            Some("test.host".to_string()), 1883, None, true, vec![], true,
        );
        let from_state = state.generate_default_filename();
        let from_fn = super::generate_default_filename(Some("test.host"));
        // Both should have same prefix (timestamp may differ by a ms)
        assert_eq!(&from_state[..10], &from_fn[..10]);
    }

    // === Playback mode tests ===

    #[test]
    fn test_playback_looping_default_false() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        assert!(!state.is_playback_looping());
    }

    #[test]
    fn test_playback_looping_toggle() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.playback_looping.store(true, Ordering::Relaxed);
        assert!(state.is_playback_looping());
        state.playback_looping.store(false, Ordering::Relaxed);
        assert!(!state.is_playback_looping());
    }

    #[test]
    fn test_playback_finished_default_false() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        assert!(!state.is_playback_finished());
    }

    #[test]
    fn test_playback_finished_cleared_on_start() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.playback_finished.store(true, Ordering::Relaxed);
        assert!(state.is_playback_finished());

        state.start_playback_session();
        assert!(!state.is_playback_finished());
    }

    // === Timing reset tests ===

    #[test]
    fn test_source_enabled_elapsed_starts_immediately() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        // Source starts enabled, so elapsed should be Some
        assert!(state.get_source_enabled_elapsed().is_some());
    }

    #[test]
    fn test_source_enabled_elapsed_none_when_disabled() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.set_source_enabled(false);
        assert!(state.get_source_enabled_elapsed().is_none());
    }

    #[test]
    fn test_source_enabled_elapsed_resets_on_reenable() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
        let before = state.get_source_enabled_elapsed().unwrap();

        state.set_source_enabled(false);
        state.reset_source_enabled_at();
        state.set_source_enabled(true);

        let after = state.get_source_enabled_elapsed().unwrap();
        assert!(after < before);
    }

    #[test]
    fn test_mirror_enabled_elapsed_some_when_enabled() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        // mirror starts enabled (initial_mirror=true)
        assert!(state.get_mirror_enabled_elapsed().is_some());
    }

    #[test]
    fn test_mirror_enabled_elapsed_none_when_disabled() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.set_mirroring(false);
        state.set_mirror_enabled_at(false);
        assert!(state.get_mirror_enabled_elapsed().is_none());
    }

    #[test]
    fn test_mirror_enabled_elapsed_resets_on_toggle() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
        let before = state.get_mirror_enabled_elapsed().unwrap();

        state.set_mirror_enabled_at(false);
        state.set_mirroring(false);
        state.set_mirror_enabled_at(true);
        state.set_mirroring(true);

        let after = state.get_mirror_enabled_elapsed().unwrap();
        assert!(after < before);
    }

    // === first_connected_at tests ===

    #[test]
    fn test_first_connected_at_none_initially() {
        let state = TuiState::new(
            AppMode::Record, 1883, None,
            Some("host".to_string()), 1883, None, true, vec![], true,
        );
        assert!(state.get_first_connected_since().is_none());
    }

    #[test]
    fn test_first_connected_at_set_on_connect() {
        let state = TuiState::new(
            AppMode::Record, 1883, None,
            Some("host".to_string()), 1883, None, true, vec![], true,
        );
        state.set_source_connected(true);
        assert!(state.get_first_connected_since().is_some());
    }

    #[test]
    fn test_first_connected_at_not_reset_on_reconnect() {
        let state = TuiState::new(
            AppMode::Record, 1883, None,
            Some("host".to_string()), 1883, None, true, vec![], true,
        );
        state.set_source_connected(true);
        let first = state.get_first_connected_since().unwrap();

        state.set_source_connected(false);
        std::thread::sleep(std::time::Duration::from_millis(50));
        state.set_source_connected(true);

        let after_reconnect = state.get_first_connected_since().unwrap();
        // first_connected_since should be the same (or very close) — not reset
        let diff = (first - after_reconnect).num_milliseconds().abs();
        assert!(diff < 10, "first_connected_at should not reset on reconnect, diff={}ms", diff);
    }

    // === Broker connection audit tests ===

    #[test]
    fn test_broker_connections_audited_on_change() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.set_broker_connections(1);
        state.set_broker_connections(2);

        let log = state.get_audit_log();
        let conn_entries: Vec<_> = log.iter()
            .filter(|e| matches!(e.area, AuditArea::Broker) && e.message.contains("Connections"))
            .collect();
        assert_eq!(conn_entries.len(), 2);
        assert!(conn_entries[0].message.contains("0 → 1"));
        assert!(conn_entries[1].message.contains("1 → 2"));
    }

    #[test]
    fn test_broker_connections_not_audited_when_unchanged() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.set_broker_connections(3);
        state.set_broker_connections(3); // Same value

        let log = state.get_audit_log();
        let conn_entries: Vec<_> = log.iter()
            .filter(|e| matches!(e.area, AuditArea::Broker) && e.message.contains("Connections"))
            .collect();
        assert_eq!(conn_entries.len(), 1); // Only one audit entry
    }

    // === Recording session count tests ===

    #[test]
    fn test_recording_session_count_starts_at_zero() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.start_recording_session();
        assert_eq!(state.get_recording_session_count(), 0);
    }

    #[test]
    fn test_recording_session_count_tracks_delta() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        // Simulate some pre-existing records
        for _ in 0..10 {
            state.increment_recorded();
        }
        state.start_recording_session(); // snapshot at 10
        for _ in 0..5 {
            state.increment_recorded();
        }
        assert_eq!(state.get_recording_session_count(), 5);
        assert_eq!(state.get_recorded_count(), 15);
    }

    // === Playback session count tests ===

    #[test]
    fn test_playback_session_count_starts_at_zero() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.start_playback_session();
        assert_eq!(state.get_playback_session_count(), 0);
    }

    #[test]
    fn test_playback_session_count_tracks_delta() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        for _ in 0..20 {
            state.increment_replayed();
        }
        state.start_playback_session(); // snapshot at 20
        for _ in 0..7 {
            state.increment_replayed();
        }
        assert_eq!(state.get_playback_session_count(), 7);
        assert_eq!(state.get_replayed_count(), 27);
    }

    #[test]
    fn test_playback_session_count_resets_on_new_session() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.start_playback_session();
        for _ in 0..10 {
            state.increment_replayed();
        }
        assert_eq!(state.get_playback_session_count(), 10);

        // Start new session
        state.start_playback_session(); // snapshot at 10
        for _ in 0..3 {
            state.increment_replayed();
        }
        assert_eq!(state.get_playback_session_count(), 3);
        assert_eq!(state.get_replayed_count(), 13);
    }

    // === Mirror session count tests ===

    #[test]
    fn test_mirror_session_counts_start_at_zero() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        let (m, p) = state.get_mirror_session_counts();
        assert_eq!(m, 0);
        assert_eq!(p, 0);
    }

    #[test]
    fn test_mirror_session_counts_track_delta() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        for _ in 0..10 {
            state.increment_mirrored();
            state.increment_published();
        }
        // Re-enable mirror to snapshot at (10, 10)
        state.set_mirror_enabled_at(true);
        for _ in 0..5 {
            state.increment_mirrored();
            state.increment_published();
        }
        let (m, p) = state.get_mirror_session_counts();
        assert_eq!(m, 5);
        assert_eq!(p, 5);
    }

    #[test]
    fn test_mirror_session_counts_published_includes_playback() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.set_mirror_enabled_at(true); // snapshot at (0, 0)
        for _ in 0..5 {
            state.increment_mirrored();
            state.increment_published();
        }
        // Playback also increments published
        for _ in 0..3 {
            state.increment_published();
        }
        let (m, p) = state.get_mirror_session_counts();
        assert_eq!(m, 5);
        assert_eq!(p, 8); // 5 mirror + 3 playback
    }

    // === Shutdown summary test ===

    #[test]
    fn test_request_quit_audit_includes_uptime() {
        let state = TuiState::new(
            AppMode::Record, 1883, None, None, 1883, None, true, vec![], true,
        );
        state.request_quit();
        let log = state.get_audit_log();
        let shutdown = log.iter().find(|e| e.message.contains("shutting down")).unwrap();
        assert!(shutdown.message.contains("uptime"));
        assert!(shutdown.message.contains("published"));
    }
}
