//! TUI state management

use crate::tui::types::{generate_default_filename, AppMode, AuditArea, AuditEntry, AuditSeverity};
use std::{
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

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
    // Verify counters
    pub verify_matched: AtomicU64,
    pub verify_mismatched: AtomicU64,
    pub verify_missing: AtomicU64,
    // Broker metrics (fed from poll_metrics)
    broker_subscriptions: AtomicUsize,
    broker_total_publishes: AtomicU64,
    broker_failed_publishes: AtomicU64,
    // Health check timer
    last_health_check: std::sync::Mutex<Instant>,
    health_check_interval: u64,
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
        health_check_interval: u64,
    ) -> Self {
        // Default recording to ON if file path provided, unless explicitly set
        let recording = initial_record.unwrap_or(file_path.is_some());
        let session_id = format!(
            "{:08x}",
            std::process::id() as u64
                ^ std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos() as u64
        );
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
            mirror_enabled_at: std::sync::Mutex::new(if initial_mirror {
                Some(Instant::now())
            } else {
                None
            }),
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
            verify_matched: AtomicU64::new(0),
            verify_mismatched: AtomicU64::new(0),
            verify_missing: AtomicU64::new(0),
            broker_subscriptions: AtomicUsize::new(0),
            broker_total_publishes: AtomicU64::new(0),
            broker_failed_publishes: AtomicU64::new(0),
            last_health_check: std::sync::Mutex::new(Instant::now()),
            health_check_interval,
        }
    }

    // === Connection timing ===
    pub fn set_source_connected(&self, connected: bool) {
        let was_connected = self.source_connected.swap(connected, Ordering::Relaxed);
        if connected && !was_connected {
            let is_reconnect = self.source_ever_connected.swap(true, Ordering::Relaxed);
            let label = if is_reconnect {
                "Re-connected"
            } else {
                "Connected"
            };
            let severity = if is_reconnect {
                AuditSeverity::Warn
            } else {
                AuditSeverity::Info
            };
            self.push_audit(
                AuditArea::Source,
                severity,
                format!(
                    "{} to {}:{}",
                    label,
                    self.source_host.as_deref().unwrap_or("?"),
                    self.source_port
                ),
            );
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
            let uptime = self
                .connected_at
                .lock()
                .ok()
                .and_then(|g| g.map(|t| t.elapsed()))
                .map(|d| {
                    let secs = d.as_secs();
                    if secs >= 3600 {
                        format!("{}h{}m{}s", secs / 3600, (secs % 3600) / 60, secs % 60)
                    } else if secs >= 60 {
                        format!("{}m{}s", secs / 60, secs % 60)
                    } else {
                        format!("{}s", secs)
                    }
                })
                .unwrap_or_else(|| "?".into());
            self.push_audit(
                AuditArea::Source,
                AuditSeverity::Warn,
                format!(
                    "Disconnected (received {} msgs, uptime {})",
                    self.received_count.load(Ordering::Relaxed),
                    uptime
                ),
            );
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
        self.get_connection_duration()
            .map(|d| chrono::Local::now() - chrono::Duration::from_std(d).unwrap_or_default())
    }

    /// Get the time of first-ever connection (never resets)
    pub fn get_first_connected_since(&self) -> Option<chrono::DateTime<chrono::Local>> {
        self.first_connected_at
            .lock()
            .ok()
            .and_then(|g| g.map(|t| t.elapsed()))
            .map(|d| chrono::Local::now() - chrono::Duration::from_std(d).unwrap_or_default())
    }

    /// Get elapsed since source was last enabled (resets on toggle)
    pub fn get_source_enabled_elapsed(&self) -> Option<Duration> {
        if self.source_enabled.load(Ordering::Relaxed) {
            self.source_enabled_at
                .lock()
                .ok()
                .and_then(|g| g.map(|t| t.elapsed()))
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
            self.mirror_enabled_at
                .lock()
                .ok()
                .and_then(|g| g.map(|t| t.elapsed()))
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
                *guard = (
                    self.mirrored_count.load(Ordering::Relaxed),
                    self.published_count.load(Ordering::Relaxed),
                );
            }
        }
    }

    /// Get mirror session counts: (mirrored_this_session, published_this_session)
    pub fn get_mirror_session_counts(&self) -> (u64, u64) {
        let m_total = self.mirrored_count.load(Ordering::Relaxed);
        let p_total = self.published_count.load(Ordering::Relaxed);
        let (m_start, p_start) = self
            .mirror_start_count
            .lock()
            .ok()
            .map(|g| *g)
            .unwrap_or((0, 0));
        (
            m_total.saturating_sub(m_start),
            p_total.saturating_sub(p_start),
        )
    }

    // === Broker timing ===
    pub fn get_broker_uptime(&self) -> Duration {
        self.broker_started_at.elapsed()
    }

    pub fn get_broker_started_at(&self) -> chrono::DateTime<chrono::Local> {
        chrono::Local::now()
            - chrono::Duration::from_std(self.get_broker_uptime()).unwrap_or_default()
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
        let start = self
            .recording_start_count
            .lock()
            .ok()
            .map(|g| *g)
            .unwrap_or(0);
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
        let start = self
            .playback_start_count
            .lock()
            .ok()
            .map(|g| *g)
            .unwrap_or(0);
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
        let flat = format!(
            "{} [{}] [{}] [{}] {}",
            ts,
            self.session_id,
            area.label(),
            sev,
            message
        );
        let entry = AuditEntry {
            timestamp: ts,
            area,
            severity,
            message,
        };
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
                    log.push(AuditEntry {
                        timestamp,
                        area,
                        severity,
                        message,
                    });
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
            self.push_audit(
                AuditArea::System,
                AuditSeverity::Info,
                "Auditing disabled".into(),
            );
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
            self.push_audit(
                AuditArea::System,
                AuditSeverity::Info,
                "Auditing enabled".into(),
            );
            true
        }
    }

    pub fn get_audit_log(&self) -> Vec<AuditEntry> {
        self.audit_log
            .lock()
            .ok()
            .map(|g| g.clone())
            .unwrap_or_default()
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
                let count = std::io::BufReader::new(file)
                    .lines()
                    .count()
                    .saturating_sub(1);
                cache.insert(path.to_string(), (count, Instant::now()));
                return Some(count);
            }
        }
        None
    }

    pub fn set_broker_connections(&self, count: usize) {
        let old = self.broker_connections.swap(count, Ordering::Relaxed);
        if old != count {
            self.push_audit(
                AuditArea::Broker,
                AuditSeverity::Info,
                format!("Connections: {} → {}", old, count),
            );
        }
    }

    pub fn get_broker_connections(&self) -> usize {
        self.broker_connections.load(Ordering::Relaxed)
    }

    /// Update broker metrics from a BrokerMetrics snapshot.
    /// Automatically emits a health check audit entry every 60 seconds.
    pub fn update_broker_metrics(&self, metrics: &crate::broker::BrokerMetrics) {
        if let Some((old, new)) = metrics.connections_changed {
            self.set_broker_connections(new);
            let _ = old; // already logged by set_broker_connections
        }
        self.broker_subscriptions
            .store(metrics.subscriptions, Ordering::Relaxed);
        self.broker_total_publishes
            .store(metrics.total_publishes, Ordering::Relaxed);
        self.broker_failed_publishes
            .store(metrics.failed_publishes, Ordering::Relaxed);
        self.maybe_emit_health_check();
    }

    /// Emit a health check audit entry if the configured interval has elapsed.
    /// Disabled when health_check_interval is 0.
    fn maybe_emit_health_check(&self) {
        if self.health_check_interval == 0 {
            return;
        }
        let interval = Duration::from_secs(self.health_check_interval);
        let should_emit = self
            .last_health_check
            .lock()
            .ok()
            .map(|g| g.elapsed() >= interval)
            .unwrap_or(false);
        if !should_emit {
            return;
        }
        if let Ok(mut guard) = self.last_health_check.lock() {
            *guard = Instant::now();
        }
        let uptime = self.get_broker_uptime();
        let secs = uptime.as_secs();
        let uptime_str = if secs >= 3600 {
            format!("{}h{}m{}s", secs / 3600, (secs % 3600) / 60, secs % 60)
        } else if secs >= 60 {
            format!("{}m{}s", secs / 60, secs % 60)
        } else {
            format!("{}s", secs)
        };
        let failed = self.broker_failed_publishes.load(Ordering::Relaxed);
        let severity = if failed > 0 {
            AuditSeverity::Warn
        } else {
            AuditSeverity::Info
        };
        self.push_audit(AuditArea::System, severity, format!(
            "Health: uptime {}, conns {}, subs {}, broker_pub {}, failed {}, rx {}, mir {}, pub {}, rec {}, rep {}",
            uptime_str,
            self.get_broker_connections(),
            self.broker_subscriptions.load(Ordering::Relaxed),
            self.broker_total_publishes.load(Ordering::Relaxed),
            failed,
            self.get_received_count(),
            self.get_mirrored_count(),
            self.get_published_count(),
            self.get_recorded_count(),
            self.get_replayed_count(),
        ));
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

    pub fn increment_verify_matched(&self) {
        self.verify_matched.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_verify_mismatched(&self) {
        self.verify_mismatched.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_verify_missing(&self, count: u64) {
        self.verify_missing.fetch_add(count, Ordering::Relaxed);
    }

    pub fn get_verify_matched(&self) -> u64 {
        self.verify_matched.load(Ordering::Relaxed)
    }

    pub fn get_verify_mismatched(&self) -> u64 {
        self.verify_mismatched.load(Ordering::Relaxed)
    }

    pub fn get_verify_missing(&self) -> u64 {
        self.verify_missing.load(Ordering::Relaxed)
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

#[cfg(test)]
mod tests {
    use super::*;

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
            60,
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
            60,
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
            60,
        );

        assert!(state.is_recording());
        state.set_recording(false);
        assert!(!state.is_recording());
        state.set_recording(true);
        assert!(state.is_recording());
    }

    #[test]
    fn test_tui_state_mirroring_toggle() {
        let state = TuiState::new(
            AppMode::Mirror,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );

        assert!(state.is_mirroring());
        state.set_mirroring(false);
        assert!(!state.is_mirroring());
        state.set_mirroring(true);
        assert!(state.is_mirroring());
    }

    #[test]
    fn test_tui_state_source_toggle() {
        let state = TuiState::new(
            AppMode::Mirror,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );

        assert!(state.is_source_enabled());
        state.set_source_enabled(false);
        assert!(!state.is_source_enabled());
        state.set_source_enabled(true);
        assert!(state.is_source_enabled());
    }

    #[test]
    fn test_tui_state_counters() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );

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
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        assert!(!state.is_quit_requested());
        state.request_quit();
        assert!(state.is_quit_requested());
    }

    #[test]
    fn test_tui_state_loop_enabled() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        assert!(!state.loop_enabled.load(Ordering::Relaxed));

        state.loop_enabled.store(true, Ordering::Relaxed);
        assert!(state.loop_enabled.load(Ordering::Relaxed));
    }

    #[test]
    fn test_tui_state_none_file_path() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
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
            60,
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
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        assert!(state.get_file_path().is_none());

        state.set_new_file("first_file.csv".to_string());

        assert_eq!(state.get_file_path(), Some("first_file.csv".to_string()));
    }

    #[test]
    fn test_tui_state_set_new_file_appends_csv() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );

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
            60,
        );
        let filename = state.generate_default_filename();

        assert!(filename.starts_with("broker-example-com-"));
        assert!(filename.ends_with(".csv"));
    }

    #[test]
    fn test_tui_state_generate_default_filename_no_host() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
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
            60,
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
            60,
        );
        assert_eq!(state.get_selected_index(), 0);
        state.navigate_selection(1);
        assert_eq!(state.get_selected_index(), 1);
        state.navigate_selection(1);
        assert_eq!(state.get_selected_index(), 0); // Wrapped

        state.navigate_selection(-1);
        assert_eq!(state.get_selected_index(), 1); // Wrapped backwards
    }

    // === Playback mode tests ===

    #[test]
    fn test_playback_looping_default_false() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        assert!(!state.is_playback_looping());
    }

    #[test]
    fn test_playback_looping_toggle() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.playback_looping.store(true, Ordering::Relaxed);
        assert!(state.is_playback_looping());
        state.playback_looping.store(false, Ordering::Relaxed);
        assert!(!state.is_playback_looping());
    }

    #[test]
    fn test_playback_finished_default_false() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        assert!(!state.is_playback_finished());
    }

    #[test]
    fn test_playback_finished_cleared_on_start() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
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
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        // Source starts enabled, so elapsed should be Some
        assert!(state.get_source_enabled_elapsed().is_some());
    }

    #[test]
    fn test_source_enabled_elapsed_none_when_disabled() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.set_source_enabled(false);
        assert!(state.get_source_enabled_elapsed().is_none());
    }

    #[test]
    fn test_source_enabled_elapsed_resets_on_reenable() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
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
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        // mirror starts enabled (initial_mirror=true)
        assert!(state.get_mirror_enabled_elapsed().is_some());
    }

    #[test]
    fn test_mirror_enabled_elapsed_none_when_disabled() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.set_mirroring(false);
        state.set_mirror_enabled_at(false);
        assert!(state.get_mirror_enabled_elapsed().is_none());
    }

    #[test]
    fn test_mirror_enabled_elapsed_resets_on_toggle() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
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
            AppMode::Record,
            1883,
            None,
            Some("host".to_string()),
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        assert!(state.get_first_connected_since().is_none());
    }

    #[test]
    fn test_first_connected_at_set_on_connect() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            Some("host".to_string()),
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.set_source_connected(true);
        assert!(state.get_first_connected_since().is_some());
    }

    #[test]
    fn test_first_connected_at_not_reset_on_reconnect() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            Some("host".to_string()),
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.set_source_connected(true);
        let first = state.get_first_connected_since().unwrap();

        state.set_source_connected(false);
        std::thread::sleep(std::time::Duration::from_millis(50));
        state.set_source_connected(true);

        let after_reconnect = state.get_first_connected_since().unwrap();
        // first_connected_since should be the same (or very close) — not reset
        let diff = (first - after_reconnect).num_milliseconds().abs();
        assert!(
            diff < 10,
            "first_connected_at should not reset on reconnect, diff={}ms",
            diff
        );
    }

    // === Broker connection audit tests ===

    #[test]
    fn test_broker_connections_audited_on_change() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.set_broker_connections(1);
        state.set_broker_connections(2);

        let log = state.get_audit_log();
        let conn_entries: Vec<_> = log
            .iter()
            .filter(|e| matches!(e.area, AuditArea::Broker) && e.message.contains("Connections"))
            .collect();
        assert_eq!(conn_entries.len(), 2);
        assert!(conn_entries[0].message.contains("0 → 1"));
        assert!(conn_entries[1].message.contains("1 → 2"));
    }

    #[test]
    fn test_broker_connections_not_audited_when_unchanged() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.set_broker_connections(3);
        state.set_broker_connections(3); // Same value

        let log = state.get_audit_log();
        let conn_entries: Vec<_> = log
            .iter()
            .filter(|e| matches!(e.area, AuditArea::Broker) && e.message.contains("Connections"))
            .collect();
        assert_eq!(conn_entries.len(), 1); // Only one audit entry
    }

    // === Recording session count tests ===

    #[test]
    fn test_recording_session_count_starts_at_zero() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.start_recording_session();
        assert_eq!(state.get_recording_session_count(), 0);
    }

    #[test]
    fn test_recording_session_count_tracks_delta() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
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
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.start_playback_session();
        assert_eq!(state.get_playback_session_count(), 0);
    }

    #[test]
    fn test_playback_session_count_tracks_delta() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
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
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
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
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        let (m, p) = state.get_mirror_session_counts();
        assert_eq!(m, 0);
        assert_eq!(p, 0);
    }

    #[test]
    fn test_mirror_session_counts_track_delta() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
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
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
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
            AppMode::Record,
            1883,
            None,
            None,
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        state.request_quit();
        let log = state.get_audit_log();
        let shutdown = log
            .iter()
            .find(|e| e.message.contains("shutting down"))
            .unwrap();
        assert!(shutdown.message.contains("uptime"));
        assert!(shutdown.message.contains("published"));
    }

    #[test]
    fn test_generate_default_filename_matches_tui_state() {
        let state = TuiState::new(
            AppMode::Record,
            1883,
            None,
            Some("test.host".to_string()),
            1883,
            None,
            true,
            vec![],
            true,
            60,
        );
        let from_state = state.generate_default_filename();
        let from_fn = generate_default_filename(Some("test.host"));
        // Both should have same prefix (timestamp may differ by a ms)
        assert_eq!(&from_state[..10], &from_fn[..10]);
    }
}
