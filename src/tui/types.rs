//! TUI types and utility functions

use std::io::IsTerminal;

/// Generate a default CSV filename from an optional host and current timestamp.
pub fn generate_default_filename(host: Option<&str>) -> String {
    let safe_host: String = host
        .unwrap_or("recording")
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
    Verify,
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
            AuditArea::Verify => "VFY",
        }
    }

    pub fn color(&self) -> ratatui::prelude::Color {
        use ratatui::prelude::Color;
        match self {
            AuditArea::Source => Color::Cyan,
            AuditArea::Broker => Color::Green,
            AuditArea::Record => Color::Green,
            AuditArea::Playback => Color::Magenta,
            AuditArea::Mirror => Color::Yellow,
            AuditArea::System => Color::White,
            AuditArea::Verify => Color::LightCyan,
        }
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
    std::io::stdout().is_terminal()
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
    fn test_should_enable_interactive_no_interactive_flag() {
        assert!(!should_enable_interactive(true));
    }

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
}
