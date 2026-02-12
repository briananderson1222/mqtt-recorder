//! Interactive TUI module for mqtt-recorder

pub mod input;
pub mod render;
pub mod state;
pub mod types;

pub use input::run_tui;
pub use state::{TuiConfig, TuiState};
pub use types::{generate_default_filename, should_enable_interactive, AuditArea, AuditSeverity};

// Library-only re-exports (used by external consumers, not the binary)
#[allow(unused_imports)]
pub use types::{AppMode, AuditEntry};
