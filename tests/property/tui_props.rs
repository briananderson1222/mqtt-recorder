//! Property-based tests for TUI module
//!
//! This module contains property tests that validate the TUI state management
//! and interactive mode detection behavior.
//!
//! **Feature: tui**

use mqtt_recorder::tui::{should_enable_interactive, AppMode, TuiConfig, TuiState};
use proptest::prelude::*;
use std::sync::atomic::Ordering;

// =============================================================================
// Property 1: should_enable_interactive respects flags and environment
// =============================================================================

#[test]
fn property_1_no_interactive_flag_disables_tui() {
    // When --no-interactive is set, TUI should be disabled regardless of TTY
    assert!(!should_enable_interactive(true));
}

#[test]
fn property_1_ci_env_disables_tui() {
    // When CI=true is set, TUI should be disabled
    std::env::set_var("CI", "true");
    let result = should_enable_interactive(false);
    std::env::remove_var("CI");
    assert!(!result);
}

#[test]
fn property_1_ci_env_any_value_disables_tui() {
    // Any value for CI env var should disable TUI
    std::env::set_var("CI", "1");
    let result = should_enable_interactive(false);
    std::env::remove_var("CI");
    assert!(!result);
}

// =============================================================================
// Property 2: AppMode enum conversions
// =============================================================================

#[test]
fn property_2_app_mode_display() {
    assert_eq!(format!("{}", AppMode::Record), "record");
    assert_eq!(format!("{}", AppMode::Replay), "replay");
    assert_eq!(format!("{}", AppMode::Mirror), "mirror");
    assert_eq!(format!("{}", AppMode::Passthrough), "passthrough");
}

#[test]
fn property_2_app_mode_values() {
    // Verify enum discriminant values
    assert_eq!(AppMode::Record as u8, 0);
    assert_eq!(AppMode::Replay as u8, 1);
    assert_eq!(AppMode::Mirror as u8, 2);
    assert_eq!(AppMode::Passthrough as u8, 3);
}

// =============================================================================
// Property 3: TuiState atomic operations
// =============================================================================

proptest! {
    #[test]
    fn property_3_tui_state_received_count_increments(count in 0u64..1000) {
        let state = TuiState::new(TuiConfig {
            broker_port: 1883,
            file_path: None,
            source_host: None,
            source_port: 1883,
            initial_record: None,
            initial_mirror: true,
            playlist: vec![],
            audit_enabled: true,
            health_check_interval: 60,
        });
        for _ in 0..count {
            state.increment_received();
        }
        prop_assert_eq!(state.get_received_count(), count);
    }

    #[test]
    fn property_3_tui_state_preserves_broker_port(port in 1024u16..65535) {
        let state = TuiState::new(TuiConfig {
            broker_port: port,
            file_path: None,
            source_host: None,
            source_port: 1883,
            initial_record: None,
            initial_mirror: true,
            playlist: vec![],
            audit_enabled: true,
            health_check_interval: 60,
        });
        prop_assert_eq!(state.broker_port, port);
    }

    #[test]
    fn property_3_tui_state_preserves_file_path(path in "[a-z]{1,20}\\.csv") {
        let state = TuiState::new(TuiConfig {
            broker_port: 1883,
            file_path: Some(path.clone()),
            source_host: None,
            source_port: 1883,
            initial_record: None,
            initial_mirror: true,
            playlist: vec![],
            audit_enabled: true,
            health_check_interval: 60,
        });
        prop_assert_eq!(state.get_file_path(), Some(path));
    }
}

#[test]
fn property_3_tui_state_recording_toggle() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: Some("test.csv".to_string()),
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });

    // Initially true (file path provided)
    assert!(state.is_recording());

    // Toggle off
    state.set_recording(false);
    assert!(!state.is_recording());

    // Toggle on
    state.set_recording(true);
    assert!(state.is_recording());
}

#[test]
fn property_3_tui_state_mirroring_toggle() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });

    // Initially true
    assert!(state.is_mirroring());

    // Toggle off
    state.set_mirroring(false);
    assert!(!state.is_mirroring());

    // Toggle on
    state.set_mirroring(true);
    assert!(state.is_mirroring());
}

#[test]
fn property_3_tui_state_source_toggle() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });

    // Initially true
    assert!(state.is_source_enabled());

    // Toggle off
    state.set_source_enabled(false);
    assert!(!state.is_source_enabled());

    // Toggle on
    state.set_source_enabled(true);
    assert!(state.is_source_enabled());
}

#[test]
fn property_3_tui_state_loop_toggle() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });

    // Initially false
    assert!(!state.loop_enabled.load(Ordering::Relaxed));

    // Toggle on
    state.loop_enabled.store(true, Ordering::Relaxed);
    assert!(state.loop_enabled.load(Ordering::Relaxed));

    // Toggle off
    state.loop_enabled.store(false, Ordering::Relaxed);
    assert!(!state.loop_enabled.load(Ordering::Relaxed));
}

// =============================================================================
// Property 4: TuiState initialization
// =============================================================================

#[test]
fn property_4_tui_state_initial_counts_zero() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });
    assert_eq!(state.get_received_count(), 0);
    assert_eq!(state.get_mirrored_count(), 0);
    assert_eq!(state.get_published_count(), 0);
    assert_eq!(state.get_recorded_count(), 0);
}

#[test]
fn property_4_tui_state_initial_loop_disabled() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });
    assert!(!state.loop_enabled.load(Ordering::Relaxed));
}

#[test]
fn property_4_tui_state_none_file_path() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });
    assert!(state.get_file_path().is_none());
}

#[test]
fn property_4_tui_state_set_new_file_updates_both_paths() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: Some("original.csv".to_string()),
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });

    state.set_new_file("swapped.csv".to_string());

    // Display path updates immediately
    assert_eq!(state.get_file_path(), Some("swapped.csv".to_string()));
    // New file path available for consumer
    assert_eq!(state.take_new_file(), Some("swapped.csv".to_string()));
    // Consumed after take
    assert_eq!(state.take_new_file(), None);
    // Display path still shows new value
    assert_eq!(state.get_file_path(), Some("swapped.csv".to_string()));
}

#[test]
fn property_4_tui_state_quit_requested_initially_false() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });
    assert!(!state.is_quit_requested());
}

#[test]
fn property_4_tui_state_request_quit() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });
    state.request_quit();
    assert!(state.is_quit_requested());
}

// =============================================================================
// Property 5: AppMode equality and cloning
// =============================================================================

#[test]
fn property_5_app_mode_equality() {
    assert_eq!(AppMode::Record, AppMode::Record);
    assert_eq!(AppMode::Replay, AppMode::Replay);
    assert_ne!(AppMode::Record, AppMode::Replay);
}

#[test]
fn property_5_app_mode_copy() {
    let mode = AppMode::Record;
    let copied = mode;
    assert_eq!(mode, copied);
}

// =============================================================================
// Property 6: TuiState broker_connections
// =============================================================================

#[test]
fn property_6_broker_connections_initializes_to_zero() {
    let state = TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    });
    assert_eq!(state.get_broker_connections(), 0);
}

proptest! {
    #[test]
    fn property_6_broker_connections_set_get(n in 0usize..10000) {
        let state = TuiState::new(TuiConfig {
            broker_port: 1883,
            file_path: None,
            source_host: None,
            source_port: 1883,
            initial_record: None,
            initial_mirror: true,
            playlist: vec![],
            audit_enabled: true,
            health_check_interval: 60,
        });
        state.set_broker_connections(n);
        prop_assert_eq!(state.get_broker_connections(), n);
    }
}

#[test]
fn property_7_broker_connections_atomic() {
    use std::sync::Arc;
    use std::thread;
    let state = Arc::new(TuiState::new(TuiConfig {
        broker_port: 1883,
        file_path: None,
        source_host: None,
        source_port: 1883,
        initial_record: None,
        initial_mirror: true,
        playlist: vec![],
        audit_enabled: true,
        health_check_interval: 60,
    }));
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let s = state.clone();
            thread::spawn(move || {
                for j in 0..100 {
                    s.set_broker_connections(i * 100 + j);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert!(state.get_broker_connections() < 1000);
}
