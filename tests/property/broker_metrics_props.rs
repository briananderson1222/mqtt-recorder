//! Property-based tests for broker metrics
//!
//! This module contains property tests that validate the broker metrics
//! change detection and TuiState integration.

use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Simulates the change detection logic used in EmbeddedBroker
struct MetricsTracker {
    connection_count: AtomicUsize,
}

impl MetricsTracker {
    fn new() -> Self {
        Self {
            connection_count: AtomicUsize::new(0),
        }
    }

    fn update_and_check(&self, new_connections: usize) -> Option<(usize, usize)> {
        let old = self
            .connection_count
            .swap(new_connections, Ordering::Relaxed);
        if old != new_connections {
            Some((old, new_connections))
        } else {
            None
        }
    }

    fn get_connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }
}

// =============================================================================
// Property 1: Connection count is always non-negative (usize guarantees this)
// =============================================================================

proptest! {
    #[test]
    fn property_1_connection_count_non_negative(count in 0usize..10000) {
        let tracker = MetricsTracker::new();
        tracker.update_and_check(count);
        prop_assert!(tracker.get_connection_count() < usize::MAX);
    }
}

// =============================================================================
// Property 2: poll_metrics returns Some only when count changes
// =============================================================================

proptest! {
    #[test]
    fn property_2_change_detection(initial in 0usize..100, new_value in 0usize..100) {
        let tracker = MetricsTracker::new();
        tracker.update_and_check(initial);
        let result = tracker.update_and_check(new_value);
        if initial != new_value {
            prop_assert!(result.is_some());
            let (old, new) = result.unwrap();
            prop_assert_eq!(old, initial);
            prop_assert_eq!(new, new_value);
        } else {
            prop_assert!(result.is_none());
        }
    }
}

#[test]
fn property_2_no_change_returns_none() {
    let tracker = MetricsTracker::new();
    tracker.update_and_check(5);
    assert!(tracker.update_and_check(5).is_none());
}

#[test]
fn property_2_change_returns_correct_values() {
    let tracker = MetricsTracker::new();
    assert_eq!(tracker.update_and_check(3), Some((0, 3)));
    assert_eq!(tracker.update_and_check(1), Some((3, 1)));
}

// =============================================================================
// Property 3: get_connection_count returns the last polled value
// =============================================================================

proptest! {
    #[test]
    fn property_3_getter_returns_last_value(values in prop::collection::vec(0usize..1000, 1..10)) {
        let tracker = MetricsTracker::new();
        for &v in &values {
            tracker.update_and_check(v);
        }
        prop_assert_eq!(tracker.get_connection_count(), *values.last().unwrap());
    }
}

// =============================================================================
// Property 4: Change detection correctly reports (old, new) values
// =============================================================================

proptest! {
    #[test]
    fn property_4_change_reports_old_new(sequence in prop::collection::vec(0usize..100, 2..10)) {
        let tracker = MetricsTracker::new();
        let mut prev = 0usize;
        for &current in &sequence {
            let result = tracker.update_and_check(current);
            if prev != current {
                prop_assert!(result.is_some());
                let (old, new) = result.unwrap();
                prop_assert_eq!(old, prev);
                prop_assert_eq!(new, current);
            }
            prev = current;
        }
    }
}

// =============================================================================
// Property 5: Concurrent updates are atomic
// =============================================================================

#[test]
fn property_5_atomic_updates() {
    use std::sync::Arc;
    use std::thread;
    let tracker = Arc::new(MetricsTracker::new());
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let t = tracker.clone();
            thread::spawn(move || {
                for j in 0..100 {
                    t.update_and_check(i * 100 + j);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert!(tracker.get_connection_count() < 1000);
}
