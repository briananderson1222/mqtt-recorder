//! Property-based tests for topic filtering
//!
//! This module contains property tests that validate the TopicFilter behavior
//! as specified in the design document.
//!
//! **Validates: Requirements 3.2-3.4, 7.1-7.2**

use proptest::prelude::*;
use std::io::Write;
use tempfile::NamedTempFile;

// Import the topics module from the main crate
use mqtt_recorder::topics::TopicFilter;

/// Strategy for generating valid non-empty topic strings
/// Topics can contain alphanumeric characters, slashes, underscores, and hyphens
fn valid_topic_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Simple single-level topics
        "[a-zA-Z][a-zA-Z0-9_-]{0,20}".prop_map(|s| s),
        // Multi-level topics with slashes
        "[a-zA-Z][a-zA-Z0-9]{0,10}/[a-zA-Z][a-zA-Z0-9]{0,10}".prop_map(|s| s),
        // Three-level topics
        "[a-zA-Z][a-zA-Z0-9]{0,5}/[a-zA-Z][a-zA-Z0-9]{0,5}/[a-zA-Z][a-zA-Z0-9]{0,5}"
            .prop_map(|s| s),
        // Topics with underscores and hyphens
        "[a-zA-Z][a-zA-Z0-9_-]{0,10}/[a-zA-Z][a-zA-Z0-9_-]{0,10}".prop_map(|s| s),
    ]
}

/// Strategy for generating topic strings with single-level wildcard (+)
fn topic_with_single_wildcard_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Wildcard at end
        "[a-zA-Z][a-zA-Z0-9]{0,10}/\\+".prop_map(|s| s),
        // Wildcard in middle
        "[a-zA-Z][a-zA-Z0-9]{0,5}/\\+/[a-zA-Z][a-zA-Z0-9]{0,5}".prop_map(|s| s),
        // Multiple wildcards
        "[a-zA-Z][a-zA-Z0-9]{0,5}/\\+/\\+".prop_map(|s| s),
        // Wildcard at start
        "\\+/[a-zA-Z][a-zA-Z0-9]{0,10}".prop_map(|s| s),
    ]
}

/// Strategy for generating topic strings with multi-level wildcard (#)
fn topic_with_multi_wildcard_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Just the wildcard
        Just("#".to_string()),
        // Wildcard at end (only valid position for #)
        "[a-zA-Z][a-zA-Z0-9]{0,10}/#".prop_map(|s| s),
        // Multi-level path with wildcard at end
        "[a-zA-Z][a-zA-Z0-9]{0,5}/[a-zA-Z][a-zA-Z0-9]{0,5}/#".prop_map(|s| s),
    ]
}

/// Strategy for generating topic strings with any MQTT wildcard
fn topic_with_any_wildcard_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        topic_with_single_wildcard_strategy(),
        topic_with_multi_wildcard_strategy(),
    ]
}

/// Strategy for generating a vector of valid topic strings
fn valid_topics_vec_strategy() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(valid_topic_strategy(), 0..10)
}

/// Strategy for generating a vector of topics that may include wildcards
fn topics_with_wildcards_vec_strategy() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(
        prop_oneof![
            valid_topic_strategy(),
            topic_with_single_wildcard_strategy(),
            topic_with_multi_wildcard_strategy(),
        ],
        1..10,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: mqtt-recorder, Property 4: Single Topic Filter Construction
    // *For any* non-empty topic string, creating a TopicFilter from that single topic
    // SHALL produce a filter containing exactly that topic.
    // **Validates: Requirements 3.2**
    #[test]
    fn property_4_single_topic_filter_construction(
        topic in valid_topic_strategy()
    ) {
        let filter = TopicFilter::from_single(topic.clone());

        // The filter should contain exactly one topic
        prop_assert_eq!(filter.topics().len(), 1, "Filter should contain exactly one topic");

        // The topic should be exactly the one provided
        prop_assert_eq!(&filter.topics()[0], &topic, "Topic should match the provided value");

        // The filter should not be empty
        prop_assert!(!filter.is_empty(), "Filter should not be empty");
    }

    // Feature: mqtt-recorder, Property 4: Single Topic Filter Construction
    // Additional test: Single topic with wildcards
    // **Validates: Requirements 3.2, 3.4**
    #[test]
    fn property_4_single_topic_filter_with_wildcards(
        topic in topic_with_any_wildcard_strategy()
    ) {
        let filter = TopicFilter::from_single(topic.clone());

        // The filter should contain exactly one topic
        prop_assert_eq!(filter.topics().len(), 1, "Filter should contain exactly one topic");

        // The topic should be exactly the one provided (including wildcards)
        prop_assert_eq!(&filter.topics()[0], &topic, "Topic with wildcards should match exactly");

        // The filter should not be empty
        prop_assert!(!filter.is_empty(), "Filter should not be empty");
    }

    // Feature: mqtt-recorder, Property 5: JSON Topics File Parsing
    // *For any* valid JSON object with a "topics" field containing an array of strings,
    // parsing that JSON SHALL produce a TopicFilter containing exactly those topics
    // in the same order.
    // **Validates: Requirements 3.3, 7.1, 7.2**
    #[test]
    fn property_5_json_topics_file_parsing(
        topics in valid_topics_vec_strategy()
    ) {
        // Create a temporary JSON file with the topics
        let mut file = NamedTempFile::new().expect("Failed to create temp file");

        // Build JSON content
        let topics_json: Vec<String> = topics.iter().map(|t| format!("\"{}\"", t)).collect();
        let json_content = format!("{{\"topics\": [{}]}}", topics_json.join(", "));

        writeln!(file, "{}", json_content).expect("Failed to write to temp file");

        // Parse the JSON file
        let filter = TopicFilter::from_json_file(file.path())
            .expect("Should successfully parse valid JSON");

        // The filter should contain exactly the same number of topics
        prop_assert_eq!(
            filter.topics().len(),
            topics.len(),
            "Filter should contain the same number of topics as the JSON file"
        );

        // The topics should be in the same order
        for (i, expected_topic) in topics.iter().enumerate() {
            prop_assert_eq!(
                &filter.topics()[i],
                expected_topic,
                "Topic at index {} should match", i
            );
        }

        // Empty check should match
        prop_assert_eq!(
            filter.is_empty(),
            topics.is_empty(),
            "is_empty() should match whether topics vec is empty"
        );
    }

    // Feature: mqtt-recorder, Property 5: JSON Topics File Parsing
    // Additional test: JSON with topics containing wildcards
    // **Validates: Requirements 3.3, 3.4, 7.1, 7.2**
    #[test]
    fn property_5_json_topics_file_parsing_with_wildcards(
        topics in topics_with_wildcards_vec_strategy()
    ) {
        // Create a temporary JSON file with the topics
        let mut file = NamedTempFile::new().expect("Failed to create temp file");

        // Build JSON content - escape any special characters for JSON
        let topics_json: Vec<String> = topics.iter().map(|t| format!("\"{}\"", t)).collect();
        let json_content = format!("{{\"topics\": [{}]}}", topics_json.join(", "));

        writeln!(file, "{}", json_content).expect("Failed to write to temp file");

        // Parse the JSON file
        let filter = TopicFilter::from_json_file(file.path())
            .expect("Should successfully parse valid JSON with wildcards");

        // The filter should contain exactly the same number of topics
        prop_assert_eq!(
            filter.topics().len(),
            topics.len(),
            "Filter should contain the same number of topics"
        );

        // The topics should be in the same order and preserve wildcards
        for (i, expected_topic) in topics.iter().enumerate() {
            prop_assert_eq!(
                &filter.topics()[i],
                expected_topic,
                "Topic at index {} should match exactly (including wildcards)", i
            );
        }
    }

    // Feature: mqtt-recorder, Property 6: Wildcard Topic Preservation
    // *For any* topic string containing MQTT wildcards (`+` or `#`), the TopicFilter
    // SHALL preserve those wildcards exactly as provided.
    // **Validates: Requirements 3.4**
    #[test]
    fn property_6_wildcard_topic_preservation_single_level(
        topic in topic_with_single_wildcard_strategy()
    ) {
        // Test with from_single
        let filter = TopicFilter::from_single(topic.clone());

        // The wildcard should be preserved exactly
        prop_assert_eq!(
            &filter.topics()[0],
            &topic,
            "Single-level wildcard (+) should be preserved exactly"
        );

        // Verify the + character is present
        prop_assert!(
            filter.topics()[0].contains('+'),
            "The + wildcard character should be present in the topic"
        );
    }

    // Feature: mqtt-recorder, Property 6: Wildcard Topic Preservation
    // Test multi-level wildcard (#) preservation
    // **Validates: Requirements 3.4**
    #[test]
    fn property_6_wildcard_topic_preservation_multi_level(
        topic in topic_with_multi_wildcard_strategy()
    ) {
        // Test with from_single
        let filter = TopicFilter::from_single(topic.clone());

        // The wildcard should be preserved exactly
        prop_assert_eq!(
            &filter.topics()[0],
            &topic,
            "Multi-level wildcard (#) should be preserved exactly"
        );

        // Verify the # character is present
        prop_assert!(
            filter.topics()[0].contains('#'),
            "The # wildcard character should be present in the topic"
        );
    }

    // Feature: mqtt-recorder, Property 6: Wildcard Topic Preservation
    // Test wildcard preservation through JSON file parsing
    // **Validates: Requirements 3.4**
    #[test]
    fn property_6_wildcard_preservation_through_json(
        topic in topic_with_any_wildcard_strategy()
    ) {
        // Create a temporary JSON file with the wildcard topic
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        let json_content = format!("{{\"topics\": [\"{}\"]}}", topic);
        writeln!(file, "{}", json_content).expect("Failed to write to temp file");

        // Parse the JSON file
        let filter = TopicFilter::from_json_file(file.path())
            .expect("Should successfully parse JSON with wildcard topic");

        // The wildcard should be preserved exactly
        prop_assert_eq!(
            &filter.topics()[0],
            &topic,
            "Wildcard should be preserved exactly through JSON parsing"
        );

        // Verify wildcard characters are present
        let has_wildcard = filter.topics()[0].contains('+') || filter.topics()[0].contains('#');
        prop_assert!(
            has_wildcard,
            "Wildcard character (+ or #) should be present in the parsed topic"
        );
    }

    // Feature: mqtt-recorder, Property 6: Wildcard Topic Preservation
    // Test that the default wildcard() method produces exactly "#"
    // **Validates: Requirements 3.1, 3.4**
    #[test]
    fn property_6_default_wildcard_is_hash(
        _dummy in Just(())
    ) {
        let filter = TopicFilter::wildcard();

        // Should contain exactly one topic
        prop_assert_eq!(filter.topics().len(), 1, "Wildcard filter should have exactly one topic");

        // Should be exactly "#"
        prop_assert_eq!(&filter.topics()[0], "#", "Default wildcard should be '#'");

        // Should not be empty
        prop_assert!(!filter.is_empty(), "Wildcard filter should not be empty");
    }
}
