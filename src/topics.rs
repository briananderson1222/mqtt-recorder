//! Topic filter module
//!
//! Handles topic subscription configuration from CLI args or JSON files.
//! This module provides the [`TopicFilter`] struct for managing MQTT topic subscriptions.
//!
//! # Topic Sources
//!
//! Topics can be configured from three sources:
//! - A single topic via CLI argument (`-t` or `--topic`)
//! - A JSON file containing multiple topics (`--topics`)
//! - Default wildcard subscription (`#`) when no topics are specified
//!
//! # MQTT Wildcards
//!
//! The module preserves MQTT wildcard patterns:
//! - `+` matches a single level (e.g., `sensors/+/temperature`)
//! - `#` matches multiple levels (e.g., `sensors/#`)
//!
//! # Example
//!
//! ```rust,ignore
//! use mqtt_recorder::topics::TopicFilter;
//! use std::path::Path;
//!
//! // Single topic
//! let filter = TopicFilter::from_single("sensors/temperature".to_string());
//! assert_eq!(filter.topics(), &["sensors/temperature"]);
//!
//! // Wildcard subscription
//! let filter = TopicFilter::wildcard();
//! assert_eq!(filter.topics(), &["#"]);
//!
//! // From JSON file
//! let filter = TopicFilter::from_json_file(Path::new("topics.json"))?;
//! ```

use crate::error::MqttRecorderError;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

/// Internal structure for deserializing the topics JSON file.
///
/// Expected JSON format:
/// ```json
/// {
///     "topics": ["topic1", "topic2", "sensors/+/temperature"]
/// }
/// ```
#[derive(Debug, serde::Deserialize)]
struct TopicsFile {
    topics: Vec<String>,
}

/// A filter containing MQTT topics to subscribe to.
///
/// This struct manages a list of topic patterns for MQTT subscriptions.
/// It supports single topics, multiple topics from a JSON file, and
/// wildcard subscriptions.
///
/// # Examples
///
/// ```rust,ignore
/// use mqtt_recorder::topics::TopicFilter;
///
/// // Create from a single topic
/// let filter = TopicFilter::from_single("home/temperature".to_string());
/// assert!(!filter.is_empty());
/// assert_eq!(filter.topics().len(), 1);
///
/// // Create wildcard subscription
/// let filter = TopicFilter::wildcard();
/// assert_eq!(filter.topics(), &["#"]);
/// ```
#[derive(Debug, Clone)]
pub struct TopicFilter {
    topics: Vec<String>,
}

impl TopicFilter {
    /// Create a TopicFilter from a single topic string.
    ///
    /// This is used when the user provides a single topic via the `-t` or `--topic`
    /// CLI argument.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic pattern to subscribe to. Can include MQTT wildcards.
    ///
    /// # Returns
    ///
    /// A new `TopicFilter` containing exactly the provided topic.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::topics::TopicFilter;
    ///
    /// let filter = TopicFilter::from_single("sensors/+/temperature".to_string());
    /// assert_eq!(filter.topics(), &["sensors/+/temperature"]);
    /// ```
    pub fn from_single(topic: String) -> Self {
        Self {
            topics: vec![topic],
        }
    }

    /// Create a TopicFilter from a JSON file.
    ///
    /// The JSON file must have the following structure:
    /// ```json
    /// {
    ///     "topics": ["topic1", "topic2", ...]
    /// }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the JSON file containing topic definitions.
    ///
    /// # Returns
    ///
    /// A `Result` containing either:
    /// - `Ok(TopicFilter)` with the topics from the file
    /// - `Err(MqttRecorderError)` if the file cannot be read or parsed
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be opened (IO error)
    /// - The file contains invalid JSON (JSON parsing error)
    /// - The JSON structure doesn't match the expected format
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::topics::TopicFilter;
    /// use std::path::Path;
    ///
    /// let filter = TopicFilter::from_json_file(Path::new("topics.json"))?;
    /// for topic in filter.topics() {
    ///     println!("Subscribing to: {}", topic);
    /// }
    /// ```
    pub fn from_json_file(path: &Path) -> Result<Self, MqttRecorderError> {
        // Open the file - this will return an IO error if the file doesn't exist
        // or cannot be read (Requirements 3.5)
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        // Parse the JSON - this will return a JSON error if the content is invalid
        // or doesn't match the expected structure (Requirements 3.6, 7.2, 7.3)
        let topics_file: TopicsFile = serde_json::from_reader(reader)?;

        // Return the filter with the parsed topics (Requirements 7.1, 7.4)
        // Note: Empty topic arrays are supported per requirement 7.4
        Ok(Self {
            topics: topics_file.topics,
        })
    }

    /// Create a wildcard TopicFilter that subscribes to all topics.
    ///
    /// This creates a filter with the MQTT multi-level wildcard `#`,
    /// which matches all topics on the broker. This is the default
    /// behavior when no topic arguments are provided.
    ///
    /// # Returns
    ///
    /// A new `TopicFilter` containing only the `#` wildcard topic.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::topics::TopicFilter;
    ///
    /// let filter = TopicFilter::wildcard();
    /// assert_eq!(filter.topics(), &["#"]);
    /// ```
    pub fn wildcard() -> Self {
        Self {
            topics: vec!["#".to_string()],
        }
    }

    /// Get the list of topics in this filter.
    ///
    /// Returns a slice containing all topic patterns that should be
    /// subscribed to. The topics may include MQTT wildcards (`+` and `#`).
    ///
    /// # Returns
    ///
    /// A slice of topic strings.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::topics::TopicFilter;
    ///
    /// let filter = TopicFilter::from_single("sensors/temperature".to_string());
    /// for topic in filter.topics() {
    ///     println!("Topic: {}", topic);
    /// }
    /// ```
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Check if the filter contains no topics.
    ///
    /// This can occur when loading from a JSON file with an empty topics array.
    /// An empty filter means no subscriptions will be made.
    ///
    /// # Returns
    ///
    /// `true` if the filter contains no topics, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::topics::TopicFilter;
    ///
    /// let filter = TopicFilter::wildcard();
    /// assert!(!filter.is_empty());
    /// ```
    #[allow(dead_code)] // Public API for library users
    pub fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_from_single_creates_filter_with_one_topic() {
        let filter = TopicFilter::from_single("test/topic".to_string());
        assert_eq!(filter.topics(), &["test/topic"]);
        assert!(!filter.is_empty());
    }

    #[test]
    fn test_from_single_preserves_wildcards() {
        // Test single-level wildcard
        let filter = TopicFilter::from_single("sensors/+/temperature".to_string());
        assert_eq!(filter.topics(), &["sensors/+/temperature"]);

        // Test multi-level wildcard
        let filter = TopicFilter::from_single("sensors/#".to_string());
        assert_eq!(filter.topics(), &["sensors/#"]);
    }

    #[test]
    fn test_wildcard_creates_hash_subscription() {
        let filter = TopicFilter::wildcard();
        assert_eq!(filter.topics(), &["#"]);
        assert!(!filter.is_empty());
    }

    #[test]
    fn test_from_json_file_parses_valid_json() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"{{"topics": ["topic1", "topic2", "sensors/+/temp"]}}"#
        )
        .unwrap();

        let filter = TopicFilter::from_json_file(file.path()).unwrap();
        assert_eq!(filter.topics(), &["topic1", "topic2", "sensors/+/temp"]);
    }

    #[test]
    fn test_from_json_file_supports_empty_array() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"{{"topics": []}}"#).unwrap();

        let filter = TopicFilter::from_json_file(file.path()).unwrap();
        assert!(filter.is_empty());
        assert_eq!(filter.topics().len(), 0);
    }

    #[test]
    fn test_from_json_file_preserves_wildcards() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"{{"topics": ["sensors/+/temp", "actuators/#"]}}"#).unwrap();

        let filter = TopicFilter::from_json_file(file.path()).unwrap();
        assert_eq!(filter.topics(), &["sensors/+/temp", "actuators/#"]);
    }

    #[test]
    fn test_from_json_file_error_on_missing_file() {
        let result = TopicFilter::from_json_file(Path::new("/nonexistent/path/topics.json"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, MqttRecorderError::Io(_)));
    }

    #[test]
    fn test_from_json_file_error_on_invalid_json() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"{{ invalid json }}"#).unwrap();

        let result = TopicFilter::from_json_file(file.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, MqttRecorderError::Json(_)));
    }

    #[test]
    fn test_from_json_file_error_on_wrong_structure() {
        let mut file = NamedTempFile::new().unwrap();
        // Missing "topics" field
        writeln!(file, r#"{{"wrong_field": ["topic1"]}}"#).unwrap();

        let result = TopicFilter::from_json_file(file.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, MqttRecorderError::Json(_)));
    }

    #[test]
    fn test_from_json_file_error_on_non_string_topics() {
        let mut file = NamedTempFile::new().unwrap();
        // Topics array contains non-string values
        writeln!(file, r#"{{"topics": [123, true]}}"#).unwrap();

        let result = TopicFilter::from_json_file(file.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, MqttRecorderError::Json(_)));
    }

    #[test]
    fn test_is_empty_returns_false_for_non_empty_filter() {
        let filter = TopicFilter::from_single("topic".to_string());
        assert!(!filter.is_empty());

        let filter = TopicFilter::wildcard();
        assert!(!filter.is_empty());
    }

    #[test]
    fn test_topics_getter_returns_all_topics() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"{{"topics": ["a", "b", "c"]}}"#).unwrap();

        let filter = TopicFilter::from_json_file(file.path()).unwrap();
        let topics = filter.topics();
        assert_eq!(topics.len(), 3);
        assert_eq!(topics[0], "a");
        assert_eq!(topics[1], "b");
        assert_eq!(topics[2], "c");
    }

    #[test]
    fn test_topic_filter_is_clone() {
        let filter = TopicFilter::from_single("test".to_string());
        let cloned = filter.clone();
        assert_eq!(filter.topics(), cloned.topics());
    }

    #[test]
    fn test_topic_filter_is_debug() {
        let filter = TopicFilter::from_single("test".to_string());
        let debug_str = format!("{:?}", filter);
        assert!(debug_str.contains("TopicFilter"));
        assert!(debug_str.contains("test"));
    }
}
