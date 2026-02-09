//! MQTT Recorder Library
//!
//! This library provides the core functionality for the MQTT Recorder CLI tool.
//! It includes modules for CLI argument parsing, MQTT client operations,
//! CSV handling, topic filtering, and embedded broker management.

pub mod broker;
pub mod cli;
pub mod error;
pub mod mqtt;
