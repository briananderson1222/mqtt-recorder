//! MQTT Recorder Library
//!
//! This library provides the core functionality for the MQTT Recorder CLI tool.
//! It includes modules for CLI argument parsing, MQTT client operations,
//! CSV handling, topic filtering, and embedded broker management.

pub mod broker;
pub mod cli;
pub mod csv_handler;
pub mod error;
pub mod fixer;
pub mod mirror;
pub mod mqtt;
pub mod recorder;
pub mod replayer;
pub mod topics;
pub mod tui;
pub mod validator;
