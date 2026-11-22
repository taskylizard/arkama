//! Command-line entry points for Arkama.
//!
//! Both binaries use the same commands, download core, and SQLite store. This crate owns
//! terminal output, process signals, and daemon startup; embedded applications use
//! `arkama_core` directly.

mod args;
mod cli;
mod commands;
mod daemon;
mod util;

pub use cli::{run_cli, run_combined};
