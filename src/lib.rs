#![feature(wrapping_int_impl)]

extern crate core;

pub mod common;
pub mod executor;
pub mod scheduler;
pub mod sequencer;
pub mod stmt_analyzer;

pub mod calvinite_tonic {
    tonic::include_proto!("calvinite"); // The string specified here must match the proto package name
}
