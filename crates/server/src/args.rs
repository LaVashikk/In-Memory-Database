use std::path::PathBuf;
use clap::{Parser, ValueEnum};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Mode { // todo: a bit outdated, actually
    // write() to page cache, no fsync. survives kill -9, not power loss
    NoFsync,
    // fdatasync per batch, reply AFTER commit
    Sync,
    // reply immediately from mem, fsync in bg
    Async,
}

/// In-memory database Proof-of-Concept
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// todo
    #[arg(short, long, default_value_t = 9000)]
    pub port: u16,

    /// todo
    #[arg(short, long, default_value = "no-fsync")]
    pub mode: Mode,

    /// todo
    #[arg(short, long, default_value = "./pocdata")]
    pub dir: PathBuf,
}
