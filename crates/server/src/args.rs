use std::path::PathBuf;
use clap::{Parser, ValueEnum};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Mode {
    // write() to page cache, no fsync. survives kill -9, not power loss
    NoFsync,
    // fdatasync per batch, reply AFTER commit
    Sync,
    // reply immediately from mem, fsync in bg
    Async,
}

impl Mode { // todo: no, make one aggregator for that staff
    // do we actually need fdatasync
    pub fn do_fsync(self) -> bool {
        matches!(self, Mode::Sync | Mode::Async)
    }
    // does stage 3 reply to client
    pub fn reply_in_stage3(self) -> bool {
        matches!(self, Mode::Sync | Mode::NoFsync)
    }
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
