// wip server binary: pipeline + crash recovery

use std::sync::mpsc as sync_mpsc;
use std::thread;

use anyhow::Context;
use clap::Parser;
use io_uring::IoUring;
use raw_shared_types::{Batch, Request, persist};
use tokio::net::TcpListener;
use tokio::sync::mpsc as async_mpsc;

#[cfg(feature = "allocator_debug")]
mod alloc_stat;
#[cfg(feature = "allocator_debug")]
#[global_allocator]
static GLOBAL: alloc_stat::Counting = alloc_stat::Counting;

// trigger snapshot & wal compaction every N puts
pub const SNAPSHOT_EVERY_LSN: u64 = 2_000_000;
pub const SNAPSHOT_MIN_LSN_GAP: u64 = 500_000;
// rotate wal file after this many bytes
const SEG_BYTES: u64 = 256 * 1024 * 1024;

// configurate it?
pub const RING_DEPTH: usize = 128;
pub const MAX_BATCH: usize = 1024;
pub const N_BUFFERS: usize = 32;
pub const OUT_CAP: usize = 256 * 1024;
pub const REQ_BOUND: usize = 65_536;
pub const RESP_BOUND: usize = 8_192;

mod args;
use args::*;
pub mod acker;

mod stage1; // todo: make better naming
mod stage2;
mod stage3;

pub enum WalMsg {
    Write(Batch),
    /// Start a fresh segment named `boundary_lsn`.
    Rotate { boundary_lsn: u64 },
    /// Unlink every segment whose start_lsn < `boundary_lsn`.
    /// The active segment starts at >= boundary_lsn by construction.
    Retire { boundary_lsn: u64 },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // recover state before opening listening port
    let (db, start_lsn) = persist::recover(&args.dir).context("cannot recover DB state!")?;
    let acker = acker::Acker::new(args.mode);

    // dbg!(&db);

    // pipeline channels
    // TODO: make better naming here
    let (item_tx, item_rx) = async_mpsc::channel::<Request>(REQ_BOUND);
    let (s23_tx, s23_rx) = sync_mpsc::channel::<WalMsg>();
    let (pool_tx, pool_rx) = sync_mpsc::channel::<Batch>();

    // spawning buffers-pool
    for _ in 0..N_BUFFERS {
        pool_tx
            .send(Batch::with_capacity(MAX_BATCH, OUT_CAP))
            .unwrap();
    }

    // stage 1 - networking
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            // .worker_threads(val) // todo: hm.. how much? `ALL - 2`?
            // .worker_threads(std::thread::available_parallelism().unwrap().get().saturating_sub(2).max(1))
            .build()
            .context("failed to create async runtime")?;
        thread::Builder::new()
            .name("stage1".into())
            .spawn(move || {
                rt.block_on(stage1::start_server(args.port, item_tx))
            })
            .context("failed to spawn stage-1")?;
    }

    // stage 3 - async-io WAL handler
    {
        // TODO: remake it!!!!!
        let dir = args.dir.clone();
        let wal_engine = stage3::WalEngine::new(RING_DEPTH, dir, start_lsn, args.mode, s23_rx, pool_tx, acker)?;
        thread::Builder::new()
            .name("stage3".into())
            .spawn(move || wal_engine.start())
            .context("failed to spawn stage-3")?;
    }

    eprintln!("server starting: port={} mode={:?} dir={:?} start_lsn={}", args.port, args.mode, args.dir, start_lsn);

    // main thread is the 'stage 2' - main DB-worker!
    stage2::run_main_loop(item_rx, pool_rx, s23_tx, db, acker, start_lsn, args.mode, args.dir);

    unreachable!()
}
