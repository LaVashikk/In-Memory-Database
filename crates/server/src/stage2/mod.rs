use std::path::PathBuf;
use std::sync::mpsc as sync_mpsc;
use raw_shared_types::{Batch, Db, Request};
use tokio::sync::mpsc as async_mpsc;

use super::{COMPACT_EVERY_LSN, Ctrl, MAX_BATCH};
use crate::args::Mode;

// single sync db thread
#[allow(clippy::too_many_arguments)]
pub fn run_main_loop(
    mut item_rx: async_mpsc::Receiver<Request>,
    free_rx: sync_mpsc::Receiver<Batch>,
    s23_tx: sync_mpsc::Sender<Batch>,
    _ctrl_tx: sync_mpsc::Sender<Ctrl>,
    mut db: Db,
    start_lsn: u64,
    mode: Mode,
    _dir: PathBuf,
) {
    let mut lsn = start_lsn;
    let last_compact_lsn = start_lsn.saturating_sub(1);
    loop {
        let mut batch = match free_rx.recv() {
            Ok(b) => b,
            Err(_) => break,
        };
        let n = item_rx.blocking_recv_many(&mut batch.items, MAX_BATCH);
        if n == 0 {
            break;
        }
        db.apply(&mut batch, &mut lsn);

        // async mode: reply immediately from mem before wal commit
        if !mode.reply_in_stage3() {
            for req in batch.items.iter_mut() {
                if let Some(r) = req.resp.take() {
                    let _ = req.reply.blocking_send(r);
                }
            }
        }
        if s23_tx.send(batch).is_err() {
            break;
        }

        // periodic snapshot trigger + signal compaction
        // TODO: blocking stage2 thread for snap write kills p99 latencies, need cow fork
        if lsn.saturating_sub(last_compact_lsn) >= COMPACT_EVERY_LSN {
            // -------------
            // let durable_lsn = lsn - 1;
            // if persist::write_snapshot(&db, &dir, durable_lsn).is_ok() {
            //     let _ = ctrl_tx.send(Ctrl::Compact(durable_lsn));
            //     last_compact_lsn = lsn;
            //     eprintln!("stage2: snap done @lsn={durable_lsn} keys={}", db.len());
            // }
        }
    }
}
