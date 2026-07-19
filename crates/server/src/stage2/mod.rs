use std::path::PathBuf;
use std::sync::mpsc as sync_mpsc;
use raw_shared_types::{Batch, Db, Request, persist, snapshot::Snapshotter};
use tokio::sync::mpsc as async_mpsc;

use super::{MAX_BATCH, SNAPSHOT_MIN_LSN_GAP};
use crate::{SNAPSHOT_EVERY_LSN, WalMsg, acker::{AckPoint, Acker}, args::Mode};

// single sync db thread
#[allow(clippy::too_many_arguments)]
pub fn run_main_loop(
    mut item_rx: async_mpsc::Receiver<Request>,
    free_rx: sync_mpsc::Receiver<Batch>,
    s23_tx: sync_mpsc::Sender<WalMsg>,
    mut db: Db,
    ack: Acker,
    start_lsn: u64,
    mode: Mode,
    dir: PathBuf,
) {
    let mut snapshotter = Snapshotter::new(start_lsn, SNAPSHOT_MIN_LSN_GAP);
    let mut lsn = start_lsn;
    let mut last_compact_lsn = start_lsn.saturating_sub(1);

    let mut recycled_batch: Option<Batch> = None;

    loop {
        let Some(mut batch) = recycled_batch.take().or_else(|| free_rx.recv().ok()) else {
            break;
        };

        let n = item_rx.blocking_recv_many(&mut batch.items, MAX_BATCH);
        if n == 0 {
            break;
        }
        db.apply(&mut batch, &mut lsn);

        // The batch is applied to the in-memory state: every request now holds
        // its response, so the `Applied` guarantee level is reached
        ack.advance(&mut batch, AckPoint::Applied);

        if batch.has_wal_work() {
            s23_tx.send( WalMsg::Write(batch) ).expect("broken stage 2->3 channel");
        } else {
            batch.recycle();
            recycled_batch = Some(batch);
            continue; // PROCESS NEXT BATCH
        }

        // periodic snapshot trigger + signal compaction
        // TODO: blocking stage2 thread for snap write kills p99 latencies, need cow fork
        if lsn.saturating_sub(last_compact_lsn) >= SNAPSHOT_EVERY_LSN {
            let durable_lsn = lsn - 1;
            // let res = snapshotter.try_begin(&db, durable_lsn);

            // Todo
            if let Ok(res) = snapshotter.write_snapshot_sync_TEMP_FUNC(&db, &dir, durable_lsn) && res {
                let _ = s23_tx.send(WalMsg::Rotate { boundary_lsn: durable_lsn });
                let _ = s23_tx.send(WalMsg::Retire { boundary_lsn: durable_lsn-1 });
                last_compact_lsn = lsn;
                eprintln!("stage2: snap done @lsn={durable_lsn} keys={}", db.len());
            }
        }
    }
}
