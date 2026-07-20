use std::collections::VecDeque;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::mpsc::{self as sync_mpsc, TryRecvError};
use io_uring::IoUring ;
use slab::Slab;
use storage::wal_format;
use wire::Operation;

use crate::acker::{AckPoint, Acker};
use crate::WalMsg;
use crate::args::Mode;
use crate::types::Batch;
use segment::Segment;
use io_work::*;

mod segment;
mod io_work;
mod fsync_planner;

type LSN = u64;

const DEFAULT_SPIN_BUDGET: i32 = 500; // todo: move to main prob?

/// WAL async I/O loop state. Data flow:
/// msg_rx -> encode -> pending -> ring/inflight -> awaiting_fsync -> recycle_tx
pub struct WalEngine {
    // --- non-changing data ---
    dir: PathBuf,
    depth: usize,

    // --- ring machinery ---
    ring: IoUring,
    /// Encoded but not yet submitted (FIFO)
    pending: VecDeque<IoWork>,
    /// Ops the kernel currently owns, keyed by the CQE's `user_data`
    inflight: Slab<IoWork>,
    /// Spins left before the loop parks on `msg_rx`
    spin_budget: i32,

    // --- stage2 boundary ---
    msg_rx: sync_mpsc::Receiver<WalMsg>,
    /// Buffers go back to the pool once nobody owes their clients anything
    recycle_tx: sync_mpsc::Sender<Batch>,

    // --- TODO how to name? ---
   	/// Watermark: last LSN encoded into the WAL
    last_ingested_lsn: LSN,
    /// Current opened wal-file
    seg: Segment,
    /// Manager of client responses
    ack: Acker,
    fsync_planner: fsync_planner::FsyncPlanner,
    /// Batches that reached `Written` but still owe someone `Durable`
    awaiting_fsync: Vec<Batch>,
}

impl WalEngine {
    pub fn new(
        ring_depth: usize,
        dir: PathBuf,
        start_lsn: LSN,
        mode: Mode,
        batch_rx: sync_mpsc::Receiver<WalMsg>,
        recycle_tx: sync_mpsc::Sender<Batch>,
        ack: Acker,
    ) -> anyhow::Result<Self> {
        let ring = IoUring::new(ring_depth as u32)?;
        let seg = Segment::open(&dir, start_lsn)?;

        Ok(
            Self {
                dir,
                depth: ring_depth,

                ring,
                // todo: actually, there is no 'ring_depth', we can have a lot of work in queue, but prob it's ok
                pending: VecDeque::with_capacity(ring_depth),
                inflight: Slab::with_capacity(ring_depth),
                spin_budget: DEFAULT_SPIN_BUDGET,

                msg_rx: batch_rx,
                recycle_tx,

                last_ingested_lsn: start_lsn,
                seg,
                ack,
                fsync_planner: fsync_planner::FsyncPlanner::new(mode.into()),
                awaiting_fsync: Vec::with_capacity(crate::N_BUFFERS),
            }
        )
    }

    pub fn out_of_spins(&self) -> bool {
        self.spin_budget <= 0
    }
    pub fn should_park(&self) -> bool {
        self.pending.is_empty() && self.inflight.is_empty()
    }
    pub fn ring_is_full(&self) -> bool {
        self.inflight.len() >= self.depth
    }


    /// Spin-loop strategy with a budget. If we receive enough new requests, we don't park, but continuously
    /// add work. However, if there are very few requests, spinning makes no sense, so we park and wait for data from the ring-cq.
    pub fn start(mut self) {
        loop {
            if let Some(msg) = self.poll_channel_or_park() {
                match msg {
                    WalMsg::Write(batch) => self.handle_batch(batch),
                    WalMsg::Rotate { boundary_lsn } => self.rotate(boundary_lsn),
                    WalMsg::Retire { boundary_lsn } => self.retire(boundary_lsn),
                }
                // update spin-budget
                self.spin_budget = DEFAULT_SPIN_BUDGET;
            }

            if self.fsync_planner.should_fsync() {
                self.pending.push_back(
                    IoWork::Fsync(self.last_ingested_lsn)
                );
                self.seg.mark_fsynced();
            }

            let is_need_submit = self.handle_pending();
            self.reap_cqes();

            // Parking on IO if there out-of-sping!
            if (self.out_of_spins() && !self.inflight.is_empty()) || self.ring_is_full() {
                self.submit_ring_and_park(1);
            }
            else if is_need_submit {
                // Actually, I can remove this ONE syscall, and just create a sq-watcher on the side of the kernel
                if let Err(e) = self.ring.submit() {
                    panic!("io_uring submit failed: {}", e);
                }
            }
        }
    }


    fn poll_channel_or_park(&mut self) -> Option<WalMsg> {
        if self.should_park() {
            Some(self.msg_rx.recv().expect("stage 2->3 channel closed"))
        } else {
            match self.msg_rx.try_recv() {
                Ok(buf) => Some(buf),
                Err(TryRecvError::Empty) => {
                    self.spin_budget -= 1;
                    None
                },
                Err(_) => panic!("stage 2->3 channel closed"),
            }
        }
    }

    fn handle_batch(&mut self, mut batch: Batch) {
        // The stream must be gapless: this batch continues exactly where the last one ended
        assert_eq!(batch.lsn_low, self.last_ingested_lsn, "WAL gap or reorder");

        // We handle this here instead of stage 2 because stage 2
        // is already a heavily loaded thread. According to the profiler,
        // stage 3 is only at ~40% load, so the WAL output buffer
        // construction has been moved here :)
        let mut lsn = batch.lsn_low;
        for req in batch.items.iter() {
            match req.op() {
                Operation::Put => {
                    lsn += 1;
                    wal_format::encode_record(&mut batch.out, lsn, &req.data);
                }

                Operation::Get | Operation::Unknown(_) => continue
            }
        }

        // Stage 2 counted the PUTs; stage 3 must have encoded exactly that many
        debug_assert_eq!(lsn, batch.lsn_hi, "stage2/stage3 PUT count diverged");

        self.last_ingested_lsn = batch.lsn_hi;

        let batch_size = batch.out.len() as u64;
        let (offset, _) = self.seg.advance_offset(batch_size);
        self.fsync_planner.on_write_queued(batch_size);

        self.pending.push_back(IoWork::write(batch, offset));
    }

    fn rotate(&mut self, boundary_lsn: u64) {
        // todo: okay... I can't drop the current segment because writing to it might still be ongoing
        // soooo...I'll just sync-wait until all the recordings are finished :)
        //
        // Maybe I'll make this asynchronous later, too
        while !self.inflight.is_empty() || !self.pending.is_empty() {
            self.handle_pending();
            self.submit_ring_and_park(1);
            // yeah, I also hate the fact that I have to call reap_cqes HERE!!!
            self.reap_cqes();
        }

        // Seal the old segment: sync fsync is fine for now, rotation is rare
        let ret = unsafe { libc::fsync(self.seg.fd_i32()) };
        assert_eq!(ret, 0, "fsync on segment seal failed");
        self.fsync_planner.on_fsync_completed();

        // and process awaiting_fsync to answer clients and recycle batch
        for mut b in self.awaiting_fsync.drain(..) {
            self.ack.advance(&mut b, AckPoint::Durable);
            b.recycle();
            let _ = self.recycle_tx.send(b);
        }

        // and now we're ready
        self.seg = Segment::open(&self.dir, boundary_lsn).expect("open segment");
    }

    fn retire(&mut self, boundary_lsn: u64) {
        if self.seg.start_lsn() < boundary_lsn {
            panic!("Segment starts after the boundary LSN. This is a bug, report to programmer")
        }

        // todo: add rotation rules and use here
        if let Ok(segs) = wal_format::list_segments(&self.dir) {
            for (lsn, path) in segs {
                if lsn < boundary_lsn {
                    let c_path = CString::new(path.as_os_str().as_bytes()).expect("Path contains null bytes");
                    self.pending.push_back(
                        IoWork::Remove(c_path)
                    );
                }
            }
        }
    }

    fn submit_ring_and_park(&mut self, want: usize) {
        if let Err(e) = self.ring.submit_and_wait(want) {
            if e.raw_os_error() != Some(libc::EINTR) {
                panic!("io_uring submit_and_wait failed: {}", e);
            }
        }
    }

    // SQ
    // returns true if the job was added to inflight
    fn handle_pending(&mut self) -> bool {
        if self.pending.is_empty() {
            return false
        }

        let mut sq = self.ring.submission();
        let before_pull = self.inflight.len();

        while !sq.is_full() && self.inflight.len() < self.depth {
            if let Some(work) = self.pending.pop_front() {
                let vacant = self.inflight.vacant_entry();
                let entry_idx = vacant.key() as u64;

                let sqe = work.sqe(self.seg.fd(), entry_idx);
                unsafe { sq.push(&sqe).unwrap() };
                vacant.insert(work);
            } else {
                break
            }
        }

        sq.sync();
        before_pull != self.inflight.len()
    }

    // todo comm: CQ
    fn reap_cqes(&mut self) {
        for cqe in self.ring.completion() {
            let idx = cqe.user_data() as usize;
            let result = cqe.result();
            let work = self.inflight.remove(idx);

            match work.complete(result) {
                Verdict::Ok(maybe_batch) => {
                    if let Some(mut batch) = maybe_batch {
                        self.ack.advance(&mut batch, AckPoint::Written);

                        if self.ack.is_settled(&batch) {
                            // No one in the batch is waiting for fsync
                            batch.recycle();
                            let _ = self.recycle_tx.send(batch);
                        } else {
                            // Clients are waiting for Durable status!
                            self.awaiting_fsync.push(batch);
                        }
                    }
                },

                Verdict::Retry(io_work) => {
                    if let IoWork::Write(ref w) = io_work {
                        self.fsync_planner.on_write_queued((w.data.out.len() - w.done) as u64);
                    }
                    self.pending.push_front(io_work);
                },

                Verdict::Durable => {
                    for mut batch in self.awaiting_fsync.drain(..) {
                        self.ack.advance(&mut batch, AckPoint::Durable);
                        batch.recycle();
                        let _ = self.recycle_tx.send(batch);
                    }

                    self.fsync_planner.on_fsync_completed();
                }

                Verdict::Fatal(error) => {
                    panic!("Fatal I/O error in WAL disk worker: {}. Halting to prevent data corruption.", error);
                },
            }
        }
    }
}
