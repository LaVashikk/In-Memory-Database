use std::collections::VecDeque;
use std::ffi::CString;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::mpsc::{self as sync_mpsc, TryRecvError};
use anyhow::{Context, bail};
use io_uring::{IoUring, opcode, squeue, types};
use raw_shared_types::{Batch, OP_PUT, persist};
use slab::Slab;

use crate::{SEG_BYTES, WalMsg};
use crate::args::Mode;

// todo: too heavy?
pub enum Verdict {
    Ok(Option<Batch>), // return batch??
    Retry(IoWork),
    Fatal(std::io::Error),
}

pub struct WriteOp {
    pub data: Batch,
    pub offset: u64,
    pub done: usize,
}

// todo: too heavy!!!!!
pub enum IoWork {
    Write(WriteOp),
    Fsync,
    Remove(PathBuf),
    // Rotation, // todo
    // Retire, // todo
}

impl IoWork {
    pub fn write(data: Batch, offset: u64) -> Self {
        Self::Write(
            WriteOp { data, offset, done: 0 }
        )
    }

    pub fn sqe(&self, fd: io_uring::types::Fd, user_data: u64) -> io_uring::squeue::Entry {
        match self {
            IoWork::Write(w) => {
                let remainder = &w.data.out[w.done..];
                let len = remainder.len() as u32;
                let ptr = remainder.as_ptr();

                opcode::Write::new(fd, ptr, len)
                    .offset(w.offset + w.done as u64)
                    .build()
                    .user_data(user_data)
            },

            IoWork::Fsync => {
                opcode::Fsync::new(fd)
                    .flags(types::FsyncFlags::DATASYNC)
                    .build()
                    .flags(squeue::Flags::IO_DRAIN)
                    .user_data(user_data)
            },

            IoWork::Remove(path) => {
                // PathBuf to CSting
                todo!()
            },

            // IoWork::Rotation => {
            //     todo!()
            // },

            // IoWork::Compact => {
            //     todo!()
            // },
        }
    }

    pub fn complete(self, res: i32) -> Verdict {
        match self {
            IoWork::Write(mut w) => {
                if res == -libc::EINTR || res == -libc::EAGAIN {
                    return Verdict::Retry(IoWork::Write(w))
                }

                if res < 0 {
                    return Verdict::Fatal(todo!()) // todo: create err
                }

                w.done += res as usize;
                if w.done < w.data.out.len() {
                    Verdict::Retry(IoWork::Write(w))
                } else {
                    Verdict::Ok(Some(w.data))
                }
            },

            IoWork::Fsync => todo!(),
            IoWork::Remove(cstring) => todo!(),
        }
    }
}

const DEFAULT_SPIN_BUDGET: i32 = 500; // todo
const IO_WAIT_NS: u32 = 500_000;

pub struct WalEngine {
    dir: PathBuf,
    last_ingested_lsn: u64,
    ring: IoUring,
    seg: Segment,
    mode: Mode,
    pending: VecDeque<IoWork>,  // or maybe just vec?
    inflight: Slab<IoWork>,
    spin_budget: i32,

    msg_rx: sync_mpsc::Receiver<WalMsg>,
    recycle_tx: sync_mpsc::Sender<Batch>,
}

impl WalEngine {
    fn new(
        ring_depth: usize,
        dir: PathBuf,
        start_lsn: u64,
        mode: Mode,
        batch_rx: sync_mpsc::Receiver<WalMsg>,
        recycle_tx: sync_mpsc::Sender<Batch>,
    ) -> anyhow::Result<Self> {
        let ring = IoUring::new(ring_depth as u32)?;
        let seg = Segment::open(&dir, start_lsn)?;

        Ok(
            Self {
                dir,
                last_ingested_lsn: start_lsn,
                ring,
                seg,
                mode,
                pending: VecDeque::with_capacity(ring_depth), // todo: actually, not the real ring_depth, but prob it's ok
                inflight: Slab::with_capacity(ring_depth),
                spin_budget: DEFAULT_SPIN_BUDGET,

                msg_rx: batch_rx,
                recycle_tx,
            }
        )
    }

    fn out_of_spins(&self) -> bool {
        self.spin_budget <= 0
    }
    fn should_park(&self) -> bool {
        self.pending.is_empty() && self.inflight.is_empty()
    }
    fn ring_is_full(&self) -> bool {
        self.inflight.len() == self.inflight.capacity() // todo: Can I access the capacity and call realloc?? maybe use depth value?
    }


    // Spin-loop strategy with a budget. If we receive enough new requests, we don't park, but continuously
    // add work. However, if there are very few requests, spinning makes no sense, so we park and wait for data from the ring-cq.
    fn start(mut self) {
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

            // todo: answers in processing_cqes - but if it's a full GET BATCH - there will be no answers at all!
            // HOWEVER, I'm thinking about dropping GET answers immediately in stage 2, without even moving them to stage 3!! seems like a solid idea.
            // todo 2: okay, but if the batch is empty without 'out' - it simply won't return to the pool - and it will die!!

            // todo: fucking answers!! [meeeh]
            // todo: fucking fsync! [meeh]
            // if self.planner.should_fsync(Instant::now()) {
                // self.pending.push_back(IoWork::Fsync { todo!() });
            // }

            let is_need_submit = self.handle_pending();
            self.reap_cqes();

            // Parking on IO if there out-of-sping!
            if (self.out_of_spins() && !self.inflight.is_empty()) || self.ring_is_full() {
                self.submit_ring_and_park(1);
            }
            else if is_need_submit {
                // tODO: nope, create a FsyncPlanner stateless stuff
                // if self.mode.do_fsync() {  // todo: remove this one
                //     self.pending.push_back(IoWork::Fsync);
                // }

                // Actually, I can remove this ONE syscall, and just create a sq-watcher on the side of the kernel.
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
        // We handle this here instead of stage 2 because stage 2
        // is already a heavily loaded thread. According to the profiler,
        // stage 3 is only at ~40% load, so the WAL output buffer
        // construction has been moved here.
        let mut lsn = batch.lsn_low;
        for req in batch.items.iter() {
            match req.op() {
                OP_PUT => {
                    lsn += 1; // before or after doing incr????
                    persist::encode_put(&mut batch.out, lsn, &req.data);
                }

                _ => continue
            }
        }

        assert!(batch.lsn_hi > self.last_ingested_lsn, "batches out of LSN order. This is a bug, report it");
        self.last_ingested_lsn = batch.lsn_hi;

        let offset = self.seg.offset;
        self.seg.offset += batch.out.len() as u64;
        // self.fsync_planner // todo create this
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

        // Seal the old segment: sync fsync is fine, rotation is rare.
        unsafe { libc::fsync(self.seg.fd) }; // todo: handle it?
        // TODO: also process FSYNC here later

        // and now we're ready
        self.seg = Segment::open(&self.dir, boundary_lsn).expect("open segment");
    }

    fn retire(&mut self, boundary_lsn: u64) {
        if self.seg.start_lsn < boundary_lsn {
            panic!("Segment starts after the boundary LSN. This is a bug, report to programmer")
        }

        if let Ok(segs) = persist::list_segments(&self.dir) {
            for (lsn, path) in segs {
                if lsn < boundary_lsn {
                    self.pending.push_back(
                        IoWork::Remove(path)
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
    // return: something was added
    fn handle_pending(&mut self) -> bool {
        if self.pending.is_empty() {
            return false
        }

        let mut sq = self.ring.submission();
        let before_pull = self.inflight.len();

        while !sq.is_full() {
            if let Some(work) = self.pending.pop_front() {
                let vacant = self.inflight.vacant_entry();
                let entry_idx = vacant.key() as u64;

                let sqe = work.sqe(types::Fd(self.seg.fd), entry_idx); // todo: use types fd wrapper, probably
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
                        // sync/nofsync mode: reply after write/fsync finishes
                        // TODO: nah, i'll create the 'Acker' tomorrow
                        if self.mode.reply_in_stage3() {
                            for req in batch.items.iter_mut() {
                                if let Some(r) = req.resp.take() {
                                    let _ = req.reply.blocking_send(r);
                                }
                            }
                        }

                        batch.recycle();
                        let _ = self.recycle_tx.send(batch);
                    }
                },

                Verdict::Retry(io_work) => {
                    self.pending.push_back(io_work); // todo: or push_front?
                },

                Verdict::Fatal(error) => {
                    panic!("Fatal I/O error in WAL disk worker: {}. Halting to prevent data corruption.", error);
                },
            }
        }
    }
}


struct Segment {
    _file: std::fs::File, // keep open for RAII
    fd: i32,
    offset: u64,
    fsync_offset: u64,
    start_lsn: u64,
}
impl Segment {
    fn open(dir: &std::path::Path, start_lsn: u64) -> std::io::Result<Segment> {
        let path = persist::segment_path(dir, start_lsn);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)?;

        let bytes = file.metadata()?.len();
        let fd = file.as_raw_fd();

        Ok(Segment {
            _file: file,
            offset: bytes,
            fsync_offset: bytes,
            fd,
            start_lsn,
        })
    }
}


// // remove old wal segments fully covered by durable snapshot
// fn compact(dir: &std::path::Path, durable_lsn: u64, active_start: u64) { // todo: temp impl
//     let segs = match persist::list_segments(dir) {
//         Ok(s) => s,
//         Err(_) => return,
//     };

//     for i in 0..segs.len() {
//         let (start, ref path) = segs[i];
//         if start == active_start {
//             continue;
//         }

//         // if next seg start is <= durable_lsn + 1, current seg is fully in snapshot
//         let next_start = segs.get(i + 1).map(|(s, _)| *s).unwrap_or(u64::MAX);
//         if next_start <= durable_lsn + 1 {
//             let _ = std::fs::remove_file(path);
//         }
//     }
// }
