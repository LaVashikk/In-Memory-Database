use std::collections::VecDeque;
use std::ffi::CString;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::mpsc::{self as sync_mpsc, TryRecvError};
use anyhow::bail;
use io_uring::{IoUring, opcode, squeue, types};
use raw_shared_types::{Batch, persist};
use slab::Slab;

use crate::{Ctrl, SEG_BYTES};
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
    Remove(CString),
    // Rotation, // todo
    // Compact, // todo
}

impl IoWork {
    pub fn sqe(&self, fd: io_uring::types::Fd, user_data: u64) -> io_uring::squeue::Entry {
        match self {
            IoWork::Write(w) => {
                let remainder = &w.data.out[w.done..];
                let len = remainder.len() as u32;
                let ptr = remainder.as_ptr();

                opcode::Write::new(fd, ptr, len)
                    .offset(w.offset)
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
                // todo???
                if res == -libc::EINTR || res == -libc::EAGAIN {
                    return Verdict::Retry(IoWork::Write(w))
                }

                if res < 0 {
                    return Verdict::Fatal(todo!())
                }

                w.done += res as usize;
                if w.done < w.data.len() {
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

const DEFAULT_SPIN_BUDGET: u32 = 500; // todo
const IO_WAIT_NS: u32 = 500_000;

pub struct WalEngine {
    lsn: u64,
    ring: IoUring,
    seg: Segment,
    mode: Mode,
    pending: VecDeque<IoWork>,  // or maybe just vec?
    inflight: Slab<IoWork>,
    spin_budget: u32,

    batch_rx: sync_mpsc::Receiver<Batch>,
    recycle_tx: sync_mpsc::Sender<Batch>,
}

impl WalEngine {
    fn new(
        ring_depth: usize,
        dir: PathBuf,
        start_lsn: u64,
        mode: Mode,
        batch_rx: sync_mpsc::Receiver<Batch>,
        recycle_tx: sync_mpsc::Sender<Batch>,
    ) -> anyhow::Result<Self> {
        let ring = IoUring::new(ring_depth as u32)?;
        let seg = Segment::open(&dir, start_lsn)?;

        Ok(
            Self {
                lsn: start_lsn,
                ring,
                seg,
                mode,
                pending: VecDeque::with_capacity(ring_depth),
                inflight: Slab::with_capacity(ring_depth),
                spin_budget: DEFAULT_SPIN_BUDGET,

                batch_rx,
                recycle_tx,
            }
        )
    }

    fn out_of_spins(&self) -> bool {
        self.spin_budget <= 0
    }
    fn should_park(&self) -> bool {
        self.pending.is_empty() && self.pending.is_empty()
    }


    fn start(mut self) {
        loop {
            let batch = self.poll_channel();

            if let Some(data) = batch && !data.out.is_empty() {
                // self.
            }

            if self.out_of_spins() {
                self.ring.submit_and_wait(1);
            }
        }
    }


    fn poll_channel(&mut self) -> Option<Batch> {
        let mut batch = if self.should_park() {
            self.batch_rx.recv().ok()?
        } else {
            match self.batch_rx.try_recv() {
                Ok(buf) => buf,
                Err(TryRecvError::Empty) => {
                    self.spin_budget -= 1;
                    return None
                },
                Err(_) => panic!("Stage 2->3 communication has been broken"),
            }
        };

        // We handle this here instead of stage 2 because stage 2
        // is already a heavily loaded thread. According to the profiler,
        // stage 3 is only at ~40% load, so the WAL output buffer
        // construction has been moved here.
        // AND
        for (idx, req) in batch.items.iter().enumerate() {
            match req.op() {
                OP_PUT => {
                    persist::encode_put(&mut batch.out, batch.lsn_low + idx as u64, &req.data);
                }

                _ => continue
            }
        }

        // update spin-budget
        self.spin_budget = DEFAULT_SPIN_BUDGET;

        Some(batch)
    }

    fn queue_write(&mut self, batch: Batch) {
        // IoWork::Write(
        //     WriteOp { data: batch, offset: self.seg.offset, done: 0 }
        // );
    }

    // SQ
    fn sqe_handle(&mut self) {

    }

    // CQ
    fn cqe_pump(&mut self) {
        todo!()
    }
}

// disk writer thread (wal append + optional fsync via io_uring)
pub fn run_io_worker(
    s23_rx: sync_mpsc::Receiver<Batch>,
    free_tx: sync_mpsc::Sender<Batch>,
    ctrl_rx: sync_mpsc::Receiver<Ctrl>,
    mut ring: IoUring,
    mode: Mode,
    dir: PathBuf,
    start_lsn: u64,
) {
    todo!()
    // let mut seg = Segment::open(&dir, start_lsn).expect("open WAL segment");

    // loop {
    //     let mut batch = match s23_rx.recv() {
    //         Ok(b) => b,
    //         Err(_) => break,
    //     };

    //     if !batch.out.is_empty() { // todo
    //         // rotate file size limit
    //         if seg.bytes >= SEG_BYTES {
    //             seg = Segment::open(&dir, batch.lsn_hi).expect("rotate WAL segment");
    //         }
    //         wal_write_all(&mut ring, seg.fd, &batch.out, seg.bytes).expect("WAL write");
    //         seg.bytes += batch.out.len() as u64;
    //         if mode.do_fsync() {
    //             wal_fsync(&mut ring, seg.fd).expect("WAL fdatasync");
    //         }
    //     }

    //     // sync/nofsync mode: reply after write/fsync finishes
    //     if mode.reply_in_stage3() {
    //         for req in batch.items.iter_mut() {
    //             if let Some(r) = req.resp.take() {
    //                 let _ = req.reply.blocking_send(r);
    //             }
    //         }
    //     }

    //     // process compaction signal
    //     while let Ok(Ctrl::Compact(durable_lsn)) = ctrl_rx.try_recv() { // todo: temp approach
    //         compact(&dir, durable_lsn, seg.start_lsn);
    //     }

    //     batch.recycle();
    //     if free_tx.send(batch).is_err() {
    //         break;
    //     }
    // }
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

// remove old wal segments fully covered by durable snapshot
fn compact(dir: &std::path::Path, durable_lsn: u64, active_start: u64) { // todo: temp impl
    let segs = match persist::list_segments(dir) {
        Ok(s) => s,
        Err(_) => return,
    };

    for i in 0..segs.len() {
        let (start, ref path) = segs[i];
        if start == active_start {
            continue;
        }

        // if next seg start is <= durable_lsn + 1, current seg is fully in snapshot
        let next_start = segs.get(i + 1).map(|(s, _)| *s).unwrap_or(u64::MAX);
        if next_start <= durable_lsn + 1 {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn wal_write_all(ring: &mut IoUring, fd: i32, buf: &[u8], mut off: u64) -> std::io::Result<()> {
    let mut done = 0usize;
    while done < buf.len() {
        let ptr = unsafe { buf.as_ptr().add(done) };
        let rem = (buf.len() - done) as u32;
        let sqe = opcode::Write::new(types::Fd(fd), ptr, rem)
            .offset(off)
            .build()
            .user_data(1);
        unsafe {
            ring.submission().push(&sqe).expect("SQ full");
        }
        ring.submit_and_wait(1)?;
        let r = ring.completion().next().expect("CQE").result();
        if r < 0 {
            return Err(std::io::Error::from_raw_os_error(-r));
        }
        if r == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "zero write",
            ));
        }
        done += r as usize;
        off += r as u64;
    }
    Ok(())
}

fn wal_fsync(ring: &mut IoUring, fd: i32) -> std::io::Result<()> {
    let sqe = opcode::Fsync::new(types::Fd(fd))
        .flags(types::FsyncFlags::DATASYNC)
        .build()
        .user_data(2);
    unsafe {
        ring.submission().push(&sqe).expect("SQ full");
    }
    ring.submit_and_wait(1)?;
    let r = ring.completion().next().expect("CQE").result();
    if r < 0 {
        return Err(std::io::Error::from_raw_os_error(-r));
    }
    Ok(())
}
