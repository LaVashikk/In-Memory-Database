use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::mpsc as sync_mpsc;
use io_uring::{opcode, types, IoUring};
use raw_shared_types::{Batch, persist};

use crate::{Ctrl, SEG_BYTES};
use crate::args::Mode;


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
    let mut seg = Segment::open(&dir, start_lsn).expect("open WAL segment");

    loop {
        let mut batch = match s23_rx.recv() {
            Ok(b) => b,
            Err(_) => break,
        };

        if !batch.out.is_empty() { // todo
            // rotate file size limit
            // if seg.bytes >= SEG_BYTES {
            //     seg = Segment::open(&dir, batch.lsn_hi).expect("rotate WAL segment");
            // }
            // wal_write_all(&mut ring, seg.fd, &batch.out, seg.bytes).expect("WAL write");
            // seg.bytes += batch.out.len() as u64;
            // if mode.do_fsync() {
            //     wal_fsync(&mut ring, seg.fd).expect("WAL fdatasync");
            // }
        }

        // sync/nofsync mode: reply after write/fsync finishes
        if mode.reply_in_stage3() {
            for req in batch.items.iter_mut() {
                if let Some(r) = req.resp.take() {
                    let _ = req.reply.blocking_send(r);
                }
            }
        }

        // process compaction signal
        while let Ok(Ctrl::Compact(durable_lsn)) = ctrl_rx.try_recv() { // todo: temp approach
            compact(&dir, durable_lsn, seg.start_lsn);
        }

        batch.recycle();
        if free_tx.send(batch).is_err() {
            break;
        }
    }
}


struct Segment {
    _file: std::fs::File, // keep open for RAII
    fd: i32,
    bytes: u64,
    start_lsn: u64,
}
impl Segment {
    fn open(dir: &std::path::Path, start_lsn: u64) -> std::io::Result<Segment> {
        let path = persist::segment_path(dir, start_lsn);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let bytes = file.metadata()?.len();
        let fd = file.as_raw_fd();
        Ok(Segment {
            _file: file,
            fd,
            bytes,
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
