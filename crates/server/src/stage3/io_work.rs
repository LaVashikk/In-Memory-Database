//! The I/O vocabulary of the WAL engine: what can be in flight
//! and how each operation ends
use std::ffi::CString;
use io_uring::{opcode, squeue, types};
use crate::types::Batch;

// todo: too heavy?
pub enum Verdict {
    /// The operation fully succeeded
	///
	/// `Some(batch)` - it was a write op. It is necessary to respond to the clients
	/// `None` - the operation carried no batch (fsync, segment removal)
    Ok(Option<Batch>),

    /// The operation made no or partial progress for a transient reason.
	/// its offset and length already point at the first unwritten byte
    Retry(IoWork),

    /// An fsync completed: every record from group-commit is now on stable storage
    Durable,

    /// Unrecoverable I/O failure (bad fd, ENOSPC, ...)
	/// Durability can't longer be promised
    Fatal(std::io::Error),
}

pub struct WriteOp {
    pub data: Batch,
    pub offset: u64,
    pub done: usize,
}

/// One unit of in-flight async-I/O
pub enum IoWork { // todo: too heavy!!!!!
    Write(WriteOp),
    Fsync(super::LSN),
    Remove(CString),
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

            IoWork::Fsync(_) => {
                opcode::Fsync::new(fd)
                    .flags(types::FsyncFlags::DATASYNC)
                    .build()
                    // todo: ~80% of the time new writes in the ring are blocked by this barrier.
                    // Removing this could significantly improve latency!
                    .flags(squeue::Flags::IO_DRAIN)
                    .user_data(user_data)
            },

            IoWork::Remove(c_path) => {
                // Path resolved via AT_FDCWD, so a relative path is interpreted against the CWD
                opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), c_path.as_ptr())
                    .build()
                    .user_data(user_data)
            },
        }
    }

    pub fn complete(self, res: i32) -> Verdict {
        match self {
            IoWork::Write(mut w) => {
                if res == -libc::EINTR || res == -libc::EAGAIN {
                    return Verdict::Retry(IoWork::Write(w))
                }

                if res < 0 {
                    return Verdict::Fatal(std::io::Error::from_raw_os_error(-res))
                }

                w.done += res as usize;
                if w.done < w.data.out.len() {
                    Verdict::Retry(IoWork::Write(w))
                } else {
                    Verdict::Ok(Some(w.data))
                }
            },

            IoWork::Fsync(_target_lsn) => {
                if res < 0 {
                    return Verdict::Fatal(std::io::Error::from_raw_os_error(-res))
                }
                Verdict::Durable
            },

            IoWork::Remove(_) => {
                if res < 0 && res != -libc::ENOENT {
                    eprintln!("Warning: failed to unlink WAL segment: code {}", res);
                }
                Verdict::Ok(None)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(n: usize) -> Batch {
        let mut b = Batch::with_capacity(0, n);
        b.out.resize(n, 0xAB);
        b
    }

    #[test]
    fn short_write_retries_at_first_unwritten_byte() {
        match IoWork::write(batch(100), 0).complete(60) {
            Verdict::Retry(IoWork::Write(w)) => assert_eq!(w.done, 60),
            _ => panic!("expected Retry"),
        }
    }

    #[test]
    fn eintr_retries_without_progress() {
        match IoWork::write(batch(100), 0).complete(-libc::EINTR) {
            Verdict::Retry(IoWork::Write(w)) => assert_eq!(w.done, 0),
            _ => panic!("expected Retry"),
        }
    }

    #[test]
    fn remove_tolerates_enoent() {
        let w = IoWork::Remove(CString::new("/nope").unwrap());
        assert!(matches!(w.complete(-libc::ENOENT), Verdict::Ok(None)));
    }

    #[test]
    fn how_heavy_actually() {
        // closes the "too heavy?" todo with a number instead of a feeling
        eprintln!("size_of<IoWork> = {}", std::mem::size_of::<IoWork>());
    }
}
