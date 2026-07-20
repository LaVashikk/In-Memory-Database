use std::os::fd::AsRawFd;
use io_uring::types;
use storage::wal_format;

pub struct Segment {
    _file: std::fs::File, // keep open for RAII
    fd: types::Fd,
    offset: u64,
    fsync_offset: u64,
    start_lsn: u64,
}
impl Segment {
    pub fn open(dir: &std::path::Path, start_lsn: u64) -> std::io::Result<Segment> {
        let path = wal_format::segment_path(dir, start_lsn);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)?;

        let bytes = file.metadata()?.len();
        let fd = types::Fd(file.as_raw_fd());

        Ok(Segment {
            _file: file,
            offset: bytes,
            fsync_offset: bytes,
            fd,
            start_lsn,
        })
    }

    // --- getters ---
    #[inline(always)]
    pub fn fd(&self) -> types::Fd { self.fd }

    #[inline(always)]
    pub fn fd_i32(&self) -> i32 { self.fd.0 }

    #[inline(always)]
    pub fn start_lsn(&self) -> u64 { self.start_lsn }

    // --- event handlers ---

    /// Returns: (old_offset, new_offset)
    pub fn advance_offset(&mut self, bytes_size: u64) -> (u64, u64) {
        let o = self.offset;
        self.offset += bytes_size;
        (o, o + bytes_size)
    }

    pub fn mark_fsynced(&mut self) {
        self.fsync_offset = self.offset;
    }
}
