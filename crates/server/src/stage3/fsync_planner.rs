use crate::args::Mode;

/// WHEN durability is requested. `AckPoint` decides WHO waits for it
#[derive(Clone, Copy)]
pub enum FsyncCadence {
    /// The WAL survives a process crash, but not a kernel panic or power loss
    Never,
    /// Back-to-back: the next fsync is armed as soon as the previous one completes;
	/// writes arriving in between share it
    Consistently,
    /// Fsync per `n` queued bytes: bounds the data-loss window by volume
    Bytes(u64),
}

impl From<Mode> for FsyncCadence {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::NoFsync => FsyncCadence::Never,
            Mode::Sync => FsyncCadence::Consistently,
            Mode::Async => FsyncCadence::Bytes(1 << 20), // todo: tuning knob
        }
    }
}

/// Decides when to push `IoWork::Fsync`
pub struct FsyncPlanner {
    cadence: FsyncCadence,
    /// Bytes queued for writing since the last armed fsync
    dirty_bytes: u64,
    /// An fsync is armed or in flight; `Consistently` won't arm another until it completes
    flush_running: bool,
}

impl FsyncPlanner {
    pub fn new(cadence: FsyncCadence) -> Self {
        Self { cadence, dirty_bytes: 0, flush_running: false }
    }

    /// `true` = append an `IoWork::Fsync` to this submission
	/// Not a pure query: a `true` answer consumes the window, so the same bytes never fsync twice
    #[inline]
    pub fn should_fsync(&mut self) -> bool {
        // Nothing dirty - nothing an fsync could make durable
        if self.dirty_bytes == 0 {
            return false;
        }
        let due = match self.cadence {
            FsyncCadence::Never => false,
            FsyncCadence::Consistently => !self.flush_running,
            FsyncCadence::Bytes(limit) => self.dirty_bytes >= limit,
        };
        if due {
            self.dirty_bytes = 0;
            self.flush_running = true;
        }
        due
    }

    #[inline]
    pub fn on_fsync_completed(&mut self) {
        self.flush_running = false;
    }

    #[inline]
    pub fn on_write_queued(&mut self, n: u64) {
        self.dirty_bytes += n;
    }
}
