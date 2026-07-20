// wip: 3-stage pipelined in-memory kv store
// s1 (tokio) -> s2 (single sync thread) -> s3 (io_uring wal)
//
// recovery: load snap -> replay wal records greater than snap_lsn

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub mod persist;
pub mod snapshot;









// one ready for handling request from client!
pub struct Request {
    // todo: hmmmm maybe use BOX?
    pub data: bytes::Bytes, // [op][klen:u32][key][val]
    pub reply: Sender<Resp>,
    pub resp: Option<Resp>,
}
impl Request {
    #[inline(always)]
    pub fn op(&self) -> u8 {
        self.data[0]
    }
    #[inline(always)]
    pub fn klen(&self) -> usize {
        rd_u32(&self.data[1..5])
    }
    #[inline(always)]
    pub fn key(&self) -> &[u8] {
        &self.data[5..5 + self.klen()]
    }
    #[inline(always)]
    pub fn val(&self) -> &[u8] {
        &self.data[5 + self.klen()..]
    }
    #[inline(always)]
    pub fn is_read_only(&self) -> bool {
        self.op() != OP_PUT
    }
}

// batch struct that circulates in fixed ring pool. fields are reused [!!]
pub struct Batch {
    pub items: Vec<Request>,
    pub out: Vec<u8>,   // wal redo buffer (lsn stamped put records)
    /// WAL numbering contract (half-open interval):
    /// - `lsn_low` = global watermark BEFORE this batch = last LSN of the previous batch.
    /// - `lsn_hi`  = watermark AFTER this batch = LSN of its last PUT.
    /// Stage 2 assigns numbers by incrementing the watermark BEFORE each PUT;
    /// stage 3 replays the same rule when encoding: increment, then encode.
    pub lsn_low: u64,   // lowest  lsn assigned in this batch
    pub lsn_hi: u64,    // highest lsn assigned in this batch
}

impl Batch {
    pub fn with_capacity(items: usize, out: usize) -> Self {
        Self {
            items: Vec::with_capacity(items),
            out: Vec::with_capacity(out),
            lsn_low: 0,
            lsn_hi: 0,
        }
    }

    #[inline(always)]
    pub fn recycle(&mut self) {
        self.items.clear();
        self.out.clear();
        self.lsn_low = 0;
        self.lsn_hi = 0;
    }

    #[inline(always)]
    pub fn has_wal_work(&self) -> bool {
        self.lsn_hi > self.lsn_low
    }
}

impl Deref for Batch {
    type Target = Vec<Request>;

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}
impl DerefMut for Batch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.items
    }
}


pub mod db;
pub use db::Db;

// apply batch in place. lsn is 1-based monotonic counter
pub fn apply(&mut self, batch: &mut Batch, lsn: &mut u64) {
    let Batch { items, out, lsn_low, lsn_hi } = batch;
    *lsn_low = *lsn;

    for req in items.iter_mut() {
        match req.op() {
            // 'GET' - just return the value if we have it
            OP_GET => {
                req.resp = Some(match self.map.get(req.key()) {
                    Some(v) => Resp::Value(v.clone()), // cheap ARC clone
                    None => Resp::Miss,
                });
            }

            // 'PUT' - set/change value in table
            OP_PUT => {
                let klen = req.klen();
                let key = &req.data[5..5 + klen];
                let val = &req.data[5 + klen..];
                *lsn += 1;
                // *lsn_hi = *lsn; // lsn_hi is the last 'changing' lsn
                match self.map.get_mut(key) {
                    Some(slot) => {
                        if slot.len() == val.len() && let Some(buf) = Arc::get_mut(slot) {
                            buf.copy_from_slice(val); // todo: safety?
                        }
                    },
                    None => { self.map.insert(key.into(), Arc::from(val)); } ,
                };

                req.resp = Some(Resp::Ok);
            }

            // TODO: add remove

            _ => req.resp = Some(Resp::UnknownOp),
        }

        *lsn_hi = *lsn; // why i just cannot do that?
    }
}
