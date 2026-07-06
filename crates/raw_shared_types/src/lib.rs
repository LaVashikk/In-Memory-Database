// wip: 3-stage pipelined in-memory kv store
// s1 (tokio) -> s2 (single sync thread) -> s3 (io_uring wal)
//
// recovery: load snap -> replay wal records greater than snap_lsn

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub mod persist;

// wire protocol opcodes
pub const OP_GET: u8 = 0;
pub const OP_PUT: u8 = 1;
// pub const OP_REM: u8 = 2; // todo

pub enum Resp {
    Value(Arc<[u8]>),
    Ok,
    Miss,
    UnknownOp,
}

impl Resp {
    #[inline]
    pub fn to_proto_code(&self) -> u8 {
        match self {
            Resp::Value(_) => 0,
            Resp::Ok => 1,
            Resp::Miss => 2,
            Resp::UnknownOp => 3,
        }
    }
}


#[inline]
pub fn rd_u32(b: &[u8]) -> usize {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize
}
#[inline]
pub fn rd_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}



// one ready for handling request from client!
pub struct Request {
    // todo: hmmmm maybe use BOX?
    pub data: Vec<u8>, // [op][klen:u32][key][val]
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
}

// batch struct that circulates in fixed ring pool. fields are reused [!!]
pub struct Batch {
    pub items: Vec<Request>,
    pub out: Vec<u8>,   // wal redo buffer (lsn stamped put records)
    pub lsn_hi: u64,    // highest lsn assigned in this batch
}
impl Batch {
    pub fn with_capacity(items: usize, out: usize) -> Self {
        Self {
            items: Vec::with_capacity(items),
            out: Vec::with_capacity(out),
            lsn_hi: 0,
        }
    }

    #[inline(always)]
    pub fn recycle(&mut self) {
        self.items.clear();
        self.out.clear();
        self.lsn_hi = 0;
    }
}

pub mod db;
pub use db::Db;
