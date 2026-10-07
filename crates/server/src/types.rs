use wire::Resp;

pub struct Request {
    pub data: bytes::Bytes, // wire body: [op][klen:u32][key][val]
    pub reply: tokio::sync::mpsc::Sender<Resp>,
    pub resp: Option<Resp>,
}

impl Request {
    #[inline(always)]
    pub fn op(&self) -> wire::Operation { wire::op(&self.data).into() }
    #[inline(always)]
    pub fn op_u8(&self) -> u8 { wire::op(&self.data) }
    #[inline(always)]
    pub fn key(&self) -> &[u8] { wire::key(&self.data) }
    #[inline(always)]
    pub fn split_kv(&self) -> (&[u8], &[u8]) { wire::split_kv(&self.data) }
    #[inline(always)]
    pub fn is_read_only(&self) -> bool { matches!(self.op(), wire::Operation::Get) }
}

/// Batch struct that circulates in fixed ring pool. Fields are reused [!!]
pub struct Batch {
    pub items: Vec<Request>,
    pub out: Vec<u8>,   // wal redo buffer
    /// WAL numbering contract (half-open interval):
    /// - `lsn_low` = global watermark BEFORE this batch = last LSN of the previous batch.
    /// - `lsn_hi`  = watermark AFTER this batch = LSN of its last PUT.
    /// Stage 2 assigns numbers by incrementing the watermark BEFORE each PUT;
    /// stage 3 replays the same rule when encoding: increment, then encode.
    pub lsn_low: u64,   // lowest  lsn
    pub lsn_hi: u64,    // highest lsn
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
