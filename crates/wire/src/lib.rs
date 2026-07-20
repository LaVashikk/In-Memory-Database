use std::sync::Arc;

// wire protocol opcodes
pub const OP_GET: u8 = 0;
pub const OP_PUT: u8 = 1;
// pub const OP_REM: u8 = 2; // todo

/// Wire body layout: [op:1][klen:u32][key][val]
pub const BODY_HDR: usize = 5;

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum Operation {
    Get = OP_GET,
    Put = OP_PUT,
    // todo
}


// raw method for encoding request to wire-protocol
pub fn encode_raw(buf: &mut Vec<u8>, op: Operation, key: &[u8], value: Option<&[u8]>) -> usize {
    let len_before = buf.len();

    if matches!(op, Operation::Put) && value.is_none() {
        return 0;
    }

    // op + klen + key + value
    let body_len = 1 + 4 + key.len() + value.map(|v| v.len()).unwrap_or(0);

    // WIRE-PROTO: [len] + [op][klen:u32][key][val]
    buf.extend_from_slice(&(body_len as u32).to_le_bytes());

    buf.push(op as u8);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);

    if let Some(value) = value {
        buf.extend_from_slice(value);
    }

    buf.len() - len_before
}

pub fn encode(op: Operation, key: &[u8], value: Option<&[u8]>) -> Option<Vec<u8>> {
    let body_len = 1 + 4 + key.len() + value.map(|v| v.len()).unwrap_or(0);
    let mut buf = Vec::<u8>::with_capacity(4 + body_len);

    if encode_raw(&mut buf, op, key, value) != 0 {
        Some(buf)
    } else {
        None
    }
}


#[inline(always)]
pub fn op(body: &[u8]) -> u8 { body[0] }

#[inline(always)]
pub fn klen(body: &[u8]) -> usize { rd_u32(&body[1..5]) }

#[inline(always)]
pub fn key(body: &[u8]) -> &[u8] { &body[BODY_HDR..BODY_HDR + klen(body)] }

#[inline(always)]
pub fn split_kv(body: &[u8]) -> (&[u8], &[u8]) {
    let k = klen(body);
    (&body[BODY_HDR..BODY_HDR + k], &body[BODY_HDR + k..])
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resp {
    Value(Arc<[u8]>), // todo: heavy shit, buuuuuuuuut uuuuuuuuugh ok?
    Ok,
    Miss,
    UnknownOp,
    Unknown,
}

impl Resp {
    #[inline]
    pub fn to_proto_code(&self) -> u8 {
        match self {
            Resp::Value(_) => 0,
            Resp::Ok => 1,
            Resp::Miss => 2,
            Resp::UnknownOp => 3,
            _ => 4,
        }
    }

    pub fn from_proto_code(body: &[u8]) -> Self {
        match body.first().copied() {
            Some(0) => Resp::Value(body[1..].into()),
            Some(1) => Resp::Ok,
            Some(2) => Resp::Miss,
            Some(3) => Resp::UnknownOp,
            _ => Resp::Unknown,
        }
    }
}


// --- helpers ---
#[inline]
pub fn rd_u32(b: &[u8]) -> usize {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize
}
#[inline]
pub fn rd_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}
