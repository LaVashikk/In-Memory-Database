use std::{fs, path::{Path, PathBuf}};
use wire::{rd_u32, rd_u64};

pub const SEG_PREFIX: &str = "wal.";
const LEN_SIZE: usize = 4;
const CHECKSUM_SIZE: usize = 8;

// list wal segments sorted by start_lsn
pub fn list_segments(dir: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut v = Vec::new();
    if !dir.exists() {
        return Ok(v);
    }

    for e in fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name();
        let name = name.to_string_lossy();
        if let Some(num) = name.strip_prefix(SEG_PREFIX) {
            if let Ok(start) = num.parse::<u64>() {
                v.push((start, e.path()));
            }
        }
    }

    v.sort_by_key(|(s, _)| *s);
    Ok(v)
}

pub fn segment_path(dir: &Path, start_lsn: u64) -> PathBuf {
    dir.join(format!("{SEG_PREFIX}{start_lsn:020}"))
}

// fnv1a 64bit checksum. fast enough atm
// TODO: try crc32fast crate
#[inline]
pub fn checksum(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325; // basic offset
    for &x in b {
        h ^= x as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3); // prime
    }
    h
}

// Body: [len](wire-body | [lsn][op][k-len][key][value])[checksum]
// size: (8 + 4 + '1' + 4 + key + value + 8) - todo 'bad padding' probably
pub fn encode_record(out: &mut Vec<u8>, lsn: u64, wire_bytes_blob: &[u8]) {
    let body_len = 8 + wire_bytes_blob.len(); // lsn + body lenght

    // [0..4] - len
    out.extend_from_slice(&(body_len as u32).to_le_bytes()); // todo: u32 or u64?
    let body_start = out.len();

    // [4..12] - LSN
    out.extend_from_slice(&(lsn).to_le_bytes());
    // [12..body_len] wire-proto body from req
    out.extend_from_slice(wire_bytes_blob);

    // checksum
    let cs = checksum(&out[body_start..]);
    out.extend_from_slice(&cs.to_le_bytes());
}

/// Result of decoder
pub struct Record<'a> {
    pub lsn: u64,
    /// Format: [op][klen][key][val]
    pub wire_body: &'a [u8],
    pub consumed: usize,
}

/// Decode one record at the start of bytes
/// `None` - torn or corrupted tail: stop replaying
pub fn decode_record(bytes: &[u8]) -> Option<Record<'_>> {
    if bytes.len() < LEN_SIZE {
        return None;
    }

    let body_len = rd_u32(&bytes[0..4]);

    // body: [lsn][op][k-len][key][value]
    let body_end = LEN_SIZE + body_len;
    if body_end + CHECKSUM_SIZE > bytes.len() {
        return None;
    }

    let body = &bytes[LEN_SIZE..body_end];

    // checksum
    let cs = rd_u64(&bytes[body_end..body_end + CHECKSUM_SIZE]);
    if checksum(body) != cs {
        return None;
    }

    Some(Record {
        lsn: rd_u64(&body[0..8]),
        wire_body: &body,
        consumed: body_end + CHECKSUM_SIZE,
    })
}
