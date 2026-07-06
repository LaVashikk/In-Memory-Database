//! persistence layer: wal framing, snapshots, crash recovery
use super::*;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const SEG_PREFIX: &str = "wal.";
const SNAP_MAGIC: u32 = u32::from_le_bytes(*b"POC1");


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
pub fn encode_put(out: &mut Vec<u8>, lsn: u64, wire_bytes_blob: &[u8]) {
    let body_len = 8 + wire_bytes_blob.len(); // lsn + body lenght
    out.extend_from_slice(&(body_len as u32).to_le_bytes()); // todo: u32 or u64?
    let body_start = out.len() + 4;

    // LSN
    out.extend_from_slice(&(lsn).to_le_bytes());
    // wire-proto body from req
    out.extend_from_slice(wire_bytes_blob);

    // checksum
    let cs = checksum(&out[body_start..]);
    out.extend_from_slice(&cs.to_le_bytes());
}

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

// crash-safe snapshot: write tmp -> fdatasync -> atomic rename -> sync parent dir
// FORMAT: "[magic][start-lsn][len] | ( [k-len][key][v-len][val] )+"
// TODO: use COW-child
pub fn write_snapshot(db: &Db, dir: &Path, lsn: u64) -> std::io::Result<()> {
    let tmp = dir.join("snapshot.tmp");
    {
        let f = fs::File::create(&tmp)?;
        let mut w = std::io::BufWriter::new(f);

        // file start
        w.write_all(&SNAP_MAGIC.to_le_bytes())?;
        w.write_all(&lsn.to_le_bytes())?;
        w.write_all(&(db.len() as u64).to_le_bytes())?;

        // Serealization
        for (k, v) in db.iter() {
            w.write_all(&(k.len() as u32).to_le_bytes())?;
            w.write_all(k)?;
            w.write_all(&(v.len() as u32).to_le_bytes())?;
            w.write_all(v)?;
        }

        w.flush()?;
        w.get_ref().sync_data()?;
    }

    fs::rename(&tmp, dir.join("snapshot"))?; // atomic rename
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all(); // fsync dir so rename survives crash
    }
    Ok(())
}

fn load_snapshot(bytes: &[u8], db: &mut Db) -> Option<u64> {
    if bytes.len() < 20 || rd_u32(&bytes[0..4]) as u32 != SNAP_MAGIC {
        eprintln!("Snapshot file is invalid!");
        return None;
    }

    let lsn = rd_u64(&bytes[4..12]);
    let db_len = rd_u64(&bytes[12..20]);
    let mut p: usize = 20; // POINTER

    // Now reading key-value pairs
    for _ in 0..(db_len as usize) {
        if p + 4 > bytes.len() {
            break;
        }

        let key_len = rd_u32(&bytes[p..p+4]);
        p += 4;
        let key = &bytes[p..p + key_len];
        p += key_len;

        let val_len = rd_u32(&bytes[p..p+4]);
        p += 4;
        let val = &bytes[p..p + val_len];
        p += key_len;

        db.insert_raw(key, val);

        // let key_len = rd_u32(&bytes[p..p + 4]);
        // let key = &bytes[p+4 .. p+4+key_len];
        // let val_len = rd_u32(&bytes[p+4+key_len .. p+4+key_len+4]);
        // let val = &bytes[p+4+key_len+4 .. p+4+key_len+4+val_len];
    }

    Some(lsn)
}

const WAL_LEN_SIZE: usize = 4;
const WAL_CHECKSUM_SIZE: usize = 8;

// recovery: load snap + replay wal tail (lsn > snap_lsn).
// stops on first torn/corrupted record (normal after kill -9)
pub fn recover(dir: &Path) -> std::io::Result<(Db, u64)> {
    fs::create_dir_all(dir)?;
    let mut db = Db::new();
    let mut snap_lsn = 0u64;
    let mut highest = 0u64;

    let snap = dir.join("snapshot");
    if snap.exists() {
        if let Some(l) = load_snapshot(&fs::read(&snap)?, &mut db) {
            snap_lsn = l;
            highest = l;
        }
    }

    let mut replayed = 0u64;
    let mut torn = false;
    for (_start, path) in list_segments(dir)? {
        let bytes = fs::read(&path)?;
        let mut p: usize = 0; // POINTER

        while p + WAL_LEN_SIZE <= bytes.len() {
            let body_len = rd_u32(&bytes[p..p + 4]);
            if p + WAL_LEN_SIZE + body_len + WAL_CHECKSUM_SIZE > bytes.len() {
                torn = true; // torn tail: not enough bytes
                break;
            }

            // body: [lsn][op][k-len][key][value]
            let end_of_body = p + WAL_LEN_SIZE + body_len;
            let body = &bytes[(p + WAL_LEN_SIZE) .. (end_of_body)];

            // checksum
            let cs = rd_u64(&bytes[(end_of_body) .. (end_of_body + WAL_CHECKSUM_SIZE)]);
            if checksum(body) != cs {
                torn = true; // corrupted tail, stop recording from wal
                break;
            }

            p = end_of_body + WAL_CHECKSUM_SIZE;

            // todo: validate this
            let lsn = rd_u64(&body[0..8]);
            if lsn <= snap_lsn {
                continue;
            }

            let op = body[8]; // TODO: pack this shit in... e.g., key len!
            let key_len = rd_u32(&body[9..13]);
            let key = &body[13..13 + key_len];
            let val = &body[13 + key_len..body_len];

            // RESTORE OPERATIONS
            if op == OP_PUT {
                db.insert_raw(key, val);
            }
            // todo: remove op later here

            if lsn > highest {
                highest = lsn;
            }
            replayed += 1;
        }

        if torn {
            break;
        }
    }
    eprintln!(
        "recover: loaded snap lsn={snap_lsn}, replayed={replayed} keys={} next_lsn={}{}",
        db.len(),
        highest + 1,
        if torn { " (wal tail torn, prob unclean shutdown)" } else { "" }
    );
    Ok((db, highest + 1))
}
