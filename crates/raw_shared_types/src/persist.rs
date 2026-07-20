//! persistence layer: wal framing, snapshots, crash recovery
use super::*;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};




// recovery: load snap + replay wal tail (lsn > snap_lsn).
// stops on first torn/corrupted record (normal after kill -9)
pub fn recover(dir: &Path) -> std::io::Result<(Db, u64)> {
    fs::create_dir_all(dir)?;
    let mut db = Db::new();
    let mut snap_lsn = 0u64;
    let mut highest = 0u64;

    let snap = dir.join("snapshot");
    if snap.exists()
        && let Some(l) = load_snapshot(&fs::read(&snap)?, &mut db) {
            snap_lsn = l;
            highest = l;
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
