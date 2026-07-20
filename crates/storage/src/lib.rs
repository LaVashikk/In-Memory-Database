pub mod db;
pub mod snapshot;
pub mod wal_format;
pub use db::Db;

use std::fs;
use std::path::Path;

/// Recovery: load snapshot, then replay WAL records with lsn > snap_lsn.
/// Stops at the first torn/corrupted record. Returns (db, next_lsn)
pub fn recover(dir: &Path) -> std::io::Result<(Db, u64)> {
    fs::create_dir_all(dir)?;
    let mut db = Db::new();
    let mut snap_lsn = 0u64;
    let mut highest = 0u64;

    let snap = dir.join("snapshot");
    if snap.exists()
        && let Some(l) = snapshot::load(&fs::read(&snap)?, &mut db) {
            snap_lsn = l;
            highest = l;
        }

    let (mut replayed, mut torn) = (0u64, false);
    'main: for (_start, path) in wal_format::list_segments(dir)? {
        let bytes = fs::read(&path)?;
        let mut p = 0usize;

        while p < bytes.len() {
            let Some(rec) = wal_format::decode_record(&bytes[p..]) else {
                torn = true;
                break 'main;
            };
            p += rec.consumed;

            if rec.lsn <= snap_lsn {
                continue;
            }

            // RESTORE OPERATIONS
            if wire::op(rec.wire_body) == wire::OP_PUT {
                let (k, v) = wire::split_kv(rec.wire_body);
                db.insert_raw(k, v);
            }

            highest = highest.max(rec.lsn);
            replayed += 1;
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
