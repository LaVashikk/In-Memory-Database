use std::{fs, io::Write, path::Path, process::Child};

use wire::{rd_u32, rd_u64};

use crate::db::Db;

pub const SNAPSHOT_MAGIC: u32 = u32::from_le_bytes(*b"SNAP");

// crash-safe snapshot: write tmp -> fdatasync -> atomic rename -> sync parent dir
// FORMAT: "[magic][start-lsn][len] | ( [k-len][key][v-len][val] )+"
pub struct Snapshotter {
    running: Option<Child>,
    last_boundary_lsn: u64,
    min_lsn_gap: u64, // reject if `N` LSNs haven't passed since the last snapshot
}

pub enum Begin {
    Started,
    RejectReason // todo io or libc error?
}
pub enum Outcome {
    Committed { boundary_lsn: u64 },
    Failed
}

pub fn load(bytes: &[u8], db: &mut Db) -> Option<u64> {
    if bytes.len() < 20 || rd_u32(&bytes[0..4]) as u32 != SNAPSHOT_MAGIC {
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
        p += val_len;

        db.insert_raw(key, val);

        // OUTDATED CODE:
        // let key_len = rd_u32(&bytes[p..p + 4]);
        // let key = &bytes[p+4 .. p+4+key_len];
        // let val_len = rd_u32(&bytes[p+4+key_len .. p+4+key_len+4]);
        // let val = &bytes[p+4+key_len+4 .. p+4+key_len+4+val_len];
    }

    Some(lsn)
}

impl Snapshotter {
    pub fn new(start_lsn: u64, min_lsn_gap: u64) -> Self {
        Self {
            running: None,
            last_boundary_lsn: start_lsn,
            min_lsn_gap,
        }
    }

    // temporary code
    pub fn write_snapshot_sync_TEMP_FUNC(&mut self, db: &Db, dir: &Path, lsn: u64) -> std::io::Result<bool> {
        if (lsn - self.last_boundary_lsn) <= self.min_lsn_gap {
            return Ok(false)
        }

        let tmp = dir.join("snapshot.tmp");
        {
            let f = fs::File::create(&tmp)?;
            let mut w = std::io::BufWriter::new(f);

            // file start
            w.write_all(&SNAPSHOT_MAGIC.to_le_bytes())?;
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

        self.last_boundary_lsn = lsn;

        Ok(true)
    }

    /// Fork a snapshot child at the current applied LSN
    /// Rejects if one is already running or the cooldown gap is not met
    pub fn try_begin(&mut self, db_ref: &Db, applied_lsn: u64) -> Begin {
        todo!()
    }

    /// Non-blocking child check: waitpid(WNOHANG) + result validation +
    /// tmp->final rename. Call once per stage-2 loop iteration; costs one
    /// syscall while a child is running, nothing otherwise!
    pub fn poll(&mut self) -> Option<Outcome> {
        todo!()
    }
}
