use std::{collections::HashMap, sync::Arc};

use crate::{Batch, OP_GET, OP_PUT, Resp, persist};

#[derive(Default)]
pub struct Db {
    // todo: this is a random pointers... maybe use something like arena idduno
    map: HashMap<Box<[u8]>, Arc<[u8]>>, // TODO: hashmap is not a perfect solution... SipHash is slow
    //but it's for this POC pretty fine
    // more todo:"Adaptive Radix Tree (ART) or maybe Concurrent B-Tree / SkipList"
}

impl Db {
    pub fn new() -> Self {
        Self {
            map: HashMap::with_capacity(1 << 20),  // todo: this also need some capacity, what if it ended?
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    // raw insert without WAL
    #[inline]
    pub fn insert_raw(&mut self, k: &[u8], v: &[u8]) {
        self.map.insert(k.into(), Arc::from(v));
    }

    #[inline]
    pub fn remove_raw(&mut self, k: &[u8]) -> Option<Arc<[u8]>> {
        self.map.remove(k)
    }

    #[inline]
    pub fn remove_entry(&mut self, k: &[u8]) -> Option<(Box<[u8]>, Arc<[u8]>)> {
        self.map.remove_entry(k)
    }

    // #[inline]
    // pub fn get(&mut self, k: &[u8]) {
    // }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.map.iter().map(|(k, v)| (k.as_ref(), v.as_ref()))
    }

    // apply batch in place. lsn is 1-based monotonic counter
    pub fn apply(&mut self, batch: &mut Batch, lsn: &mut u64) {
        let Batch { items, out, lsn_hi } = batch;
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
                    let id = *lsn;
                    *lsn += 1;
                    *lsn_hi = id; // lsn_hi is the last 'changing' lsn
                    persist::encode_put(out, id, &req.data); // format redo byte stream.
                    self.map.insert(key.into(), Arc::from(val));
                    req.resp = Some(Resp::Ok);
                }

                // TODO: add remove

                _ => req.resp = Some(Resp::UnknownOp),
            }
        }
    }
}
