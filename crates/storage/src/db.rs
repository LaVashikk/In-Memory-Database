use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub struct Db {
    // todo: this is a random pointers... maybe use something like arena idduno,
    // or "Adaptive Radix Tree" or maybe "Concurrent B-Tree / SkipList"
    map: HashMap<Box<[u8]>, Arc<[u8]>, ahash::RandomState>,
    //but it's for this POC pretty fine
}

impl Db {
    pub fn new() -> Self {
        Self {
            map: HashMap::with_capacity_and_hasher(1 << 20, ahash::RandomState::new()),  // todo: this also need some capacity, what i should to use?
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    pub fn get(&self, k: &[u8]) -> Option<&Arc<[u8]>> {
        self.map.get(k)
    }

    /// In-place overwrite when the size matches and no reader holds the value;
    /// otherwise a fresh allocation replaces the slot
    #[inline]
    pub fn put(&mut self, k: &[u8], v: &[u8]) {
        match self.map.get_mut(k) {
            Some(slot) => {
                if slot.len() == v.len() && let Some(buf) = Arc::get_mut(slot) {
                    buf.copy_from_slice(v);
                }
            },
            None => { self.map.insert(k.into(), Arc::from(v)); } ,
        }
    }


    #[inline]
    pub fn remove(&mut self, k: &[u8]) -> Option<Arc<[u8]>> {
        self.map.remove(k)
    }

    #[inline]
    pub fn remove_entry(&mut self, k: &[u8]) -> Option<(Box<[u8]>, Arc<[u8]>)> {
        self.map.remove_entry(k)
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.map.iter().map(|(k, v)| (k.as_ref(), v.as_ref()))
    }

    // raw insert, only for recovery
    #[inline]
    pub(crate) fn insert_raw(&mut self, k: &[u8], v: &[u8]) {
        self.map.insert(k.into(), Arc::from(v));
    }
}

// --- debug stuff ---
impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct MapFmt<'a>(&'a std::collections::HashMap<Box<[u8]>, std::sync::Arc<[u8]>, ahash::RandomState>);

        impl std::fmt::Debug for MapFmt<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_map()
                    .entries(self.0.iter().map(|(k, v)| {
                        let key = std::str::from_utf8(k).unwrap_or("~NaN~");
                        let val = std::str::from_utf8(v).unwrap_or("~NaN~");
                        (key, val)
                    }))
                    .finish()
            }
        }

        f.debug_struct("Db")
            .field("map", &MapFmt(&self.map))
            .finish()
    }
}
