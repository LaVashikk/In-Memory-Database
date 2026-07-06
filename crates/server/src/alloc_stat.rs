// TODO: stuff for counting allocator to make sure we dont realloc buffers in s2-3 state
use std::{alloc::{GlobalAlloc, Layout, System}, sync::atomic::{AtomicU64, Ordering::Relaxed}};

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static FREES: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

pub struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(l.size() as u64, Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        FREES.fetch_add(1, Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        REALLOCS.fetch_add(1, Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Stats {
    pub allocs: u64,
    pub reallocs: u64,
    pub frees: u64,
    pub bytes: u64,
}
pub fn snapshot() -> Stats {
    Stats {
        allocs: ALLOCS.load(Relaxed),
        reallocs: REALLOCS.load(Relaxed),
        frees: FREES.load(Relaxed),
        bytes: BYTES.load(Relaxed),
    }
}
