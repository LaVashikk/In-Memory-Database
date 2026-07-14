//! simple load generator: K connections x P pipeline depth
//!
//! usage: cargo bench -p server --bench loadgen -- [addr] [conns] [pipeline] [secs] [put_pct] [val_size]
//! defaults: 127.0.0.1:9000 16 64 10 50 64
//!
//! each conn runs in its own thread with blocking tcp.
//! flow: prefill keyspace -> barrier -> 1s warmup -> measure window.
//! note: latency is measured per ROUND (P requests). for true per-request p99, use pipeline=1.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const OP_GET: u8 = 0;
const OP_PUT: u8 = 1;

const KEYSPACE: u64 = 100_000;
const KEY_LEN: usize = 9; // "k" + 8 digits
const WARMUP: Duration = Duration::from_secs(1);

struct Cfg {
    addr: String,
    conns: usize,
    pipeline: usize,
    secs: u64,
    put_pct: u32,
    val_size: usize,
}

struct Shared {
    stop: AtomicBool,
    recording: AtomicBool,
    total_ops: AtomicU64,
    total_tx: AtomicU64,
    total_rx: AtomicU64,
    total_miss: AtomicU64,
}

// format "kNNNNNNNN" inline without allocations
#[inline]
fn write_key(buf: &mut [u8; KEY_LEN], mut n: u64) {
    for i in (1..KEY_LEN).rev() {
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
}

// wire format: [len:u32][op][klen:u32][key][val]
#[inline]
fn push_req(buf: &mut Vec<u8>, op: u8, key: &[u8], val: &[u8]) {
    let body = 1 + 4 + key.len() + val.len();
    buf.extend_from_slice(&(body as u32).to_le_bytes());
    buf.push(op);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(val);
}

#[inline]
fn xorshift(r: &mut u64) -> u64 {
    *r ^= *r << 13;
    *r ^= *r >> 7;
    *r ^= *r << 17;
    *r
}

// stateful response parser for the whole connection lifetime
struct FrameReader {
    buf: Vec<u8>,
    have: usize,
    pos: usize,
    misses: u64,
}

impl FrameReader {
    fn new() -> Self {
        Self {
            buf: vec![0u8; 1 << 20],
            have: 0,
            pos: 0,
            misses: 0,
        }
    }

    // reads exactly `count` responses, returns total bytes received
    fn read_frames(&mut self, sock: &mut TcpStream, mut count: usize) -> io::Result<u64> {
        let mut rx = 0u64;
        while count > 0 {
            // do we have a full frame in [pos..have]?
            if self.have - self.pos >= 4 {
                let len = u32::from_le_bytes(
                    self.buf[self.pos..self.pos + 4].try_into().unwrap(),
                ) as usize;
                if self.have - self.pos >= 4 + len {
                    if len >= 1 && self.buf[self.pos + 4] == 2 {
                        self.misses += 1; // Resp::Miss - prefill sanity check
                    }
                    self.pos += 4 + len;
                    count -= 1;
                    continue;
                }
            }
            
            // shift remainder to the front if needed
            if self.pos > 0 {
                self.buf.copy_within(self.pos..self.have, 0);
                self.have -= self.pos;
                self.pos = 0;
            }
            
            if self.have == self.buf.len() {
                let new_len = self.buf.len() * 2;
                self.buf.resize(new_len, 0);
            }
            
            let n = sock.read(&mut self.buf[self.have..])?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "server closed"));
            }
            self.have += n;
            rx += n as u64;
        }
        Ok(rx)
    }
}

// connect and prefill our chunk of keyspace so GETs actually hit something
fn setup_conn(tid: usize, cfg: &Cfg) -> io::Result<(TcpStream, FrameReader)> {
    let mut sock = TcpStream::connect(&cfg.addr)?;
    sock.set_nodelay(true)?;
    let mut frames = FrameReader::new();

    let val = vec![b'v'; cfg.val_size];
    let mut keybuf = *b"k00000000";
    let mut sendbuf = Vec::with_capacity(cfg.pipeline * (16 + KEY_LEN + cfg.val_size));

    let start = tid as u64 * KEYSPACE / cfg.conns as u64;
    let end = (tid as u64 + 1) * KEYSPACE / cfg.conns as u64;
    let mut i = start;
    
    while i < end {
        sendbuf.clear();
        let n = (end - i).min(cfg.pipeline as u64) as usize;
        for j in 0..n as u64 {
            write_key(&mut keybuf, i + j);
            push_req(&mut sendbuf, OP_PUT, &keybuf, &val);
        }
        sock.write_all(&sendbuf)?;
        frames.read_frames(&mut sock, n)?;
        i += n as u64;
    }
    Ok((sock, frames))
}

fn worker(tid: usize, cfg: Arc<Cfg>, sh: Arc<Shared>, barrier: Arc<Barrier>) -> Vec<u64> {
    let setup = setup_conn(tid, &cfg);
    barrier.wait(); // wait for everyone to finish prefill regardless of errors

    let (mut sock, mut frames) = match setup {
        Ok(x) => x,
        Err(e) => {
            eprintln!("conn {tid}: setup failed: {e}");
            return Vec::new();
        }
    };

    let val = vec![b'v'; cfg.val_size];
    let mut keybuf = *b"k00000000";
    let mut sendbuf = Vec::with_capacity(cfg.pipeline * (16 + KEY_LEN + cfg.val_size));
    let mut lats = Vec::with_capacity(1 << 16);
    let (mut ops, mut tx, mut rx) = (0u64, 0u64, 0u64);

    // non-zero seed, different per thread
    let mut rng: u64 = 0x9e37_79b9_7f4a_7c15 ^ (tid as u64 + 1).wrapping_mul(0xd1b5_4a32_d192_ed03);

    while !sh.stop.load(Relaxed) {
        let rec = sh.recording.load(Relaxed);

        // buffer P requests (outside the latency measurement)
        sendbuf.clear();
        for _ in 0..cfg.pipeline {
            let r = xorshift(&mut rng);
            write_key(&mut keybuf, r % KEYSPACE);
            if ((r >> 32) as u32 % 100) < cfg.put_pct {
                push_req(&mut sendbuf, OP_PUT, &keybuf, &val);
            } else {
                push_req(&mut sendbuf, OP_GET, &keybuf, &[]);
            }
        }

        let t0 = Instant::now();
        if sock.write_all(&sendbuf).is_err() {
            break;
        }
        let got = match frames.read_frames(&mut sock, cfg.pipeline) {
            Ok(n) => n,
            Err(_) => break,
        };
        let dt = t0.elapsed().as_nanos() as u64;

        if rec {
            lats.push(dt); // this is latency for the whole batch of P requests
            ops += cfg.pipeline as u64;
            tx += sendbuf.len() as u64;
            rx += got;
        }
    }

    sh.total_ops.fetch_add(ops, Relaxed);
    sh.total_tx.fetch_add(tx, Relaxed);
    sh.total_rx.fetch_add(rx, Relaxed);
    sh.total_miss.fetch_add(frames.misses, Relaxed);
    lats
}

fn main() {
    let a: Vec<String> = std::env::args().filter(|arg| arg != "--bench").collect();
    let cfg = Arc::new(Cfg {
        addr: a.get(1).cloned().unwrap_or_else(|| "127.0.0.1:9000".into()),
        conns: a.get(2).and_then(|s| s.parse().ok()).unwrap_or(16),
        pipeline: a.get(3).and_then(|s| s.parse().ok()).unwrap_or(64),
        secs: a.get(4).and_then(|s| s.parse().ok()).unwrap_or(10),
        put_pct: a.get(5).and_then(|s| s.parse().ok()).unwrap_or(50),
        val_size: a.get(6).and_then(|s| s.parse().ok()).unwrap_or(64),
    });
    assert!(cfg.conns >= 1 && cfg.pipeline >= 1, "conns/pipeline must be >= 1");

    // dead-lock protection: request + response batches must fit into socket buffers
    let batch_out = cfg.pipeline * (4 + 1 + 4 + KEY_LEN + cfg.val_size);
    let batch_in = cfg.pipeline * (4 + 1 + cfg.val_size);
    if batch_out.max(batch_in) > 128 * 1024 {
        eprintln!(
            "warning: batch req/resp = {batch_out}/{batch_in} B > 128 KiB - \
            potential read/write deadlock, consider lowering pipeline or val_size"
        );
    }

    eprintln!(
        "bench: addr={} conns={} pipeline={} secs={} put%={} val={}B keyspace={}",
        cfg.addr, cfg.conns, cfg.pipeline, cfg.secs, cfg.put_pct, cfg.val_size, KEYSPACE
    );

    let sh = Arc::new(Shared {
        stop: AtomicBool::new(false),
        recording: AtomicBool::new(false),
        total_ops: AtomicU64::new(0),
        total_tx: AtomicU64::new(0),
        total_rx: AtomicU64::new(0),
        total_miss: AtomicU64::new(0),
    });
    let barrier = Arc::new(Barrier::new(cfg.conns + 1));

    let mut handles = Vec::new();
    for tid in 0..cfg.conns {
        let (cfg, sh, barrier) = (cfg.clone(), sh.clone(), barrier.clone());
        handles.push(thread::spawn(move || worker(tid, cfg, sh, barrier)));
    }

    barrier.wait(); // all connections finished prefilling
    eprintln!("prefill done ({KEYSPACE} keys), warmup {WARMUP:?} ...");
    thread::sleep(WARMUP);

    sh.recording.store(true, Relaxed);
    let t0 = Instant::now();
    thread::sleep(Duration::from_secs(cfg.secs));
    let elapsed = t0.elapsed().as_secs_f64(); // measurement window, ignores warmup/join times
    sh.stop.store(true, Relaxed);

    let mut lat = Vec::new();
    for h in handles {
        lat.extend(h.join().unwrap());
    }
    lat.sort_unstable();
    let pct = |p: f64| -> f64 {
        if lat.is_empty() { return 0.0; }
        let i = ((lat.len() as f64 - 1.0) * p).round() as usize;
        lat[i] as f64 / 1000.0 // convert ns to us
    };

    let ops = sh.total_ops.load(Relaxed);
    let tx = sh.total_tx.load(Relaxed);
    let rx = sh.total_rx.load(Relaxed);
    let miss = sh.total_miss.load(Relaxed);

    println!("\n--- RESULT ---");
    println!("window          {elapsed:.2} s");
    println!("total ops       {ops}");
    println!("throughput      {:.0} ops/sec", ops as f64 / elapsed);
    println!(
        "net TX / RX     {:.1} / {:.1} MiB/sec",
        tx as f64 / elapsed / 1048576.0,
        rx as f64 / elapsed / 1048576.0
    );
    println!("get misses      {miss}");
    println!("--- latency per round (round = {} requests) ---", cfg.pipeline);
    println!("--- (for true per-request p99 run with pipeline=1) ---");
    println!("p50             {:>10.1} us", pct(0.50));
    println!("p90             {:>10.1} us", pct(0.90));
    println!("p99             {:>10.1} us", pct(0.99));
    println!("p999            {:>10.1} us", pct(0.999));
    println!("max             {:>10.1} us", pct(1.0));
    println!("rounds          {}", lat.len());
}
