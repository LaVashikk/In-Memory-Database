// Throughput benchmark - measures max PUT and GET ops/sec.
//
// Run:
//   cargo run --example bench -p smart_client --features tokio --release
//
// Optional env vars:
//   BENCH_ADDR     = 127.0.0.1:9000   server address
//   BENCH_SECS     = 5                seconds per phase
//   BENCH_CONC     = 32               concurrent tasks per phase
//   BENCH_VAL_SIZE = 64               value size in bytes for PUTs

use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use smart_client::async_client;

fn cfg(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn print_header(phase: &str, conc: usize, secs: u64) {
    println!(
        "\n┌─ {phase} ─── concurrency={conc}  duration={secs}s ──────────────────┐"
    );
}

fn print_result(ops: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = ops as f64 / secs;
    println!("│  ops total : {ops}");
    println!("│  elapsed   : {:.3}s", secs);
    println!("│  throughput: {:.0} ops/sec", ops_per_sec);
    println!("└────────────────────────────────────────────────────────────────┘");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: String  = cfg("BENCH_ADDR",     "127.0.0.1:9000");
    let secs: u64     = cfg("BENCH_SECS",     "5").parse()?;
    let conc: usize   = cfg("BENCH_CONC",     "32").parse()?;
    let val_sz: usize = cfg("BENCH_VAL_SIZE", "64").parse()?;

    let duration = Duration::from_secs(secs);
    let value    = vec![b'x'; val_sz];
    // Fixed key pool - large enough to avoid hot-key artifacts on GET.
    let key_count: u64 = 10_000;

    println!("══════════════════════════════════════════════════════════════════");
    println!("  smart_client throughput bench");
    println!("  addr={addr}  secs={secs}  concurrency={conc}  val={val_sz}B");
    println!("══════════════════════════════════════════════════════════════════");

    let client = async_client::connect(addr.clone()).await?;
    println!("\nConnected to {addr}");

    // WARMUP: pre-populate keys so GETs don't all Miss
    print!("Warming up ({key_count} keys) … ");
    let value_arc = Arc::new(value.clone());
    let mut handles = Vec::with_capacity(conc);
    let keys_per_task = key_count / conc as u64;

    for t in 0..conc {
        let c = client.clone();
        let v = Arc::clone(&value_arc);
        let start_key = t as u64 * keys_per_task;
        let end_key   = start_key + keys_per_task;
        handles.push(tokio::spawn(async move {
            for i in start_key..end_key {
                let key = format!("bench:{i:08}");
                c.put(key.as_bytes(), &v).await?;
            }
            anyhow::Ok(())
        }));
    }
    for h in handles {
        h.await??;
    }
    println!("done.");

    // PUT BENCH
    print_header("PUT", conc, secs);
    let put_ops = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + duration;
    let mut handles = Vec::with_capacity(conc);

    for t in 0..conc {
        let c        = client.clone();
        let v        = Arc::clone(&value_arc);
        let counter  = Arc::clone(&put_ops);
        // Each task cycles through its own slice of the key space to avoid
        // contention on a single key (which would be unrealistically fast).
        let base_key = (t as u64 * keys_per_task) % key_count;
        handles.push(tokio::spawn(async move {
            let mut i = 0u64;
            while Instant::now() < deadline {
                let key = format!("bench:{:08}", (base_key + i) % key_count);
                match c.put(key.as_bytes(), &v).await {
                    Ok(_)  => { counter.fetch_add(1, Relaxed); }
                    Err(e) => { eprintln!("put error: {e}"); break; }
                }
                i += 1;
            }
        }));
    }

    let t0 = Instant::now();
    for h in handles { h.await?; }
    let put_elapsed = t0.elapsed();
    print_result(put_ops.load(Relaxed), put_elapsed);

    // GET BENCH
    print_header("GET", conc, secs);
    let get_ops  = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + duration;
    let mut handles = Vec::with_capacity(conc);

    for t in 0..conc {
        let c       = client.clone();
        let counter = Arc::clone(&get_ops);
        let base_key = (t as u64 * keys_per_task) % key_count;
        handles.push(tokio::spawn(async move {
            let mut i = 0u64;
            while Instant::now() < deadline {
                let key = format!("bench:{:08}", (base_key + i) % key_count);
                match c.get(key.as_bytes()).await {
                    Ok(_)  => { counter.fetch_add(1, Relaxed); }
                    Err(e) => { eprintln!("get error: {e}"); break; }
                }
                i += 1;
            }
        }));
    }

    let t0 = Instant::now();
    for h in handles { h.await?; }
    let get_elapsed = t0.elapsed();
    print_result(get_ops.load(Relaxed), get_elapsed);

    // SUMMARY
    println!("\n══════════════════════════════════════════════════════════════════");
    println!("  PUT  {:.0} ops/sec", put_ops.load(Relaxed) as f64 / put_elapsed.as_secs_f64());
    println!("  GET  {:.0} ops/sec", get_ops.load(Relaxed) as f64 / get_elapsed.as_secs_f64());
    println!("══════════════════════════════════════════════════════════════════");

    Ok(())
}
