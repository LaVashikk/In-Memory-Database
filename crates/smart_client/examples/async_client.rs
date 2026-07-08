// Async example: put a few keys concurrently, then get them back.
//
// Run with:
//   cargo run --example async_client --features tokio
//
// Make sure the server is up:
//   cargo run -- --port 9000

use smart_client::{AsyncClient, Resp, async_client};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = "127.0.0.1:9000";
    println!("Connecting to {addr} …");
    let client: AsyncClient = async_client::connect(addr).await?;
    println!("Connected!\n");

    // Sequential PUTs
    let pairs: &[(&[u8], &[u8])] = &[
        (b"async_key_1", b"hello from async"),
        (b"async_key_2", b"tokio is cool"),
        (b"async_key_3", b"rust ftw"),
    ];

    for (k, v) in pairs {
        let resp = client.put(k, v).await?;
        println!(
            "PUT {:?}  →  {resp:?}",
            String::from_utf8_lossy(k)
        );
    }

    println!();

    // Concurrent GETs (all in-flight at the same time)
    // Server executes sequentially, but we issue all requests without awaiting
    // each one - the writer task queues them up and the reader task delivers
    // responses in FIFO order to the right oneshot channels.
    println!("Firing concurrent GETs …");
    let handles: Vec<_> = pairs
        .iter()
        .map(|(k, _)| {
            let client = client.clone();
            let key = k.to_vec();
            tokio::spawn(async move { client.get(&key).await })
        })
        .collect();

    for (handle, (k, _)) in handles.into_iter().zip(pairs.iter()) {
        let resp = handle.await??;
        match &resp {
            Resp::Value(val) => println!(
                "GET {:?}  →  {:?}",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(val)
            ),
            other => println!("GET {:?}  →  {other:?}", String::from_utf8_lossy(k)),
        }
    }

    println!();

    // Miss
    let resp = client.get(b"nope").await?;
    println!("GET 'nope'  →  {resp:?}  (expected Miss)");

    Ok(())
}
