// Sync example: put a few keys, then get them back.
//
// Run with:
//   cargo run --example sync_client
//
// Make sure the server is up:
//   cargo run -- --port 9000

use smart_client::{Client, Resp};

fn main() -> anyhow::Result<()> {
    let addr = "127.0.0.1:9000";
    println!("Connecting to {addr} …");
    let client = Client::connect(addr)?;
    println!("Connected!\n");

    // PUT a bunch of keys
    let pairs: &[(&[u8], &[u8])] = &[
        (b"name",    b"lavashik"),
        (b"project", b"in-memory-poc"),
        (b"lang",    b"rust"),
        (b"mood",    b"hype"),
    ];

    for (k, v) in pairs {
        let resp = client.put(k, v)?;
        println!("PUT {:?} = {:?}  →  {resp:?}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
    }

    println!();

    // GET them back
    for (k, _) in pairs {
        let resp = client.get(k)?;
        match &resp {
            Resp::Value(val) => println!("GET {:?}  →  {:?}", String::from_utf8_lossy(k), String::from_utf8_lossy(val)),
            other           => println!("GET {:?}  →  {other:?}", String::from_utf8_lossy(k)),
        }
    }

    println!();

    // GET a key that doesn't exist
    let resp = client.get(b"does_not_exist")?;
    println!("GET 'does_not_exist'  →  {resp:?}  (expected Miss)");

    Ok(())
}
