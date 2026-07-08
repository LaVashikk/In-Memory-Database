//! Blocking (sync) smart client.

use std::{
    io::{Read, Write}, net::{TcpStream, ToSocketAddrs}, sync::{Arc, atomic::{AtomicBool, Ordering}, mpsc}, thread, time::Duration,
};

use anyhow::{Context, Result};
use raw_shared_types::{Operation, Resp, encode_raw};

use crate::reader::FrameReader;

struct Call {
    op:    Operation,
    key:   Box<[u8]>,
    value: Option<Box<[u8]>>,
    tx:    mpsc::SyncSender<Result<Resp>>,
}

type ReplyTx = mpsc::SyncSender<Result<Resp>>;
type FifoTx  = mpsc::Sender<ReplyTx>;
type FifoRx  = mpsc::Receiver<ReplyTx>;

// -- public api --
#[derive(Clone)]
pub struct Client {
    tx: mpsc::SyncSender<Call>,
}

impl Client {
    pub fn connect<A>(addr: A) -> Result<Self>
    where A: ToSocketAddrs + ToString + Clone + Send + 'static,
    {
        let stream = TcpStream::connect(addr.clone())
            .with_context(|| format!("connecting to {}", addr.to_string()))?;
        stream.set_nodelay(true)?;

        let (tx, rx) = mpsc::sync_channel::<Call>(512);
        let addr_str = addr.to_string();

        thread::Builder::new()
            .name("smart_client::driver".into())
            .spawn(move || driver(stream, rx, addr, addr_str))
            .expect("spawn driver");

        Ok(Client { tx })
    }

    pub fn get(&self, key: &[u8]) -> Result<Resp> {
        self.call(Operation::Get, key, None)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<Resp> {
        self.call(Operation::Put, key, Some(value))
    }

    fn call(&self, op: Operation, key: &[u8], value: Option<&[u8]>) -> Result<Resp> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.tx
            .send(Call { op, key: key.into(), value: value.map(Into::into), tx })
            .map_err(|_| anyhow::anyhow!("client driver is dead"))?;
        rx.recv().map_err(|_| anyhow::anyhow!("client driver is dead"))?
    }
}

// -- driver: manages connection lifecycle + runs writer loop inline --
fn driver<A: ToSocketAddrs + Clone>(
    mut stream: TcpStream,
    rx: mpsc::Receiver<Call>,
    addr: A,
    addr_str: String,
) {
    let dead = Arc::new(AtomicBool::new(false));
    let mut write_buf = Vec::<u8>::with_capacity(64 * 1024);

    'reconnect: loop {
        let (mut sock, reader_stream) = match clone_pair(&stream) {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("[smart_client] clone failed: {e}");
                reconnect(&mut stream, &addr, &addr_str);
                continue;
            }
        };

        // init stuff
        dead.store(false, Ordering::Relaxed);
        let (fifo_tx, fifo_rx) = mpsc::channel::<ReplyTx>();
        let d = Arc::clone(&dead);
        let rdr = thread::Builder::new()
            .name("smart_client::reader".into())
            .spawn(move || read_loop(reader_stream, fifo_rx, d))
            .expect("spawn reader");

        // writer loop
        loop {
            let first = match rx.recv() {
                Ok(c)  => c,
                Err(_) => return, // all Client handles dropped
            };
            if dead.load(Ordering::Relaxed) {
                let _ = first.tx.send(Err(anyhow::anyhow!("connection lost")));
                drop(sock); drop(fifo_tx);
                reconnect(&mut stream, &addr, &addr_str);
                let _ = rdr.join();
                continue 'reconnect;
            }

            let mut batch = vec![first];
            while let Ok(c) = rx.try_recv() { batch.push(c); }

            write_buf.clear();
            for call in batch {
                encode_raw(&mut write_buf, call.op, &call.key, call.value.as_deref());
                let _ = fifo_tx.send(call.tx);
            }

            if let Err(e) = sock.write_all(&write_buf).and_then(|_| sock.flush()) {
                eprintln!("[smart_client] writer: {e}");
                dead.store(true, Ordering::Relaxed);
                drop(sock); drop(fifo_tx);
                reconnect(&mut stream, &addr, &addr_str);
                let _ = rdr.join();
                continue 'reconnect;
            }
        }
    }
}

fn clone_pair(stream: &TcpStream) -> std::io::Result<(TcpStream, TcpStream)> {
    Ok((stream.try_clone()?, stream.try_clone()?))
}

// -- reader --
fn read_loop(stream: TcpStream, fifo: FifoRx, dead: Arc<AtomicBool>) {
    let mut framer = FrameReader::new();
    let mut sock = std::io::BufReader::new(&stream);
    let mut tmp = [0u8; 8192];

    'outer: loop {
        let n = match sock.read(&mut tmp) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        framer.feed(&tmp[..n]);
        while let Some(resp) = framer.try_extract() {
            match fifo.recv() {
                Ok(tx) => { let _ = tx.send(Ok(resp)); }
                Err(_) => break 'outer,
            }
        }
    }

    dead.store(true, Ordering::Relaxed);
    while let Ok(tx) = fifo.try_recv() {
        let _ = tx.send(Err(anyhow::anyhow!("connection lost")));
    }
}

// -- reconnect --
fn reconnect<A: ToSocketAddrs + Clone>(stream: &mut TcpStream, addr: &A, addr_str: &str) {
    let mut delay = Duration::from_millis(100);
    loop {
        eprintln!("[smart_client] reconnecting to {addr_str} in {delay:?}…");
        thread::sleep(delay);
        match TcpStream::connect(addr.clone()) {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                *stream = s;
                eprintln!("[smart_client] reconnected to {addr_str}");
                return;
            }
            Err(e) => {
                eprintln!("[smart_client] reconnect failed: {e}");
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        }
    }
}
