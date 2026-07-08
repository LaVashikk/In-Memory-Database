//! Async smart client (tokio).

use std::sync::Arc;

use anyhow::{Result, anyhow};
use raw_shared_types::{Operation, Resp, encode_raw};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, mpsc, oneshot},
};

use crate::reader::FrameReader;

// -- internal types --

struct Call {
    op:    Operation,
    key:   Box<[u8]>,
    value: Option<Box<[u8]>>,
    tx:    oneshot::Sender<Result<Resp>>,
}

type FifoTx = mpsc::UnboundedSender<oneshot::Sender<Result<Resp>>>;
type FifoRx = mpsc::UnboundedReceiver<oneshot::Sender<Result<Resp>>>;

fn fifo_channel() -> (FifoTx, FifoRx) { mpsc::unbounded_channel() }

type RxShared = Arc<Mutex<mpsc::Receiver<Call>>>;

// -- public api --

#[derive(Clone)]
pub struct AsyncClient {
    to_writer: mpsc::Sender<Call>,
}

impl AsyncClient {
    pub async fn get(&self, key: &[u8]) -> Result<Resp> {
        self.call(Operation::Get, key, None).await
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<Resp> {
        self.call(Operation::Put, key, Some(value)).await
    }

    async fn call(&self, op: Operation, key: &[u8], value: Option<&[u8]>) -> Result<Resp> {
        let (tx, rx) = oneshot::channel::<Result<Resp>>();
        self.to_writer
            .send(Call { op, key: key.into(), value: value.map(Into::into), tx })
            .await
            .map_err(|_| anyhow!("writer task is gone"))?;
        rx.await.map_err(|_| anyhow!("reader task is gone"))?
    }
}

/// Connect to `addr`, spawn writer + reader tasks with auto-reconnect.
pub async fn connect<A>(addr: A) -> Result<AsyncClient>
where
    A: tokio::net::ToSocketAddrs + Clone + Send + Sync + 'static,
    for<'a> &'a A: std::fmt::Debug,
{
    let (tx, rx) = mpsc::channel::<Call>(512);
    let rx_shared = Arc::new(Mutex::new(rx));

    let stream = tokio::net::TcpStream::connect(addr.clone()).await?;
    stream.set_nodelay(true)?;
    tokio::spawn(supervisor(addr, stream, rx_shared));

    Ok(AsyncClient { to_writer: tx })
}

// -- supervisor (reconnect loop) --
async fn supervisor<A>(addr: A, initial: tokio::net::TcpStream, rx: RxShared)
where
    A: tokio::net::ToSocketAddrs + Clone + Send + Sync + 'static,
    for<'a> &'a A: std::fmt::Debug,
{
    let mut stream = initial;
    let mut delay = std::time::Duration::from_millis(100);

    loop {
        let (read_half, write_half) = tokio::io::split(stream);
        let (fifo_tx, fifo_rx) = fifo_channel();

        tokio::select! {
            _ = writer_task(write_half, Arc::clone(&rx), fifo_tx) => {}
            _ = reader_task(read_half, fifo_rx) => {}
        }

        stream = loop {
            eprintln!("[smart_client/async] reconnecting to {:?} in {delay:?}…", &addr);
            tokio::time::sleep(delay).await;
            match tokio::net::TcpStream::connect(addr.clone()).await {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    delay = std::time::Duration::from_millis(100);
                    eprintln!("[smart_client/async] reconnected");
                    break s;
                }
                Err(e) => {
                    eprintln!("[smart_client/async] reconnect failed: {e}");
                    delay = (delay * 2).min(std::time::Duration::from_secs(5));
                }
            }
        };
    }
}

// -- writer task --
async fn writer_task<W>(mut sock: W, rx: RxShared, fifo_tx: FifoTx)
where W: AsyncWriteExt + Unpin,
{
    let mut write_buf = Vec::<u8>::with_capacity(64 * 1024);

    loop {
        let batch = {
            let mut guard = rx.lock().await;
            let first = match guard.recv().await {
                Some(c) => c,
                None    => return,
            };
            let mut batch = vec![first];
            while let Ok(c) = guard.try_recv() { batch.push(c); }
            batch
        };

        write_buf.clear();
        for call in batch {
            encode_raw(&mut write_buf, call.op, &call.key, call.value.as_deref());
            let _ = fifo_tx.send(call.tx);
        }

        if let Err(e) = sock.write_all(&write_buf).await {
            eprintln!("[smart_client/async] writer: {e}");
            return;
        }
        if let Err(e) = sock.flush().await {
            eprintln!("[smart_client/async] writer flush: {e}");
            return;
        }
    }
}

// -- reader task --
async fn reader_task<R>(mut read_half: R, mut fifo_rx: FifoRx)
where R: AsyncReadExt + Unpin,
{
    let mut framer = FrameReader::new();
    let mut tmp = [0u8; 8192];

    loop {
        let n = match read_half.read(&mut tmp).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        framer.feed(&tmp[..n]);

        while let Some(resp) = framer.try_extract() {
            match fifo_rx.recv().await {
                Some(tx) => { let _ = tx.send(Ok(resp)); }
                None     => return,
            }
        }
    }

    while let Ok(tx) = fifo_rx.try_recv() {
        let _ = tx.send(Err(anyhow!("connection lost")));
    }
}
