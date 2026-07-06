use std::time::Duration;
use anyhow::Context;
use raw_shared_types::{Request, Resp};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc as async_mpsc;
use super::RESP_BOUND;

pub async fn start_server(port: u16, item_tx: tokio::sync::mpsc::Sender<Request>) {
    #[cfg(feature = "allocator_debug")]
    tokio::spawn(async {
        let mut last = super::alloc_stat::snapshot();
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let now = super::alloc_stat::snapshot();
            eprintln!(
                "[alloc stats 2s] allocs +{:<9} reallocs +{:<5} frees +{:<9} live {:<8}",
                now.allocs - last.allocs,
                now.reallocs - last.reallocs,
                now.frees - last.frees,
                now.allocs as i64 - now.frees as i64,
            );
            last = now;
        }
    });

    let listener = TcpListener::bind(("0.0.0.0", port)).await.expect("cannot bind TCP to this port");
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            continue  // todo: better handle it
        };

        let _ = sock.set_nodelay(true);
        tokio::spawn(
            handle_conn(sock, item_tx.clone())
        );
    }

    unreachable!()
}

// network ingress (tokio)
pub async fn handle_conn(sock: tokio::net::TcpStream, item_tx: async_mpsc::Sender<Request>) {
    let (rd, mut wr) = sock.into_split();
    let mut reader = BufReader::with_capacity(64 * 1024, rd);
    let (resp_tx, mut resp_rx) = async_mpsc::channel::<Resp>(RESP_BOUND);

    // WRITER
    tokio::spawn(async move {
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        while let Some(first) = resp_rx.recv().await {
            encode(&mut buf, &first);
            while let Ok(r) = resp_rx.try_recv() {
                encode(&mut buf, &r);
                if buf.len() >= 256 * 1024 {
                    break;
                }
            }
            if wr.write_all(&buf).await.is_err() {
                break;
            }
            buf.clear();
        }
    });

    // READER
    let mut len_buf = [0u8; 4];
    loop {
        if reader.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len == 0 || len > 8 * 1024 * 1024 {
            break;
        }
        let mut data = vec![0u8; len]; // todo: yeah alloc here, I see, I know... And idk how to fix it now... maybe BytesMut
        if reader.read_exact(&mut data).await.is_err() {
            break;
        }
        let req = Request {
            data,
            reply: resp_tx.clone(),
            resp: None,
        };
        if item_tx.send(req).await.is_err() {
            break;
        }
    }
}

#[inline]
fn encode(buf: &mut Vec<u8>, r: &Resp) {
    match r {
        Resp::Value(v) => {
            buf.extend_from_slice(&((1 + v.len()) as u32).to_le_bytes());
            buf.push(r.to_proto_code());
            buf.extend_from_slice(v);
        }

        Resp::Ok | Resp::Miss | Resp::UnknownOp => {
            buf.extend_from_slice(&1u32.to_le_bytes());
            buf.push(r.to_proto_code());
        }
    }
}
