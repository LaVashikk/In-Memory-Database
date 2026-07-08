//! `smart_client` — sync and async clients for the in-memory KV store.
//!
//! # Sync
//! ```no_run
//! let client = smart_client::Client::connect("127.0.0.1:9000").unwrap();
//! client.put(b"k", b"v").unwrap();
//! let resp = client.get(b"k").unwrap();
//! ```
//!
//! # Async (feature = "tokio")
//! ```no_run
//! # #[cfg(feature = "tokio")]
//! # async fn example() {
//! let client = smart_client::async_client::connect("127.0.0.1:9000").await.unwrap();
//! client.put(b"k", b"v").await.unwrap();
//! # }
//! ```

pub mod reader;
pub mod sync;
#[cfg(feature = "tokio")]
pub mod async_client;

pub use sync::Client;
#[cfg(feature = "tokio")]
pub use async_client::AsyncClient;

pub use raw_shared_types::{Operation, Resp, OP_GET, OP_PUT};
