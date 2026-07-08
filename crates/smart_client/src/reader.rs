//! Frame reader state machine for length-prefixed TCP responses.
//!
//! Pure state machine - no I/O. The caller feeds raw bytes via `feed()`,
//! then pulls decoded frames via `try_extract()`. Works identically for
//! sync and async callers; the only difference is how bytes are obtained.

use raw_shared_types::{Resp, rd_u32};

const FRAME_HDR: usize = 4; // u32 LE length prefix

pub struct FrameReader {
    buf: Vec<u8>,
}

impl FrameReader {
    pub fn new() -> Self {
        Self { buf: Vec::with_capacity(4096) }
    }

    /// Append raw bytes received from the socket into the internal buffer.
    #[inline]
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Try to extract one complete response frame from buffered data.
    /// Returns `Some(Resp)` if a full frame is available, `None` if more
    /// bytes are needed. Call repeatedly after `feed()` to drain all
    /// complete frames before doing the next read.
    pub fn try_extract(&mut self) -> Option<Resp> {
        if self.buf.len() < FRAME_HDR {
            return None;
        }

        let body_len = rd_u32(&self.buf[..FRAME_HDR]);
        let total = FRAME_HDR + body_len;
        if self.buf.len() < total {
            return None;
        }

        let resp = Resp::from_proto_code(&self.buf[FRAME_HDR..total]);
        self.buf.drain(..total);

        Some(resp)
    }
}
