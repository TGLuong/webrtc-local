use std::fmt::Debug;

use bytes::Bytes;

#[derive(Clone)]
pub struct RtpPacket {
    pub sequence: u16,
    pub timestamp: u32,
    pub marker: bool,
    pub payload: Bytes,
}

impl Debug for RtpPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtpPacket")
            .field("sequence", &self.sequence)
            .field("timestamp", &self.timestamp)
            .field("marker", &self.marker)
            .field("payload", &format!("{:02x?}", self.payload.to_vec()))
            .finish()
    }
}
