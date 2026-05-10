//! JSON SDP offer/answer payloads exchanged during JSEP (shared by QUIC, WebSocket, and custom [`crate::Signaling`] impls).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SignalEnvelope {
    Offer { sdp: String },
    Answer { sdp: String },
}
