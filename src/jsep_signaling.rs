//! Transport abstraction for JSEP: move [`SignalEnvelope`] between peers without tying negotiation to QUIC or WebSocket.

use async_trait::async_trait;

use crate::jsep_envelope::SignalEnvelope;

/// Sends and receives JSON [`SignalEnvelope`] values. Framing is defined by each implementation
/// ([`crate::QuicSignaling`], [`crate::TcpWebSocket`], etc.).
#[async_trait]
pub trait Signaling: Send {
    async fn send_envelope(&mut self, env: &SignalEnvelope) -> anyhow::Result<()>;
    async fn recv_envelope(&mut self) -> anyhow::Result<SignalEnvelope>;
}
