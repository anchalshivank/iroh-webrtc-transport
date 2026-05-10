//! JSEP over a WebSocket (e.g. browser ↔ native, or any `tokio-tungstenite` client). One JSON [`SignalEnvelope`] per text frame.
//!
//! For room/bootstrap framing (join before JSEP), handle that on the socket first, then call
//! [`crate::negotiate_dc_as_offerer`] / [`crate::negotiate_dc_as_answerer`] on the same stream.

use anyhow::Context as _;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;

use crate::jsep_envelope::SignalEnvelope;
use crate::jsep_signaling::Signaling;

/// WebSocket after `connect_async` (plain TCP or TLS).
pub type TcpWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[async_trait]
impl Signaling for TcpWebSocket {
    async fn send_envelope(&mut self, env: &SignalEnvelope) -> anyhow::Result<()> {
        let text = serde_json::to_string(env).context("serialize envelope")?;
        self.send(Message::Text(text.into()))
            .await
            .map_err(|e| anyhow::anyhow!("ws send: {e}"))?;
        Ok(())
    }

    async fn recv_envelope(&mut self) -> anyhow::Result<SignalEnvelope> {
        loop {
            let msg = self
                .next()
                .await
                .context("websocket closed")?
                .map_err(|e| anyhow::anyhow!("ws recv: {e}"))?;
            match msg {
                Message::Text(t) => {
                    let env: SignalEnvelope =
                        serde_json::from_str(t.as_str()).context("parse SignalEnvelope")?;
                    return Ok(env);
                }
                Message::Binary(b) => {
                    let env: SignalEnvelope =
                        serde_json::from_slice(&b).context("parse SignalEnvelope from binary")?;
                    return Ok(env);
                }
                Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => anyhow::bail!("websocket close frame"),
                Message::Frame(_) => continue,
            }
        }
    }
}
