//! Exchange WebRTC SDP (JSEP) over an iroh QUIC bidirectional stream.

use anyhow::Context as _;
use async_trait::async_trait;
use iroh::endpoint::{RecvStream, SendStream};
use tokio::io::AsyncWriteExt;

use crate::jsep_envelope::SignalEnvelope;
use crate::jsep_signaling::Signaling;

async fn read_line_signaling(recv: &mut RecvStream) -> anyhow::Result<String> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = recv
            .read(&mut byte)
            .await
            .map_err(|e| anyhow::anyhow!("recv read: {e:?}"))?;
        let Some(n) = n else {
            anyhow::bail!("signaling stream closed before newline");
        };
        if n == 0 {
            continue;
        }
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
    }
    String::from_utf8(line).context("signaling line utf-8")
}

/// JSEP transport: one JSON [`crate::SignalEnvelope`] per line (`\n` terminated) on a QUIC bidi stream.
pub struct QuicSignaling {
    send: SendStream,
    recv: RecvStream,
}

impl QuicSignaling {
    pub fn new(send: SendStream, recv: RecvStream) -> Self {
        Self { send, recv }
    }
}

#[async_trait]
impl Signaling for QuicSignaling {
    async fn send_envelope(&mut self, env: &SignalEnvelope) -> anyhow::Result<()> {
        let mut line = serde_json::to_string(env).context("serialize signaling")?;
        line.push('\n');
        self.send
            .write_all(line.as_bytes())
            .await
            .context("write signaling")?;
        self.send.flush().await.context("flush signaling")?;
        Ok(())
    }

    async fn recv_envelope(&mut self) -> anyhow::Result<SignalEnvelope> {
        let line = read_line_signaling(&mut self.recv).await?;
        let env: SignalEnvelope =
            serde_json::from_str(line.trim()).context("parse signaling JSON")?;
        Ok(env)
    }
}
