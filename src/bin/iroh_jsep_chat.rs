//! WebRTC data-channel chat: native **offerer** dials a browser that is waiting on `accept_jsep_signaling`.
//! JSEP runs over iroh QUIC (`QuicSignaling`); no WebSocket.
//!
//! Usage: `cargo run --bin static-server`, open http://127.0.0.1:8080/, choose “iroh QUIC”, Connect, then:
//! `cargo run --bin iroh-jsep-chat -- <browser-node-id-z32> <relay-url>`

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context as _;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};
use iroh_webrtc_transport::{
    negotiate_dc_as_offerer, JSEP_SIGNALING_ALPN, QuicSignaling,
};
use tokio::io::{self, AsyncBufReadExt, BufReader};
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;

const DC_LABEL: &str = "chat";

async fn dc_line_chat(dc: Arc<RTCDataChannel>) -> anyhow::Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    dc.on_message(Box::new(move |msg: DataChannelMessage| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(msg.data.to_vec());
        })
    }));

    println!("Chat ready on WebRTC data channel `{DC_LABEL}`. Empty line exits.\n");
    let mut stdin = BufReader::new(io::stdin()).lines();
    loop {
        tokio::select! {
            line = stdin.next_line() => {
                match line.context("stdin")? {
                    None => break,
                    Some(l) if l.is_empty() => break,
                    Some(l) => {
                        let mut line = l;
                        line.push('\n');
                        dc.send_text(line).await.context("data channel send")?;
                    }
                }
            }
            chunk = rx.recv() => {
                let Some(chunk) = chunk else { break };
                let text = String::from_utf8_lossy(&chunk);
                print!("[peer] {text}");
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let node_id_str = args.next().context(
        "Usage: iroh-jsep-chat <browser-node-id> <relay-url>\nExample relay: https://use1-1.relay.iroh.network./",
    )?;
    let relay_str = args.next().context("missing <relay-url>")?;

    let pk = PublicKey::from_str(&node_id_str).context("parse browser node id")?;
    let relay: RelayUrl = relay_str.parse().context("parse relay URL")?;
    let addr = EndpointAddr::new(pk).with_relay_url(relay);

    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![JSEP_SIGNALING_ALPN.to_vec()])
        .bind()
        .await
        .context("bind endpoint")?;

    endpoint.online().await;

    println!("Dialing browser {pk} for JSEP…");
    let conn = endpoint
        .connect(addr, JSEP_SIGNALING_ALPN)
        .await
        .context("connect for JSEP")?;
    let (send, recv) = conn.open_bi().await.context("open_bi signaling")?;

    let mut sig: QuicSignaling = QuicSignaling::new(send, recv);
    let (_pc, dc) = negotiate_dc_as_offerer(&mut sig, DC_LABEL)
        .await
        .context("WebRTC offer + data channel")?;

    dc_line_chat(dc).await?;
    Ok(())
}
