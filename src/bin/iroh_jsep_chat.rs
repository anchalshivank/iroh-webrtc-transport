//! WebRTC data-channel chat: native **offerer** dials a browser that is waiting on `accept_jsep_signaling`.
//! JSEP runs over iroh QUIC (`QuicSignaling`); no WebSocket.
//!
//! Usage: `cargo run --bin static-server`, open http://127.0.0.1:8080/, choose “iroh QUIC — wait for peer”, Connect, then:
//! `cargo run --bin iroh-jsep-chat -- <browser-node-id-z32> <relay-url>`

use std::str::FromStr;

use anyhow::Context as _;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};
use iroh_webrtc_transport::{
    negotiate_dc_as_offerer, JSEP_SIGNALING_ALPN, QuicSignaling, Str0mPeer,
};
use tokio::io::{self, AsyncBufReadExt, BufReader};

const DC_LABEL: &str = "chat";

async fn dc_line_chat(peer: Str0mPeer) -> anyhow::Result<()> {
    let (user_tx, mut peer_rx) = peer.spawn_line_io_bridge();

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
                        user_tx.send(line.into_bytes()).map_err(|_| anyhow::anyhow!("str0m bridge closed"))?;
                    }
                }
            }
            chunk = peer_rx.recv() => {
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
    let peer = negotiate_dc_as_offerer(&mut sig, DC_LABEL)
        .await
        .context("WebRTC offer + data channel")?;

    dc_line_chat(peer).await?;
    Ok(())
}
