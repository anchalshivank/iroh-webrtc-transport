//! WebRTC data-channel line chat over the JSEP WebSocket relay (`signaling-server`).
//!
//! Usage: `cargo run --bin signaling-server` then `cargo run --bin ws-chat -- ws://127.0.0.1:3000/ws demo`

use std::sync::Arc;

use anyhow::Context as _;
use futures_util::{SinkExt, StreamExt};
use iroh_webrtc_transport::{
    negotiate_dc_as_answerer, negotiate_dc_as_offerer, TcpWebSocket,
};
use serde_json::Value;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;

const DC_LABEL: &str = "chat";

#[derive(Clone, Copy)]
enum Role {
    Offer,
    Answer,
}

async fn read_assigned(ws: &mut TcpWebSocket) -> anyhow::Result<Role> {
    let msg = ws
        .next()
        .await
        .context("websocket closed before assigned")?
        .map_err(|e| anyhow::anyhow!("ws: {e}"))?;
    let text = match msg {
        WsMessage::Text(t) => t.to_string(),
        WsMessage::Binary(b) => String::from_utf8(b.to_vec()).context("assigned utf-8")?,
        _ => anyhow::bail!("unexpected ws frame before assigned"),
    };
    let v: Value = serde_json::from_str(text.trim())?;
    match v.get("cmd").and_then(|c| c.as_str()) {
        Some("error") => anyhow::bail!(
            "server: {}",
            v.get("message").and_then(|m| m.as_str()).unwrap_or("error")
        ),
        Some("assigned") => match v.get("role").and_then(|r| r.as_str()) {
            Some("offer") => Ok(Role::Offer),
            Some("answer") => Ok(Role::Answer),
            _ => anyhow::bail!("assigned without role"),
        },
        _ => anyhow::bail!("expected {{\"cmd\":\"assigned\",...}}, got {text}"),
    }
}

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
    let url = args.next().context(
        "Usage: ws-chat <ws-url> <room>\nExample: ws-chat ws://127.0.0.1:3000/ws demo",
    )?;
    let room = args.next().context("missing <room>")?;

    let (mut ws, _) = connect_async(&url)
        .await
        .with_context(|| format!("connect to {url}"))?;

    let join = serde_json::json!({ "cmd": "join", "room": room }).to_string();
    ws.send(WsMessage::Text(join.into())).await?;

    let role = read_assigned(&mut ws).await?;
    let (_pc, dc) = match role {
        Role::Offer => negotiate_dc_as_offerer(&mut ws, DC_LABEL).await?,
        Role::Answer => negotiate_dc_as_answerer(&mut ws).await?,
    };

    dc_line_chat(dc).await?;
    Ok(())
}
