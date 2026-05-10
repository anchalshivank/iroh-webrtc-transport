use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::PublicKey;
use iroh::{
    Endpoint, EndpointAddr, RelayUrl, TransportAddr,
    endpoint::{
        presets,
        transports::{AddrKind, TransportBias},
        QuicTransportConfig, RecvStream, SendStream,
    },
};
use iroh_webrtc_transport::{
    AttachOptions, WebRtcTransport, WEBRTC_TRANSPORT_ID, custom_addr_from_opaque_data,
};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};

const SIGNALING_ALPN: &[u8] = b"iroh-webrtc-transport/signal/0";
const APP_ALPN: &[u8] = b"iroh-webrtc-transport/loopback/0";

const DC_LABEL: &str = "iroh-quic-signaled";

/// Server uses sixteen `0x01` bytes as its WebRtcTransport opaque custom address (see `server.rs`).
const SERVER_CUSTOM_OPAQUE: [u8; 16] = [1u8; 16];

fn quic_with_datagrams() -> QuicTransportConfig {
    QuicTransportConfig::builder()
        .datagram_receive_buffer_size(Some(256 * 1024))
        .datagram_send_buffer_size(256 * 1024)
        .build()
}

fn custom_bias() -> TransportBias {
    TransportBias::primary().with_rtt_advantage(std::time::Duration::from_millis(100))
}

fn mixed_server_addr(node_id: PublicKey, relay: RelayUrl) -> EndpointAddr {
    EndpointAddr::new(node_id)
        .with_relay_url(relay)
        .with_addrs([TransportAddr::Custom(custom_addr_from_opaque_data(
            &SERVER_CUSTOM_OPAQUE,
        ))])
}

async fn line_chat(mut send: SendStream, recv: RecvStream, peer_label: &str) -> Result<()> {
    let mut stdin = BufReader::new(io::stdin()).lines();
    let mut peer_lines = BufReader::new(recv).lines();

    println!("Chat ready. Lines you type go to the server; prefix `{peer_label}` shows their lines.");
    println!("Empty line or Ctrl+D exits.\n");

    loop {
        tokio::select! {
            line = stdin.next_line() => {
                match line.context("read stdin")? {
                    None => break,
                    Some(l) if l.is_empty() => break,
                    Some(l) => {
                        send.write_all(l.as_bytes()).await.context("chat send")?;
                        send.write_all(b"\n").await.context("chat send")?;
                        send.flush().await.context("chat send")?;
                    }
                }
            }
            line = peer_lines.next_line() => {
                match line.context("read peer line")? {
                    None => {
                        println!("(peer closed the stream)");
                        break;
                    }
                    Some(l) => println!("[{peer_label}] {l}"),
                }
            }
        }
    }

    let _ = send.finish();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let node_id_str = args
        .next()
        .context("Usage: client <node-id> <relay-url>\nRelay URL is printed by the server (same line as node id).")?;
    let relay_str = args.next().context(
        "Usage: client <node-id> <relay-url>\nExample relay: https://use1-1.relay.iroh.network./",
    )?;

    let node_id = PublicKey::from_str(&node_id_str).context("parse node id (z-base32)")?;
    let relay: RelayUrl = relay_str.parse().context("parse relay URL")?;

    let server_addr = EndpointAddr::new(node_id).with_relay_url(relay.clone());

    let transport = Arc::new(WebRtcTransport::new(vec![2; 16]));
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![SIGNALING_ALPN.to_vec(), APP_ALPN.to_vec()])
        .transport_config(quic_with_datagrams())
        .transport_bias(AddrKind::Custom(WEBRTC_TRANSPORT_ID), custom_bias())
        .add_custom_transport(transport.clone())
        .bind()
        .await
        .context("bind client endpoint")?;

    endpoint.online().await;

    // --- Phase 1: signaling + bridge (no extra SCTP ping) ---
    let connection = endpoint
        .connect(server_addr.clone(), SIGNALING_ALPN)
        .await
        .context("connect for WebRTC signaling over default iroh path")?;

    let (send, recv) = connection.open_bi().await.context("open bi stream")?;

    let mut sig = iroh_webrtc_transport::QuicSignaling::new(send, recv);
    let peer = iroh_webrtc_transport::negotiate_dc_as_offerer(&mut sig, DC_LABEL)
        .await
        .context("WebRTC offer + SCTP data channel")?;

    transport
        .attach_data_channel(
            peer,
            custom_addr_from_opaque_data(&[1u8; 16]),
            AttachOptions {
                mirror_sctp_echo: false,
                tap_inbound_to: None,
            },
        )
        .context("attach WebRTC bridge")?;

    println!("client: WebRTC bridge attached. Closing signaling QUIC.");
    connection.close(0u8.into(), b"signaling done");

    // --- Phase 2: chat ---
    let mixed = mixed_server_addr(node_id, relay);
    let app_conn = endpoint
        .connect(mixed, APP_ALPN)
        .await
        .context("connect APP_ALPN (mixed relay + server custom addr)")?;

    let (send, recv) = app_conn.open_bi().await.context("open_bi chat")?;
    line_chat(send, recv, "server").await.context("chat")?;
    app_conn.close(0u8.into(), b"done");

    endpoint.close().await;

    Ok(())
}
