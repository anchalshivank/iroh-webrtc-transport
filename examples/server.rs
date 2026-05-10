use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use iroh::{
    Endpoint, Watcher,
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

/// QUIC connections carrying WebRTC SDP (JSEP) over the normal iroh path.
const SIGNALING_ALPN: &[u8] = b"iroh-webrtc-transport/signal/0";
/// Chat session runs over this ALPN on a path that prefers the WebRTC custom transport.
const APP_ALPN: &[u8] = b"iroh-webrtc-transport/loopback/0";

const DC_LABEL: &str = "iroh-quic-signaled";

fn quic_with_datagrams() -> QuicTransportConfig {
    QuicTransportConfig::builder()
        .datagram_receive_buffer_size(Some(256 * 1024))
        .datagram_send_buffer_size(256 * 1024)
        .build()
}

fn custom_bias() -> TransportBias {
    TransportBias::primary().with_rtt_advantage(Duration::from_millis(100))
}

/// Line-oriented chat: each line is one message (UTF-8). Empty line or Ctrl+D on stdin ends your side.
async fn line_chat(mut send: SendStream, recv: RecvStream, peer_label: &str) -> Result<()> {
    let mut stdin = BufReader::new(io::stdin()).lines();
    let mut peer_lines = BufReader::new(recv).lines();

    println!("Chat ready. Lines you type go to the client; prefix `{peer_label}` shows their lines.");
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
    let transport = Arc::new(WebRtcTransport::new(vec![1; 16]));
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![SIGNALING_ALPN.to_vec(), APP_ALPN.to_vec()])
        .transport_config(quic_with_datagrams())
        .transport_bias(AddrKind::Custom(WEBRTC_TRANSPORT_ID), custom_bias())
        .add_custom_transport(transport.clone())
        .bind()
        .await
        .context("bind server endpoint")?;

    endpoint.online().await;
    let dial_to_server = endpoint.watch_addr().get();
    let relay = dial_to_server
        .relay_urls()
        .next()
        .cloned()
        .context(
            "no relay URL in local EndpointAddr; check network and preset (presets::N0)",
        )?;

    println!(
        "Signaling: run\n  cargo run --example client -- {} {}\nThen a chat session opens on the second QUIC connection (WebRTC-bridged path when selected).\nServer custom addr opaque bytes: sixteen 0x01 (must match client).",
        dial_to_server.id,
        relay
    );

    let server = {
        let ep = endpoint.clone();
        let transport = transport.clone();
        tokio::spawn(async move {
            // --- Phase 1: signaling + SCTP bridge attach ---
            let Some(incoming) = ep.accept().await else {
                anyhow::bail!("endpoint closed before incoming connection");
            };
            let mut accepting = incoming
                .accept()
                .context("begin accepting QUIC handshake")?;
            let alpn = accepting.alpn().await.context("read negotiated ALPN")?;
            anyhow::ensure!(
                alpn == SIGNALING_ALPN,
                "unexpected ALPN from peer: {:?}",
                String::from_utf8_lossy(&alpn)
            );

            let connection = accepting.await.context("finish handshake")?;
            let (send, recv) = connection.accept_bi().await.context("accept bi stream")?;

            let mut sig = iroh_webrtc_transport::QuicSignaling::new(send, recv);
            let (_pc, dc) = iroh_webrtc_transport::negotiate_dc_as_answerer(&mut sig)
                .await
                .context("WebRTC answer + SCTP data channel")?;

            transport
                .attach_data_channel(
                    dc,
                    custom_addr_from_opaque_data(&[2u8; 16]),
                    AttachOptions {
                        mirror_sctp_echo: false,
                        tap_inbound_to: None,
                    },
                )
                .context("attach WebRTC bridge")?;

            println!(
                "server: WebRTC bridge attached (label {:?}). Closing signaling QUIC.",
                DC_LABEL
            );
            connection.closed().await;

            // --- Phase 2: line chat over QUIC bidi stream (custom / WebRTC path) ---
            let Some(incoming2) = ep.accept().await else {
                anyhow::bail!("endpoint closed before second accept");
            };
            let mut accepting2 = incoming2
                .accept()
                .context("accept APP_ALPN handshake")?;
            let alpn2 = accepting2.alpn().await.context("read APP ALPN")?;
            anyhow::ensure!(
                alpn2 == APP_ALPN,
                "unexpected second ALPN: {:?}",
                String::from_utf8_lossy(&alpn2)
            );
            let app_conn = accepting2.await.context("finish APP handshake")?;
            let (send, recv) = app_conn.accept_bi().await.context("accept_bi chat")?;
            line_chat(send, recv, "client").await.context("chat")?;
            app_conn.close(0u8.into(), b"chat done");

            Result::<()>::Ok(())
        })
    };

    server.await.context("join server task")??;
    endpoint.close().await;

    Ok(())
}
