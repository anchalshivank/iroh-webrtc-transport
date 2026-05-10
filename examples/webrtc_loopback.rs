//! Two endpoints in one process:
//! 1. JSEP over iroh QUIC (`SIGNALING_ALPN`), then attach the SCTP data channel to [`WebRtcTransport`]
//!    (bridge: `poll_send` / `poll_recv` ↔ SCTP).
//! 2. A second iroh connection on `APP_ALPN` with **relay + custom** addresses and a transport bias
//!    toward the WebRTC custom path, then **QUIC unreliable datagrams** so iroh exercises `poll_send` /
//!    `poll_recv` through the same SCTP channel.
//!
//! Run with:
//! `cargo run --example webrtc_loopback`

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh::{
    Endpoint, Watcher,
    endpoint::{
        presets,
        transports::{AddrKind, TransportBias},
        QuicTransportConfig,
    },
    EndpointAddr, TransportAddr,
};
use iroh_webrtc_transport::{
    AttachOptions, WebRtcTransport, WEBRTC_TRANSPORT_ID, custom_addr_from_opaque_data,
};

const SIGNALING_ALPN: &[u8] = b"iroh-webrtc-transport/signal/0";
const APP_ALPN: &[u8] = b"iroh-webrtc-transport/loopback/0";

const DC_LABEL: &str = "iroh-loopback";

fn quic_with_datagrams() -> QuicTransportConfig {
    QuicTransportConfig::builder()
        .datagram_receive_buffer_size(Some(256 * 1024))
        .datagram_send_buffer_size(256 * 1024)
        .build()
}

fn custom_bias() -> TransportBias {
    TransportBias::primary().with_rtt_advantage(Duration::from_millis(100))
}

/// Prefer the WebRTC custom path while still including whatever relay/IP addresses discovery published.
fn mixed_server_addr(server_ep: &Endpoint, server_transport: &WebRtcTransport) -> EndpointAddr {
    let w = server_ep.watch_addr().get();
    let mut addrs: Vec<TransportAddr> = w.addrs.iter().cloned().collect();
    let custom = TransportAddr::Custom(server_transport.local_addr());
    if !addrs.contains(&custom) {
        addrs.push(custom);
    }
    EndpointAddr::from_parts(w.id, addrs)
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let server_transport = Arc::new(WebRtcTransport::new(vec![1; 16]));
    let client_transport = Arc::new(WebRtcTransport::new(vec![2; 16]));

    let server_endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![SIGNALING_ALPN.to_vec(), APP_ALPN.to_vec()])
        .transport_config(quic_with_datagrams())
        .transport_bias(AddrKind::Custom(WEBRTC_TRANSPORT_ID), custom_bias())
        .add_custom_transport(server_transport.clone())
        .bind()
        .await
        .context("bind server endpoint")?;

    let client_endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![SIGNALING_ALPN.to_vec(), APP_ALPN.to_vec()])
        .transport_config(quic_with_datagrams())
        .transport_bias(AddrKind::Custom(WEBRTC_TRANSPORT_ID), custom_bias())
        .add_custom_transport(client_transport.clone())
        .bind()
        .await
        .context("bind client endpoint")?;

    server_endpoint.online().await;
    client_endpoint.online().await;

    let server_addr = server_endpoint.watch_addr().get();

    let server = {
        let endpoint = server_endpoint.clone();
        let server_transport = server_transport.clone();
        tokio::spawn(async move {
            // --- Signaling + WebRTC attach ---
            let Some(incoming) = endpoint.accept().await else {
                anyhow::bail!("server endpoint closed before accept");
            };
            let mut accepting = incoming
                .accept()
                .context("begin accepting QUIC handshake")?;
            let alpn = accepting.alpn().await.context("read ALPN")?;
            anyhow::ensure!(alpn == SIGNALING_ALPN, "unexpected ALPN on loopback");
            let connection = accepting.await.context("accept incoming connection")?;
            let (send, recv) = connection.accept_bi().await.context("accept bi stream")?;

            let mut sig = iroh_webrtc_transport::QuicSignaling::new(send, recv);
            let peer = iroh_webrtc_transport::negotiate_dc_as_answerer(&mut sig)
                .await
                .context("WebRTC answer + SCTP data channel")?;

            server_transport
                .attach_data_channel(
                    peer,
                    custom_addr_from_opaque_data(&[2u8; 16]),
                    AttachOptions {
                        mirror_sctp_echo: true,
                        tap_inbound_to: None,
                    },
                )
                .context("attach server bridge")?;

            println!("server (loopback): SCTP data channel open; bridge echoes");
            connection.closed().await;

            // --- Second connection: QUIC datagrams over custom transport (via bridge) ---
            let Some(incoming2) = endpoint.accept().await else {
                anyhow::bail!("server endpoint closed before second accept");
            };
            let mut accepting2 = incoming2
                .accept()
                .context("accept app QUIC handshake")?;
            let alpn2 = accepting2.alpn().await.context("read app ALPN")?;
            anyhow::ensure!(alpn2 == APP_ALPN, "expected APP_ALPN second");
            let app_conn = accepting2.await.context("finish app handshake")?;
            let dg = app_conn
                .read_datagram()
                .await
                .context("server read_datagram")?;
            app_conn
                .send_datagram(dg)
                .context("server send_datagram echo")?;
            app_conn.closed().await;

            Result::<()>::Ok(())
        })
    };

    // --- Client: signaling + SCTP tap ---
    let connection = client_endpoint
        .connect(server_addr.clone(), SIGNALING_ALPN)
        .await
        .context("connect for signaling over default path")?;
    let (send, recv) = connection.open_bi().await.context("open bi stream")?;

    let mut sig = iroh_webrtc_transport::QuicSignaling::new(send, recv);
    let peer = iroh_webrtc_transport::negotiate_dc_as_offerer(&mut sig, DC_LABEL)
        .await
        .context("WebRTC offer + SCTP data channel")?;

    let (tap_tx, mut tap_rx) = tokio::sync::mpsc::unbounded_channel();
    client_transport
        .attach_data_channel(
            peer,
            custom_addr_from_opaque_data(&[1u8; 16]),
            AttachOptions {
                mirror_sctp_echo: false,
                tap_inbound_to: Some(tap_tx),
            },
        )
        .context("attach client bridge")?;

    let payload = b"loopback ping";
    client_transport
        .webrtc_out_sender()
        .send(payload.to_vec())
        .map_err(|_| anyhow::anyhow!("client SCTP out queue closed"))?;

    let echoed = tokio::time::timeout(Duration::from_secs(60), tap_rx.recv())
        .await
        .context("timeout waiting for SCTP tap echo")?
        .context("no SCTP tap echo")?;

    println!(
        "client (loopback bridge tap): echoed {:?}",
        String::from_utf8_lossy(&echoed)
    );

    connection.close(0u8.into(), b"signaling done");

    // --- Client: QUIC unreliable datagrams (iroh → poll_send → SCTP → … → poll_recv → iroh) ---
    let mixed = mixed_server_addr(&server_endpoint, &server_transport);
    let app_conn = client_endpoint
        .connect(mixed, APP_ALPN)
        .await
        .context("connect APP_ALPN over mixed addr (custom preferred)")?;

    let dg_payload = Bytes::from_static(b"quic-datagram-over-webrtc-bridge");
    app_conn
        .send_datagram(dg_payload.clone())
        .context("client send_datagram")?;
    let back = app_conn
        .read_datagram()
        .await
        .context("client read_datagram")?;
    anyhow::ensure!(
        back == dg_payload,
        "datagram round-trip mismatch: {:?} vs {:?}",
        back,
        dg_payload
    );
    println!(
        "client (loopback): QUIC datagram round-trip {:?} (via custom/WebRTC path when selected)",
        String::from_utf8_lossy(&back)
    );

    app_conn.close(0u8.into(), b"app done");

    server.await.context("join server task")??;

    client_endpoint.close().await;
    server_endpoint.close().await;
    Ok(())
}
