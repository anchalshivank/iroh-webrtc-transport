//! Two native endpoints connected through the WebRTC custom transport.
//!
//! JSEP signaling rides a *separate* plain endpoint over IP/UDP; the app conn
//! is dialed with a custom-addr-only `EndpointAddr` from an endpoint that has
//! never seen the server's IP — so the connection can only exist if QUIC
//! datagrams actually flow through the negotiated SCTP data channel.
//! (One endpoint dialing both JSEP and the app conn would let iroh's learned
//! IP addrs win the initial-packet race instead of proving the channel.)

use std::sync::Arc;
use std::time::Duration;

use iroh::{Endpoint, EndpointAddr, SecretKey, TransportAddr, endpoint::presets};
use iroh_webrtc_transport::{
    AttachOptions, JSEP_SIGNALING_ALPN, QuicSignaling, WEBRTC_TRANSPORT_ID, WebRtcTransport,
    custom_addr_from_opaque_data, negotiate_dc_as_answerer, negotiate_dc_as_offerer,
};
use tokio::io::AsyncWriteExt;

const APP_ALPN: &[u8] = b"iroh-webrtc-transport/test-app/0";
const DC_LABEL: &str = "iroh";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_over_webrtc_datachannel() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init()
        .ok();
    tokio::time::timeout(Duration::from_secs(90), async {
        let server_key = SecretKey::from_bytes(&[7u8; 32]);
        let client_key = SecretKey::from_bytes(&[9u8; 32]);
        let sig_key = SecretKey::from_bytes(&[3u8; 32]);
        let server_transport =
            Arc::new(WebRtcTransport::new(server_key.public().as_bytes().to_vec()));
        let client_transport =
            Arc::new(WebRtcTransport::new(client_key.public().as_bytes().to_vec()));

        let server_ep = Endpoint::builder(presets::Minimal)
            .alpns(vec![JSEP_SIGNALING_ALPN.to_vec(), APP_ALPN.to_vec()])
            .secret_key(server_key)
            .add_custom_transport(server_transport.clone())
            .bind()
            .await
            .expect("server bind");
        // The app endpoint: only ever sees the server's custom addr.
        let client_ep = Endpoint::builder(presets::Minimal)
            .alpns(vec![APP_ALPN.to_vec()])
            .secret_key(client_key.clone())
            .add_custom_transport(client_transport.clone())
            .bind()
            .await
            .expect("client bind");
        // The signaling endpoint: separate identity so the server's learned
        // IP addrs stay scoped to it and can't leak into the app conn.
        let sig_ep = Endpoint::builder(presets::Minimal)
            .alpns(vec![JSEP_SIGNALING_ALPN.to_vec()])
            .secret_key(sig_key)
            .bind()
            .await
            .expect("signaling bind");

        let server_id = server_ep.id();
        let server_addr = server_ep.addr();
        eprintln!("[test] server addr: {server_addr:?}");
        let client_id = client_ep.id();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let incoming = server_ep.accept().await.expect("accept");
                let mut connecting = incoming.accept().expect("accept handshake");
                let alpn = connecting.alpn().await.expect("alpn");
                let conn = connecting.await.expect("handshake");
                let (mut send, mut recv) = conn.accept_bi().await.expect("accept_bi");
                if alpn.as_slice() == JSEP_SIGNALING_ALPN {
                    let mut sig = QuicSignaling::new(send, recv);
                    let peer = negotiate_dc_as_answerer(&mut sig)
                        .await
                        .expect("JSEP answer");
                    server_transport
                        .attach_data_channel(
                            peer,
                            custom_addr_from_opaque_data(client_id.as_bytes()),
                            AttachOptions::default(),
                        )
                        .expect("attach server channel");
                } else {
                    assert_eq!(alpn.as_slice(), APP_ALPN);
                    // Echo server: bounce whatever arrives back.
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(Some(n)) = recv.read(&mut buf).await {
                        send.write_all(&buf[..n]).await.expect("echo write");
                        send.flush().await.expect("echo flush");
                    }
                }
            }
        });

        // 1. JSEP offer over the separate signaling endpoint, then attach the
        //    channel to the *app* endpoint's transport.
        let sig_conn = sig_ep
            .connect(server_addr, JSEP_SIGNALING_ALPN)
            .await
            .expect("signaling connect");
        let (send, recv) = sig_conn.open_bi().await.expect("signaling stream");
        let mut sig = QuicSignaling::new(send, recv);
        let peer = negotiate_dc_as_offerer(&mut sig, DC_LABEL)
            .await
            .expect("JSEP offer");
        client_transport
            .attach_data_channel(
                peer,
                custom_addr_from_opaque_data(server_id.as_bytes()),
                AttachOptions::default(),
            )
            .expect("attach client channel");
        sig_conn.close(0u32.into(), b"done");

        // 2. App conn with a custom-addr-only ticket: no IP, no relay —
        //    the only way through is the SCTP data channel.
        let mut app_addr = EndpointAddr::new(server_id);
        app_addr.addrs.insert(TransportAddr::Custom(custom_addr_from_opaque_data(
            server_id.as_bytes(),
        )));
        let conn = client_ep
            .connect(app_addr, APP_ALPN)
            .await
            .expect("app connect over datachannel");

        // The selected path must be the custom transport — there is nothing else.
        let paths = conn.paths();
        let open: Vec<String> = paths
            .iter()
            .map(|p| format!("{:?}{}", p.remote_addr(), if p.is_selected() { " *" } else { "" }))
            .collect();
        assert!(
            paths.iter().all(|p| matches!(p.remote_addr(), TransportAddr::Custom(c) if c.id() == WEBRTC_TRANSPORT_ID))
                && paths.iter().any(|p| p.is_selected()),
            "expected only a selected custom path, open paths: {open:?}"
        );

        // 3. Real bytes round-trip through the channel.
        let (mut send, mut recv) = conn.open_bi().await.expect("app stream");
        send.write_all(b"ping over sctp").await.expect("write");
        send.flush().await.expect("flush");
        let mut buf = [0u8; 64];
        let n = recv.read(&mut buf).await.expect("read").expect("eof");
        assert_eq!(&buf[..n], b"ping over sctp");

        server.abort();
    })
    .await
    .expect("test timed out");
}
