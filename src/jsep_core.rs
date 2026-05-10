//! WebRTC JSEP negotiation (offer/answer + ICE-in-SDP) over any [`Signaling`] transport.
//!
//! Wire types: [`crate::jsep_envelope`], [`crate::jsep_signaling`]. Implementations:
//! [`crate::QuicSignaling`], [`crate::TcpWebSocket`].

use std::sync::{Arc, Once};
use std::time::Duration;

use anyhow::{Context as _, bail};
use tokio::time::timeout;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice::mdns::MulticastDnsMode;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::jsep_envelope::SignalEnvelope;
use crate::jsep_signaling::Signaling;

static INSTALL_RUSTLS_CRYPTO: Once = Once::new();

pub(crate) fn ensure_rustls_crypto_provider() {
    INSTALL_RUSTLS_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("install rustls CryptoProvider (aws_lc_rs) for WebRTC DTLS");
    });
}

pub(crate) async fn build_peer_connection() -> anyhow::Result<Arc<RTCPeerConnection>> {
    ensure_rustls_crypto_provider();
    let mut m = MediaEngine::default();
    m.register_default_codecs()
        .context("register_default_codecs")?;

    let registry = Registry::new();
    let registry = register_default_interceptors(registry, &mut m)
        .map_err(|e| anyhow::anyhow!("register_default_interceptors: {e}"))?;

    let mut settings = SettingEngine::default();
    settings.set_ice_multicast_dns_mode(MulticastDnsMode::Disabled);

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .with_setting_engine(settings)
        .build();

    let cfg = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            ..Default::default()
        }],
        ..Default::default()
    };

    let pc = api
        .new_peer_connection(cfg)
        .await
        .context("new_peer_connection")?;
    Ok(Arc::new(pc))
}

async fn wait_peer_connected(pc: &RTCPeerConnection) -> anyhow::Result<()> {
    const PER: Duration = Duration::from_millis(100);
    const LIMIT: Duration = Duration::from_secs(60);
    let mut elapsed = Duration::ZERO;
    loop {
        match pc.connection_state() {
            RTCPeerConnectionState::Connected => return Ok(()),
            RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed => {
                bail!("WebRTC peer connection state {:?}", pc.connection_state());
            }
            _ => {}
        }
        if elapsed >= LIMIT {
            bail!(
                "timeout waiting for WebRTC Connected, last state {:?}",
                pc.connection_state()
            );
        }
        tokio::time::sleep(PER).await;
        elapsed += PER;
    }
}

async fn wait_dc_open(dc: &RTCDataChannel) -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(60),
        async {
            const PER: Duration = Duration::from_millis(50);
            loop {
                if dc.ready_state()
                    == webrtc::data_channel::data_channel_state::RTCDataChannelState::Open
                {
                    return Ok(());
                }
                tokio::time::sleep(PER).await;
            }
        },
    )
    .await
    .context("timeout waiting for data channel open")?
}

pub async fn negotiate_dc_as_offerer<S: Signaling + ?Sized>(
    sig: &mut S,
    dc_label: &str,
) -> anyhow::Result<(Arc<RTCPeerConnection>, Arc<RTCDataChannel>)> {
    let pc = build_peer_connection().await?;

    let dc = pc
        .create_data_channel(dc_label, None)
        .await
        .context("create_data_channel")?;

    let offer = pc.create_offer(None).await.context("create_offer")?;
    let mut gather = pc.gathering_complete_promise().await;
    pc.set_local_description(offer)
        .await
        .context("set_local_description (offer)")?;
    let _ = gather.recv().await;

    let offer_desc = pc
        .local_description()
        .await
        .context("missing local_description after offer")?;

    sig.send_envelope(&SignalEnvelope::Offer {
        sdp: offer_desc.sdp,
    })
    .await?;

    let answer_env = sig.recv_envelope().await?;
    let sdp = match answer_env {
        SignalEnvelope::Answer { sdp } => sdp,
        other => bail!("expected answer, got {:?}", other),
    };

    let answer_desc = RTCSessionDescription::answer(sdp).map_err(|e| anyhow::anyhow!("{e}"))?;
    pc.set_remote_description(answer_desc)
        .await
        .context("set_remote_description (answer)")?;

    wait_peer_connected(&pc).await?;
    wait_dc_open(&dc).await?;

    Ok((pc, dc))
}

pub async fn negotiate_dc_as_answerer<S: Signaling + ?Sized>(
    sig: &mut S,
) -> anyhow::Result<(Arc<RTCPeerConnection>, Arc<RTCDataChannel>)> {
    let pc = build_peer_connection().await?;

    let (dc_tx, mut dc_rx) = tokio::sync::mpsc::channel::<Arc<RTCDataChannel>>(1);
    pc.on_data_channel(Box::new(move |d: Arc<RTCDataChannel>| {
        let dc_tx = dc_tx.clone();
        Box::pin(async move {
            let _ = dc_tx.send(d).await;
        })
    }));

    let offer_env = sig.recv_envelope().await?;
    let sdp = match offer_env {
        SignalEnvelope::Offer { sdp } => sdp,
        other => bail!("expected offer, got {:?}", other),
    };

    let offer_desc = RTCSessionDescription::offer(sdp).map_err(|e| anyhow::anyhow!("{e}"))?;
    pc.set_remote_description(offer_desc)
        .await
        .context("set_remote_description (offer)")?;

    let answer = pc.create_answer(None).await.context("create_answer")?;
    let mut gather = pc.gathering_complete_promise().await;
    pc.set_local_description(answer)
        .await
        .context("set_local_description (answer)")?;
    let _ = gather.recv().await;

    let answer_desc = pc
        .local_description()
        .await
        .context("missing local_description after answer")?;

    sig.send_envelope(&SignalEnvelope::Answer {
        sdp: answer_desc.sdp,
    })
    .await?;

    let dc = dc_rx
        .recv()
        .await
        .context("on_data_channel did not fire")?;

    wait_peer_connected(&pc).await?;
    wait_dc_open(&dc).await?;

    Ok((pc, dc))
}
