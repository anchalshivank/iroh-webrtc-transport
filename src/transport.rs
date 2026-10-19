use std::io;
use std::sync::Arc;

use iroh::endpoint::transports::{CustomEndpoint, CustomTransport};
use iroh_base::CustomAddr;
use n0_watcher::Watchable;

use crate::bridge::{AttachOptions, WebRtcTunnel};
use crate::endpoint::WebRtcEndpoint;
use crate::str0m_peer::Str0mPeer;

pub use crate::bridge::WEBRTC_TRANSPORT_ID;

/// A WebRTC-backed custom transport: iroh `poll_send` / `poll_recv` are bridged to negotiated SCTP data channels.
///
/// `local_addr_bytes` convention shared with the browser implementation (`host/net/src/webrtc.rs`):
/// the endpoint's public-key bytes, so peers can derive our custom addr (and we theirs) from
/// `remote_id()` alone — self-certifying.
#[derive(Debug, Clone)]
pub struct WebRtcTransport {
    pub(crate) tunnel: Arc<WebRtcTunnel>,
}

impl WebRtcTransport {
    pub fn new(local_addr_bytes: Vec<u8>) -> Self {
        Self {
            tunnel: WebRtcTunnel::new(local_addr_bytes),
        }
    }

    /// Custom address this transport uses for [`CustomTransport::bind`] local advertisement and dialing.
    pub fn local_addr(&self) -> CustomAddr {
        self.tunnel.local_addr()
    }

    /// Queue that feeds the str0m SCTP data channel attached for `remote_data` (that peer's
    /// `CustomAddr` data). Use for tests or direct SCTP injection (same path as iroh `poll_send`).
    pub fn webrtc_out_sender(
        &self,
        remote_data: &[u8],
    ) -> Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>> {
        self.tunnel.out_sender_for(remote_data)
    }

    /// Wire a negotiated SCTP data channel into this transport so iroh can send/receive QUIC datagrams on it.
    ///
    /// Call from an async context (Tokio runtime). `remote_custom_addr` must match the peer's advertised
    /// [`CustomAddr`] data (same bytes they passed to [`WebRtcTransport::new`]). One call per peer —
    /// a re-attach replaces the old channel's outbound queue (the stale driver task exits when it
    /// observes the closed queue, and its teardown can't evict the replacement).
    pub fn attach_data_channel(
        &self,
        peer: Str0mPeer,
        remote_custom_addr: CustomAddr,
        opts: AttachOptions,
    ) -> anyhow::Result<()> {
        self.tunnel
            .attach_str0m_peer(peer, remote_custom_addr, opts)
    }
}

impl CustomTransport for WebRtcTransport {
    fn bind(&self) -> io::Result<Box<dyn CustomEndpoint>> {
        self.tunnel.mark_bound()?;
        let in_rx = self.tunnel.take_inbound_receiver()?;
        let local = self.tunnel.local_addr();
        let watchable = Watchable::new(vec![local]);
        let endpoint = WebRtcEndpoint::new(self.tunnel.clone(), in_rx, watchable);
        Ok(Box::new(endpoint))
    }
}
