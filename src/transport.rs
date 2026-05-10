use std::io;
use std::sync::Arc;

use iroh::endpoint::transports::{CustomEndpoint, CustomTransport};
use iroh_base::CustomAddr;
use n0_watcher::Watchable;

use crate::bridge::{AttachOptions, WebRtcTunnel};
use crate::endpoint::WebRtcEndpoint;

/// Custom transport id for [`CustomAddr`] parts (see iroh `TRANSPORTS.md` for registration).
pub const WEBRTC_TRANSPORT_ID: u64 = u64::from_le_bytes(*b"irohwebr");

/// One inbound datagram worth of bytes from the SCTP data channel, tagged with the peer's [`CustomAddr`].
#[derive(Debug)]
pub(crate) struct InboundPacket {
    pub(crate) source_custom: CustomAddr,
    pub(crate) payload: Vec<u8>,
}

/// A WebRTC-backed custom transport: iroh `poll_send` / `poll_recv` are bridged to a negotiated SCTP data channel.
#[derive(Debug, Clone)]
pub struct WebRtcTransport {
    /// Opaque bytes advertised as this endpoint's [`CustomAddr`] data (paired with [`WEBRTC_TRANSPORT_ID`]).
    local_addr_bytes: Vec<u8>,
    pub(crate) tunnel: Arc<WebRtcTunnel>,
}

impl WebRtcTransport {
    pub fn new(local_addr_bytes: Vec<u8>) -> Self {
        let tunnel = WebRtcTunnel::new(local_addr_bytes.clone());
        Self {
            local_addr_bytes,
            tunnel,
        }
    }

    /// Custom address this transport uses for [`CustomTransport::bind`] local advertisement and dialing.
    pub fn local_addr(&self) -> CustomAddr {
        CustomAddr::from_parts(WEBRTC_TRANSPORT_ID, &self.local_addr_bytes)
    }

    /// Wire a negotiated SCTP data channel into this transport so iroh can send/receive QUIC datagrams on it.
    ///
    /// Call from an async context (Tokio runtime). `remote_custom_addr` must match the peer's advertised
    /// [`CustomAddr`] data (same bytes they passed to [`WebRtcTransport::new`]).
    pub fn attach_data_channel(
        &self,
        dc: std::sync::Arc<webrtc::data_channel::RTCDataChannel>,
        remote_custom_addr: CustomAddr,
        opts: AttachOptions,
    ) -> anyhow::Result<()> {
        self.tunnel.attach(dc, remote_custom_addr, opts)
    }
}

impl CustomTransport for WebRtcTransport {
    fn bind(&self) -> io::Result<Box<dyn CustomEndpoint>> {
        self.tunnel.mark_bound()?;
        let in_rx = self.tunnel.take_inbound_receiver()?;
        let local = CustomAddr::from_parts(WEBRTC_TRANSPORT_ID, &self.local_addr_bytes);
        let watchable = Watchable::new(vec![local]);
        let endpoint = WebRtcEndpoint::new(self.tunnel.clone(), in_rx, watchable);
        Ok(Box::new(endpoint))
    }
}
