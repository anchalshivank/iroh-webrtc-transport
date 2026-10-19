use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

use iroh::endpoint::transports::{CustomSender, Transmit};
use iroh_base::CustomAddr;

use crate::bridge::{WEBRTC_TRANSPORT_ID, WebRtcTunnel};

#[derive(Debug)]
pub(crate) struct WebRtcSender {
    tunnel: Arc<WebRtcTunnel>,
}

impl WebRtcSender {
    pub(crate) fn new(tunnel: Arc<WebRtcTunnel>) -> Self {
        Self { tunnel }
    }

    fn split_transmit<'a>(transmit: &'a Transmit<'a>) -> impl Iterator<Item = Vec<u8>> + 'a {
        let segment_size = transmit
            .segment_size
            .unwrap_or(transmit.contents.len())
            .max(1);
        transmit.contents.chunks(segment_size).map(|c| c.to_vec())
    }
}

impl CustomSender for WebRtcSender {
    fn is_valid_send_addr(&self, addr: &CustomAddr) -> bool {
        addr.id() == WEBRTC_TRANSPORT_ID && self.tunnel.has_peer(addr.data())
    }

    fn poll_send(
        &self,
        _cx: &mut Context,
        dst: &CustomAddr,
        _src: Option<&CustomAddr>,
        transmit: &Transmit<'_>,
    ) -> Poll<io::Result<()>> {
        let Some(out_tx) = self.tunnel.out_sender_for(dst.data()) else {
            tracing::debug!(
                ?dst,
                len = transmit.contents.len(),
                "no WebRTC channel for remote"
            );
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "no WebRTC data channel attached for that remote CustomAddr",
            )));
        };
        tracing::trace!(
            ?dst,
            len = transmit.contents.len(),
            "sending QUIC datagram over SCTP"
        );

        // One SCTP message per QUIC segment — message boundary is the
        // datagram boundary (max_transmit_segments is 1 so contents is
        // normally a single packet).
        for chunk in Self::split_transmit(transmit) {
            if out_tx.send(chunk).is_err() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "WebRTC outbound queue closed",
                )));
            }
        }
        Poll::Ready(Ok(()))
    }
}
