use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

use iroh::endpoint::transports::{CustomSender, Transmit};
use iroh_base::CustomAddr;
use tokio::sync::mpsc;

use crate::bridge::WebRtcTunnel;

#[derive(Debug)]
pub(crate) struct WebRtcSender {
    out_tx: mpsc::UnboundedSender<Vec<u8>>,
    tunnel: Arc<WebRtcTunnel>,
}

impl WebRtcSender {
    pub(crate) fn new(out_tx: mpsc::UnboundedSender<Vec<u8>>, tunnel: Arc<WebRtcTunnel>) -> Self {
        Self { out_tx, tunnel }
    }

    fn split_transmit<'a>(transmit: &'a Transmit<'a>) -> impl Iterator<Item = Vec<u8>> + 'a {
        let segment_size = transmit
            .segment_size
            .unwrap_or(transmit.contents.len())
            .max(1);
        transmit
            .contents
            .chunks(segment_size)
            .map(|c| c.to_vec())
    }
}

impl CustomSender for WebRtcSender {
    fn is_valid_send_addr(&self, addr: &CustomAddr) -> bool {
        self.tunnel
            .remote_custom()
            .is_some_and(|r| r.id() == addr.id() && r.data() == addr.data())
    }

    fn poll_send(
        &self,
        _cx: &mut Context,
        dst: &CustomAddr,
        transmit: &Transmit<'_>,
    ) -> Poll<io::Result<()>> {
        if self.tunnel.remote_custom().is_none() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "WebRTC data channel not attached to WebRtcTransport",
            )));
        }
        if !self.is_valid_send_addr(dst) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "poll_send dst does not match attached remote CustomAddr",
            )));
        }

        for chunk in Self::split_transmit(transmit) {
            if self.out_tx.send(chunk).is_err() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "WebRTC outbound queue closed",
                )));
            }
        }
        Poll::Ready(Ok(()))
    }
}
