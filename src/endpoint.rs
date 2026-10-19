use std::{
    io,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use iroh::endpoint::transports::{CustomEndpoint, CustomSender, RecvInfo};
use iroh_base::CustomAddr;
use n0_watcher::Watchable;
use noq_udp::RecvMeta;
use tokio::sync::mpsc;

use crate::bridge::{InboundPacket, WebRtcTunnel};
use crate::sender::WebRtcSender;

#[derive(Debug)]
pub(crate) struct WebRtcEndpoint {
    tunnel: Arc<WebRtcTunnel>,
    receiver: Mutex<mpsc::Receiver<InboundPacket>>,
    watchable: Watchable<Vec<CustomAddr>>,
}

impl WebRtcEndpoint {
    pub fn new(
        tunnel: Arc<WebRtcTunnel>,
        receiver: mpsc::Receiver<InboundPacket>,
        watchable: Watchable<Vec<CustomAddr>>,
    ) -> Self {
        Self {
            tunnel,
            receiver: Mutex::new(receiver),
            watchable,
        }
    }
}

impl CustomEndpoint for WebRtcEndpoint {
    fn watch_local_addrs(&self) -> n0_watcher::Direct<Vec<CustomAddr>> {
        self.watchable.watch()
    }

    fn max_transmit_segments(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }

    fn create_sender(&self) -> Arc<dyn CustomSender> {
        Arc::new(WebRtcSender::new(self.tunnel.clone()))
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        metas: &mut [RecvMeta],
        recv_infos: &mut [RecvInfo],
    ) -> Poll<io::Result<usize>> {
        let n = bufs.len();
        debug_assert_eq!(n, metas.len());
        debug_assert_eq!(n, recv_infos.len());
        if n == 0 {
            return Poll::Ready(Ok(0));
        }

        let mut rx = self
            .receiver
            .lock()
            .map_err(|_| io::Error::other("poisoned receiver mutex"))?;
        match rx.poll_recv(cx) {
            Poll::Ready(Some(packet)) => {
                tracing::trace!(
                    src = ?packet.source_custom,
                    len = packet.payload.len(),
                    "received QUIC datagram over SCTP"
                );
                if bufs[0].len() < packet.payload.len() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WebRTC bridge: recv buffer smaller than datagram",
                    )));
                }
                bufs[0][..packet.payload.len()].copy_from_slice(&packet.payload);
                metas[0].len = packet.payload.len();
                metas[0].stride = packet.payload.len();
                recv_infos[0] = RecvInfo::new(packet.source_custom, Some(self.tunnel.local_addr()));
                Poll::Ready(Ok(1))
            }
            Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WebRTC inbound queue closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}
