use std::{
    io,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use iroh::endpoint::transports::{Addr, CustomEndpoint, CustomSender};
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
        Arc::new(WebRtcSender::new(
            self.tunnel.out_sender(),
            self.tunnel.clone(),
        ))
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        metas: &mut [RecvMeta],
        source_addrs: &mut [Addr],
    ) -> Poll<io::Result<usize>> {
        let n = bufs.len();
        debug_assert_eq!(n, metas.len());
        debug_assert_eq!(n, source_addrs.len());
        if n == 0 {
            return Poll::Ready(Ok(0));
        }

        let mut recv = self
            .receiver
            .lock()
            .map_err(|_| io::Error::other("poisoned receiver mutex"))?;
        match recv.try_recv() {
            Ok(packet) => {
                if bufs[0].len() < packet.payload.len() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WebRTC bridge: recv buffer smaller than datagram",
                    )));
                }
                bufs[0][..packet.payload.len()].copy_from_slice(&packet.payload);
                source_addrs[0] = Addr::Custom(packet.source_custom);
                metas[0].len = packet.payload.len();
                metas[0].stride = packet.payload.len();
                Poll::Ready(Ok(1))
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                drop(recv);
                self.tunnel.register_recv_waker(cx.waker());
                let mut recv = self
                    .receiver
                    .lock()
                    .map_err(|_| io::Error::other("poisoned receiver mutex"))?;
                if let Ok(packet) = recv.try_recv() {
                    if bufs[0].len() < packet.payload.len() {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "WebRTC bridge: recv buffer smaller than datagram",
                        )));
                    }
                    bufs[0][..packet.payload.len()].copy_from_slice(&packet.payload);
                    source_addrs[0] = Addr::Custom(packet.source_custom);
                    metas[0].len = packet.payload.len();
                    metas[0].stride = packet.payload.len();
                    return Poll::Ready(Ok(1));
                }
                Poll::Pending
            }
            Err(mpsc::error::TryRecvError::Disconnected) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WebRTC inbound queue closed",
            ))),
        }
    }
}
