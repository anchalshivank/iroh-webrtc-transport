//! Bridges iroh [`CustomSender::poll_send`] / [`CustomEndpoint::poll_recv`] to a WebRTC SCTP data channel.
//!
//! One [`WebRtcTunnel`] is shared by [`crate::WebRtcTransport`], its [`crate::endpoint::WebRtcEndpoint`], and
//! [`crate::sender::WebRtcSender`]. After JSEP establishes a channel, call [`WebRtcTunnel::attach_str0m_peer`].
//!
//! ## `Arc` vs `Mutex` / `RwLock` (why both appear)
//!
//! - `Arc` shares **ownership** of the tunnel across the transport, endpoint, and sender so they see the same queues.
//! - `Mutex` is **not** a substitute for `Arc`: it only serializes access to a value. Here it wraps
//!   one-off `take()` slots and the recv waker—short critical sections; `RwLock` would not help those.
//! - [`CustomAddr`] for the remote peer is read on every `poll_send` validation but written once at attach, so it
//!   lives in a [`RwLock`] so concurrent readers do not exclude each other.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use iroh_base::CustomAddr;
use tokio::sync::mpsc;

/// Custom transport id for [`CustomAddr`] parts (see iroh `TRANSPORTS.md` for registration).
pub const WEBRTC_TRANSPORT_ID: u64 = u64::from_le_bytes(*b"irohwebr");

/// One inbound datagram worth of bytes from the SCTP data channel, tagged with the peer's [`CustomAddr`].
#[derive(Debug)]
pub(crate) struct InboundPacket {
    pub(crate) source_custom: CustomAddr,
    pub(crate) payload: Vec<u8>,
}

const IN_QUEUE: usize = 1024;

/// Optional behavior when attaching a data channel to a [`crate::WebRtcTransport`].
#[derive(Debug, Default, Clone)]
pub struct AttachOptions {
    /// If true, every inbound SCTP payload is also sent back on the same data channel (demo echo).
    pub mirror_sctp_echo: bool,
    /// If set, a copy of each inbound payload is forwarded here (e.g. for example logging).
    pub tap_inbound_to: Option<mpsc::UnboundedSender<Vec<u8>>>,
}

/// Shared bridge between iroh custom transport I/O and one SCTP data channel.
#[derive(Debug)]
pub(crate) struct WebRtcTunnel {
    /// Opaque local address bytes (same as [`crate::WebRtcTransport::local_addr`] data).
    #[allow(dead_code)]
    local_addr_bytes: Vec<u8>,
    bound: AtomicBool,
    attached: AtomicBool,
    out_tx: mpsc::UnboundedSender<Vec<u8>>,
    out_rx: Mutex<Option<mpsc::UnboundedReceiver<Vec<u8>>>>,
    in_tx: mpsc::Sender<InboundPacket>,
    in_rx: Mutex<Option<mpsc::Receiver<InboundPacket>>>,
    remote_custom: RwLock<Option<CustomAddr>>,
    recv_waker: Mutex<Option<std::task::Waker>>,
}

impl WebRtcTunnel {
    pub(crate) fn new(local_addr_bytes: Vec<u8>) -> Arc<Self> {
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (in_tx, in_rx) = mpsc::channel(IN_QUEUE);
        Arc::new(Self {
            local_addr_bytes,
            bound: AtomicBool::new(false),
            attached: AtomicBool::new(false),
            out_tx,
            out_rx: Mutex::new(Some(out_rx)),
            in_tx,
            in_rx: Mutex::new(Some(in_rx)),
            remote_custom: RwLock::new(None),
            recv_waker: Mutex::new(None),
        })
    }

    pub(crate) fn mark_bound(&self) -> io::Result<()> {
        if self
            .bound
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(io::Error::other(
                "WebRtcTransport::bind: only one bind() is supported per WebRtcTransport instance",
            ));
        }
        Ok(())
    }

    pub(crate) fn take_inbound_receiver(&self) -> io::Result<mpsc::Receiver<InboundPacket>> {
        self.in_rx
            .lock()
            .map_err(|_| io::Error::other("poisoned tunnel lock"))?
            .take()
            .ok_or_else(|| io::Error::other("inbound receiver already taken"))
    }

    pub(crate) fn out_sender(&self) -> mpsc::UnboundedSender<Vec<u8>> {
        self.out_tx.clone()
    }

    pub(crate) fn remote_custom(&self) -> Option<CustomAddr> {
        self.remote_custom.read().ok().and_then(|g| g.clone())
    }

    pub(crate) fn wake_recv_pollers(&self) {
        if let Ok(mut g) = self.recv_waker.lock() {
            if let Some(w) = g.take() {
                w.wake();
            }
        }
    }

    pub(crate) fn register_recv_waker(&self, waker: &std::task::Waker) {
        if let Ok(mut g) = self.recv_waker.lock() {
            *g = Some(waker.clone());
        }
    }

    pub(crate) fn try_mark_attached(&self) -> io::Result<()> {
        if self
            .attached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(io::Error::other(
                "WebRtcTunnel::attach: data channel already attached",
            ));
        }
        Ok(())
    }

    pub(crate) fn set_remote_custom(&self, addr: CustomAddr) -> io::Result<()> {
        let mut g = self
            .remote_custom
            .write()
            .map_err(|_| io::Error::other("poisoned tunnel lock"))?;
        *g = Some(addr);
        Ok(())
    }

    pub(crate) fn take_outbound_receiver(
        &self,
    ) -> io::Result<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>> {
        self.out_rx
            .lock()
            .map_err(|_| io::Error::other("poisoned tunnel lock"))?
            .take()
            .ok_or_else(|| io::Error::other("outbound receiver already taken"))
    }

    pub(crate) fn inbound_sender(&self) -> mpsc::Sender<InboundPacket> {
        self.in_tx.clone()
    }
}

/// Build the [`CustomAddr`] for a peer that advertises the given opaque address bytes on this transport id.
pub fn custom_addr_from_opaque_data(addr_data: &[u8]) -> CustomAddr {
    CustomAddr::from_parts(WEBRTC_TRANSPORT_ID, addr_data)
}
