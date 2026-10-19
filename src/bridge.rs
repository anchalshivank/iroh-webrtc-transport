//! Bridges iroh [`CustomSender::poll_send`] / [`CustomEndpoint::poll_recv`] to WebRTC SCTP data channels.
//!
//! One [`WebRtcTunnel`] is shared by [`crate::WebRtcTransport`], its [`crate::endpoint::WebRtcEndpoint`], and
//! [`crate::sender::WebRtcSender`]. After JSEP establishes a channel, call [`WebRtcTunnel::attach_str0m_peer`]
//! — one tunnel serves every peer: outbound payloads route by the destination's [`CustomAddr`] data,
//! inbound datagrams arrive on a shared queue tagged with the source's [`CustomAddr`].
//!
//! ## `Arc` vs `Mutex` (why both appear)
//!
//! - `Arc` shares **ownership** of the tunnel across the transport, endpoint, and sender so they see the same queues.
//! - `Mutex` is **not** a substitute for `Arc`: it only serializes access to a value. Here it wraps the
//!   per-peer outbound map and the one-off inbound receiver — short critical sections.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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

#[derive(Debug)]
/// Outbound queue for one attached peer, plus the attach generation — a
/// renegotiated channel's teardown must not evict its replacement's entry.
struct PeerOut {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    generation: u64,
}

/// Shared bridge between iroh custom transport I/O and SCTP data channels — one per remote peer.
#[derive(Debug)]
pub(crate) struct WebRtcTunnel {
    /// Opaque local address bytes (same as [`crate::WebRtcTransport::local_addr`] data).
    local_addr_bytes: Vec<u8>,
    bound: AtomicBool,
    /// Live channels keyed by the remote's `CustomAddr` data — `poll_send` routes by it.
    peers: Mutex<HashMap<Vec<u8>, PeerOut>>,
    /// Attach generation counter — bumped per attach so stale teardown can be distinguished.
    generation: AtomicU64,
    in_tx: mpsc::Sender<InboundPacket>,
    in_rx: Mutex<Option<mpsc::Receiver<InboundPacket>>>,
}

impl WebRtcTunnel {
    pub(crate) fn new(local_addr_bytes: Vec<u8>) -> Arc<Self> {
        let (in_tx, in_rx) = mpsc::channel(IN_QUEUE);
        Arc::new(Self {
            local_addr_bytes,
            bound: AtomicBool::new(false),
            peers: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
            in_tx,
            in_rx: Mutex::new(Some(in_rx)),
        })
    }

    /// The [`CustomAddr`] this transport advertises.
    pub(crate) fn local_addr(&self) -> CustomAddr {
        CustomAddr::from_parts(WEBRTC_TRANSPORT_ID, &self.local_addr_bytes)
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

    /// Outbound sender for the peer with this custom-addr data, if attached.
    pub(crate) fn out_sender_for(
        &self,
        remote_data: &[u8],
    ) -> Option<mpsc::UnboundedSender<Vec<u8>>> {
        self.peers
            .lock()
            .ok()?
            .get(remote_data)
            .map(|p| p.tx.clone())
    }

    pub(crate) fn has_peer(&self, remote_data: &[u8]) -> bool {
        self.peers
            .lock()
            .map(|p| p.contains_key(remote_data))
            .unwrap_or(false)
    }

    /// Claim this peer's outbound slot; the driver task drains `rx`. Replacing an
    /// existing entry drops the old sender, which ends the old driver's `recv()`.
    /// Returns the generation identifying this attach.
    pub(crate) fn register_peer_out(
        &self,
        remote_data: &[u8],
    ) -> (u64, mpsc::UnboundedReceiver<Vec<u8>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        self.peers
            .lock()
            .expect("poisoned peers lock")
            .insert(remote_data.to_vec(), PeerOut { tx, generation });
        (generation, rx)
    }

    /// Remove a peer's outbound entry — only if its generation still matches,
    /// so a replaced channel's teardown can't evict the replacement.
    pub(crate) fn remove_peer_out(&self, remote_data: &[u8], generation: u64) {
        if let Ok(mut peers) = self.peers.lock() {
            if peers.get(remote_data).map(|p| p.generation) == Some(generation) {
                peers.remove(remote_data);
            }
        }
    }

    pub(crate) fn inbound_sender(&self) -> mpsc::Sender<InboundPacket> {
        self.in_tx.clone()
    }
}

/// Build the [`CustomAddr`] for a peer that advertises the given opaque address bytes on this transport id.
pub fn custom_addr_from_opaque_data(addr_data: &[u8]) -> CustomAddr {
    CustomAddr::from_parts(WEBRTC_TRANSPORT_ID, addr_data)
}
