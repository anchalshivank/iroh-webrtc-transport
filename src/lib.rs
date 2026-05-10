//! WebRTC data channels bridged to iroh custom transport, with JSEP signaling pluggable via [`Signaling`].
//!
//! Use [`QuicSignaling`] + [`negotiate_dc_as_offerer`] / [`negotiate_dc_as_answerer`] for iroh QUIC streams,
//! or [`TcpWebSocket`] + the same negotiate functions after your own WebSocket framing (e.g. room join).
//! For a browser iroh endpoint (WASM, relay-backed), see the `browser-iroh` crate and `scripts/build-browser-wasm.sh`.
//! JSEP over QUIC uses [`JSEP_SIGNALING_ALPN`] with [`QuicSignaling`]; native offerer: `iroh-jsep-chat` binary.
//! Browser static assets: `static-server` (port 8080). WebSocket JSEP relay only: `signaling-server` (port 3000).

mod bridge;
mod endpoint;
mod jsep_alpn;
mod jsep_core;
mod jsep_envelope;
mod jsep_quic;
mod jsep_signaling;
mod jsep_ws;
mod sender;
mod transport;

pub use bridge::{custom_addr_from_opaque_data, AttachOptions};
pub use jsep_alpn::JSEP_SIGNALING_ALPN;
pub use jsep_core::{negotiate_dc_as_answerer, negotiate_dc_as_offerer};
pub use jsep_envelope::SignalEnvelope;
pub use jsep_quic::QuicSignaling;
pub use jsep_signaling::Signaling;
pub use jsep_ws::TcpWebSocket;
pub use transport::{WebRtcTransport, WEBRTC_TRANSPORT_ID};