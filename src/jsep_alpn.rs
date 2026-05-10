//! Shared QUIC ALPN for JSEP (must match `browser-iroh` and native binaries).

/// ALPN for one newline-framed JSON SDP envelope per direction on a QUIC bidi stream ([`crate::QuicSignaling`]).
pub const JSEP_SIGNALING_ALPN: &[u8] = b"iroh-webrtc-transport/signal/0";
