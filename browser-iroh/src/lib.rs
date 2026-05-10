//! iroh `Endpoint` in the browser (relay) + QUIC newline JSON JSEP (same framing as the crate's `QuicSignaling`).
//!
//! Build: `bash scripts/build-browser-wasm.sh`

use std::str::FromStr;
use std::sync::Arc;

use iroh::endpoint::{RecvStream, SendStream};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};
use tokio::io::AsyncWriteExt;
use wasm_bindgen::prelude::*;

/// Must match `iroh-webrtc-transport` `JSEP_SIGNALING_ALPN`.
const SIGNALING_ALPN: &[u8] = b"iroh-webrtc-transport/signal/0";

#[wasm_bindgen]
pub struct IrohBrowserNode {
    ep: Arc<Endpoint>,
}

#[wasm_bindgen]
impl IrohBrowserNode {
    /// Bind with `presets::N0`, register JSEP ALPN, wait until relay is usable.
    pub async fn connect_n0() -> Result<IrohBrowserNode, JsValue> {
        let ep = Endpoint::builder(presets::N0)
            .alpns(vec![SIGNALING_ALPN.to_vec()])
            .bind()
            .await
            .map_err(|e| JsValue::from_str(&format!("iroh bind: {e}")))?;
        ep.online().await;
        Ok(IrohBrowserNode {
            ep: Arc::new(ep),
        })
    }

    #[wasm_bindgen(js_name = nodeId)]
    pub fn node_id(&self) -> String {
        self.ep.id().to_string()
    }

    /// Home relay URL from the current [`EndpointAddr`] (pass to native `iroh-jsep-chat`).
    #[wasm_bindgen(js_name = homeRelayUrl)]
    pub fn home_relay_url(&self) -> Option<String> {
        self.ep.addr().relay_urls().next().map(|u| u.to_string())
    }

    /// Wait for a peer to dial this node with JSEP ALPN, then accept the bidirectional signaling stream.
    #[wasm_bindgen(js_name = acceptJsepSignaling)]
    pub async fn accept_jsep_signaling(&self) -> Result<JsepQuicSignaling, JsValue> {
        let incoming = self
            .ep
            .accept()
            .await
            .ok_or_else(|| JsValue::from_str("endpoint closed (accept)"))?;
        let mut accepting = incoming
            .accept()
            .map_err(|e| JsValue::from_str(&format!("accept handshake: {e}")))?;
        let alpn = accepting
            .alpn()
            .await
            .map_err(|e| JsValue::from_str(&format!("read ALPN: {e}")))?;
        if alpn.as_slice() != SIGNALING_ALPN {
            return Err(JsValue::from_str(&format!(
                "unexpected ALPN (want JSEP): {}",
                String::from_utf8_lossy(&alpn)
            )));
        }
        let conn = accepting
            .await
            .map_err(|e| JsValue::from_str(&format!("finish handshake: {e}")))?;
        let (send, recv) = conn
            .accept_bi()
            .await
            .map_err(|e| JsValue::from_str(&format!("accept_bi: {e}")))?;
        Ok(JsepQuicSignaling { send, recv })
    }

    /// Dial another iroh node over QUIC with the JSEP ALPN, then open the signaling bidi stream (offerer side).
    ///
    /// `remote_node_id_z32` is the peer’s [`PublicKey`] string (same as shown in the UI).  
    /// `remote_relay_url` must be the peer’s **home relay** (their page shows it); use that URL, not yours, unless you know what you’re doing.
    #[wasm_bindgen(js_name = dialJsepSignaling)]
    pub async fn dial_jsep_signaling(
        &self,
        remote_node_id_z32: &str,
        remote_relay_url: &str,
    ) -> Result<JsepQuicSignaling, JsValue> {
        let remote_node_id_z32 = remote_node_id_z32.trim();
        let remote_relay_url = remote_relay_url.trim();
        if remote_node_id_z32.is_empty() {
            return Err(JsValue::from_str("peer node id is empty"));
        }
        if remote_relay_url.is_empty() {
            return Err(JsValue::from_str("peer relay URL is empty"));
        }
        let pk = PublicKey::from_str(remote_node_id_z32)
            .map_err(|e| JsValue::from_str(&format!("parse peer node id: {e}")))?;
        let relay: RelayUrl = remote_relay_url
            .parse()
            .map_err(|e| JsValue::from_str(&format!("parse peer relay URL: {e}")))?;
        let addr = EndpointAddr::new(pk).with_relay_url(relay);
        let conn = self
            .ep
            .connect(addr, SIGNALING_ALPN)
            .await
            .map_err(|e| JsValue::from_str(&format!("iroh connect: {e}")))?;
        let (send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| JsValue::from_str(&format!("open_bi: {e}")))?;
        Ok(JsepQuicSignaling { send, recv })
    }
}

/// One newline-terminated UTF-8 line per message (same framing as Rust `QuicSignaling`).
#[wasm_bindgen]
pub struct JsepQuicSignaling {
    send: SendStream,
    recv: RecvStream,
}

#[wasm_bindgen]
impl JsepQuicSignaling {
    /// Send a line; appends `\n` if missing.
    #[wasm_bindgen(js_name = sendLine)]
    pub async fn send_line(&mut self, text: &str) -> Result<(), JsValue> {
        let mut line = text.to_string();
        if !line.ends_with('\n') {
            line.push('\n');
        }
        self.send
            .write_all(line.as_bytes())
            .await
            .map_err(|e| JsValue::from_str(&format!("send_line write: {e}")))?;
        self.send
            .flush()
            .await
            .map_err(|e| JsValue::from_str(&format!("send_line flush: {e}")))?;
        Ok(())
    }

    /// Read until `\n` (not included in returned string).
    #[wasm_bindgen(js_name = recvLine)]
    pub async fn recv_line(&mut self) -> Result<String, JsValue> {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = self
                .recv
                .read(&mut byte)
                .await
                .map_err(|e| JsValue::from_str(&format!("recv_line: {e:?}")))?;
            let Some(n) = n else {
                return Err(JsValue::from_str("recv_line: stream closed before newline"));
            };
            if n == 0 {
                continue;
            }
            if byte[0] == b'\n' {
                break;
            }
            line.push(byte[0]);
        }
        String::from_utf8(line).map_err(|e| JsValue::from_str(&format!("recv_line utf-8: {e}")))
    }
}

#[wasm_bindgen(js_name = initPanicHook)]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}
