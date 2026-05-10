# iroh-webrtc-transport

Rust library and demos that combine **iroh** (QUIC, custom transports) with **WebRTC** data channels: JSEP signaling can run over **iroh QUIC streams**, **WebSocket**, or **tokio-tungstenite**, then SCTP traffic is bridged into iroh’s custom transport path where configured.

---

## Library (`src/`)


| Piece                                                                                          | Role                                                                                          |
| ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `[Signaling](src/jsep_signaling.rs)`                                                           | Async trait: `send_envelope` / `recv_envelope` for `[SignalEnvelope](src/jsep_envelope.rs)`.  |
| `[SignalEnvelope](src/jsep_envelope.rs)`                                                       | JSON `offer` / `answer` SDP payloads.                                                         |
| `[negotiate_dc_as_offerer](src/jsep_core.rs)` / `[negotiate_dc_as_answerer](src/jsep_core.rs)` | Generic WebRTC negotiation over any `Signaling` impl (webrtc-rs).                             |
| `[QuicSignaling](src/jsep_quic.rs)`                                                            | Newline-framed JSON on an iroh **bidi** stream (matches browser WASM framing).                |
| `[TcpWebSocket](src/jsep_ws.rs)`                                                               | `Signaling` for `tokio-tungstenite` WebSockets (one JSON text frame per envelope).            |
| `[JSEP_SIGNALING_ALPN](src/jsep_alpn.rs)`                                                      | Shared ALPN bytes for QUIC JSEP: `iroh-webrtc-transport/signal/0`.                            |
| `[WebRtcTunnel](src/bridge.rs)` / `[WebRtcTransport](src/transport.rs)`                        | Bridge SCTP data channel ↔ iroh custom transport (`poll_send` / `poll_recv`, attach, wakers). |


---

## Binaries


| Binary               | Purpose                                                                                                                                      |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `static-server`      | Serves `static/` only (HTML, `app.js`, `pkg/*.wasm`). Default [http://127.0.0.1:8080/](http://127.0.0.1:8080/)                               |
| `signaling-server`   | WebSocket JSEP relay only: `ws://127.0.0.1:3000/ws`. Rooms pair two peers; forwards SDP JSON. No static files.                              |
| `ws-chat`            | Native CLI chat using **WebSocket** signaling + same negotiate path as the browser WS mode.                                                  |
| `iroh-jsep-chat`     | Native CLI **offerer**: dials the browser’s iroh node, **QUIC JSEP** on `JSEP_SIGNALING_ALPN`, then stdin/stdout on the WebRTC data channel. |


Typical split UI + optional WS signaling:

```bash
cargo run --bin static-server
cargo run --bin signaling-server   # only if you use “WebSocket room” in the page
```

---

## Browser (`static/` + `browser-iroh/`)

- `browser-iroh/` — WASM crate: iroh `Endpoint` (`presets::N0`), `acceptJsepSignaling` (answer side), `JsepQuicSignaling` `sendLine` / `recvLine` (same newline protocol as `QuicSignaling`).
- `static/index.html` + `static/app.js` — Loads WASM, shows node id and home relay, **iroh QUIC** vs **WebSocket** signaling mode, WebRTC data-channel line chat.
- `window.SIGNALING_WS_URL` in `index.html` (default `ws://127.0.0.1:3000/ws`) when static (8080) and signaling (3000) run on different ports.

Build WASM into `static/pkg/`:

```bash
bash scripts/build-browser-wasm.sh
```

Requires `wasm32-unknown-unknown` and `wasm-bindgen` (see script).

---

## Signaling modes (browser ↔ native)

1. **iroh QUIC (no WebSocket)**
   - Run `static-server`, open the page, choose **iroh QUIC**, then **Connect** (wait).
   - Run `iroh-jsep-chat` with the printed **node id** and **relay URL**.
   - Native sends the SDP offer over QUIC; the page answers in JavaScript.
2. **WebSocket**
   - Run `signaling-server` and `static-server`.
   - In the page choose **WebSocket room**, then **Connect**; ensure `SIGNALING_WS_URL` matches your signaling server.
   - Pair with `ws-chat`, another tab, or any client that speaks the same join + envelope protocol.

---

## Examples (`examples/`)

- `server` / `client` — Full iroh path: signaling over QUIC, `WebRtcTransport` attach, QUIC app traffic (e.g. line chat) over the custom transport bias path.
- `webrtc_loopback` — Loopback signaling + bridge + datagram echo.
- `chat` — Additional chat-oriented example if present.

---

## Repository layout

```
src/                 # bridge, jsep_envelope, jsep_signaling, jsep_core, jsep_quic, jsep_ws, transport, …
src/bin/             # signaling-server, static-server, ws-chat, iroh-jsep-chat
browser-iroh/        # WASM iroh + JsepQuicSignaling (separate Cargo package)
static/              # index.html, app.js, pkg/ (generated WASM + JS glue)
scripts/build-browser-wasm.sh
```

---

## Quick reference


| Goal                     | Commands                                                                           |
| ------------------------ | ---------------------------------------------------------------------------------- |
| Serve UI                 | `cargo run --bin static-server` → [http://127.0.0.1:8080/](http://127.0.0.1:8080/) |
| WS JSEP relay            | `cargo run --bin signaling-server` → ws://127.0.0.1:3000/ws                        |
| Native WS chat           | `cargo run --bin ws-chat -- ws://127.0.0.1:3000/ws <room>`                         |
| Native QUIC JSEP offerer | `cargo run --bin iroh-jsep-chat -- <node-id> <relay-url>`                          |
| Rebuild browser WASM     | `bash scripts/build-browser-wasm.sh`                                               |


---

## Notes

- **Two peers per room** on the WebSocket signaling server (offer / answer slots).  
- Browser **iroh QUIC** path assumes the **native** side is the **offerer** (`iroh-jsep-chat`); the tab **accepts** the QUIC connection and answers SDP in JS.  
- `static/pkg/` is listed in `.gitignore`; run the WASM build after clone unless you vendor the artifacts.

