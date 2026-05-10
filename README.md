# iroh-webrtc-transport

Rust library and demos that combine **iroh** (QUIC, custom transports) with **WebRTC** data channels: JSEP signaling can run over **iroh QUIC streams**, **WebSocket**, or **tokio-tungstenite**, then SCTP traffic is bridged into iroh’s custom transport path where configured.

---

## Library (`src/`)


| Piece                                                                                          | Role                                                                                          |
| ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| [Signaling](src/jsep_signaling.rs)                                                             | Async trait: `send_envelope` / `recv_envelope` for [SignalEnvelope](src/jsep_envelope.rs).     |
| [SignalEnvelope](src/jsep_envelope.rs)                                                         | JSON `offer` / `answer` SDP payloads.                                                         |
| [negotiate_dc_as_offerer](src/jsep_core.rs) / [negotiate_dc_as_answerer](src/jsep_core.rs)       | Generic WebRTC negotiation over any `Signaling` impl (native stack: **str0m**).               |
| [QuicSignaling](src/jsep_quic.rs)                                                              | Newline-framed JSON on an iroh **bidi** stream (matches browser WASM framing).                |
| [TcpWebSocket](src/jsep_ws.rs)                                                                 | `Signaling` for `tokio-tungstenite` WebSockets (one JSON text frame per envelope).            |
| [JSEP_SIGNALING_ALPN](src/jsep_alpn.rs)                                                        | Shared ALPN bytes for QUIC JSEP: `iroh-webrtc-transport/signal/0`.                            |
| [WebRtcTunnel](src/bridge.rs) / [WebRtcTransport](src/transport.rs)                            | Bridge SCTP data channel ↔ iroh custom transport (`poll_send` / `poll_recv`, attach, wakers). |


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
- `static/index.html` + `static/app.js` — Loads WASM, shows node id and home relay, **iroh QUIC** (wait / dial) vs **WebSocket** room, WebRTC data-channel line chat.
- `window.SIGNALING_WS_URL` in `index.html` (default `ws://127.0.0.1:3000/ws`) when static (8080) and signaling (3000) run on different ports.

Build WASM into `static/pkg/`:

```bash
bash scripts/build-browser-wasm.sh
```

Requires `wasm32-unknown-unknown` and `wasm-bindgen` (see script).

---

## Signaling modes

### Browser ↔ native

1. **iroh QUIC (no WebSocket)**
   - Run `static-server`, open the page, choose **iroh QUIC — wait for peer**, then **Connect**.
   - Run `iroh-jsep-chat` with the browser’s **node id** and **home relay URL** (shown on the page).
   - Native dials over iroh and sends the SDP offer on QUIC; the page answers in JavaScript.
2. **WebSocket (native peer)**
   - Run `signaling-server` and `static-server`.
   - In the page choose **WebSocket room**, then **Connect**; ensure `SIGNALING_WS_URL` matches your signaling server.
   - Pair with `ws-chat` or any client that speaks the same join + envelope protocol.

### Browser ↔ browser over iroh QUIC (node id + peer relay)

Uses the same JSEP ALPN and newline JSON framing as `iroh-jsep-chat`, but both sides are the static page.

1. Run `static-server` (and rebuild WASM after pull: `bash scripts/build-browser-wasm.sh`).
2. **Answerer tab**: signaling **iroh QUIC — wait for peer** → **Connect**.
3. **Offerer tab**: signaling **iroh QUIC — dial peer**, paste the answerer’s **node id** and **home relay URL** (exact strings from their page) → **Connect**.

The offerer must use the **peer’s** relay URL (not necessarily the same as yours, though often it is under the same N0 preset). Deep link: `http://127.0.0.1:8080/?sig=dial&peer=<z32>&relay=<encoded-relay-url>` (relay must be URL-encoded if it contains special characters).

### Browser ↔ browser (WebSocket room)

The **WebSocket room** path pairs **two browsers** without typing node ids; each runs JSEP + `RTCPeerConnection` in `static/app.js`.

1. Start the relay and static files (from two terminals):

   ```bash
   cargo run --bin signaling-server
   cargo run --bin static-server
   ```

2. Open the UI twice, e.g. [http://127.0.0.1:8080/](http://127.0.0.1:8080/) — two tabs, two windows, or two machines (use a reachable `ws://` or `wss://` in `window.SIGNALING_WS_URL` for cross-host).

3. In **both** tabs: set signaling to **WebSocket room**, use the **same room name** (default `demo`), click **Connect**. The first peer becomes the SDP **offer** role and the second the **answer** role; the server may buffer the offer until the second peer joins.

4. When the data channel opens, type in either tab; lines appear in both logs.

Optional deep link (same room, WS mode): `http://127.0.0.1:8080/?sig=ws&room=demo`

---

## Examples (`examples/`)

- **Browser ↔ browser** — Not a Rust `examples/*` target: `static-server` + two tabs; use **iroh QUIC** wait/dial (node id + peer relay) or **WebSocket room** + `signaling-server` (see [Signaling modes](#signaling-modes)).
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
| Browser ↔ browser (WS)   | `signaling-server` + `static-server`; two tabs, **WebSocket room**, same room, **Connect** |
| Browser ↔ browser (iroh) | `static-server` only; tab A **wait for peer**, tab B **dial peer** with A’s node id + home relay |
| Native WS chat           | `cargo run --bin ws-chat -- ws://127.0.0.1:3000/ws <room>`                         |
| Native QUIC JSEP offerer | `cargo run --bin iroh-jsep-chat -- <node-id> <relay-url>`                          |
| Rebuild browser WASM     | `bash scripts/build-browser-wasm.sh`                                               |


---

## Notes

- **Two peers per room** on the WebSocket signaling server (offer / answer slots); that is enough for **browser ↔ browser** or **browser ↔ `ws-chat`**.  
- Browser **iroh QUIC — wait for peer** answers SDP in JS; the **offerer** can be `iroh-jsep-chat` or another tab using **dial peer** (paste answerer’s node id + **their** home relay).  
- `static/pkg/` is listed in `.gitignore`; run the WASM build after clone unless you vendor the artifacts.

