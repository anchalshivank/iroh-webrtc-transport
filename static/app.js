import initWasm, { IrohBrowserNode, initPanicHook } from "./pkg/iroh_browser_node.js";

const DC_LABEL = "chat";

const logEl = document.getElementById("log");
const roomEl = document.getElementById("room");
const sigModeEl = document.getElementById("sig-mode");
const statusEl = document.getElementById("status");
const irohStatusEl = document.getElementById("iroh-status");
const irohRelayEl = document.getElementById("iroh-relay");
const connectBtn = document.getElementById("connect");
const msgEl = document.getElementById("msg");
const sendBtn = document.getElementById("send");
const wsUrlDisplayEl = document.getElementById("ws-url-display");
const peerNodeEl = document.getElementById("peer-node");
const peerRelayEl = document.getElementById("peer-relay");
const irohDialRow = document.getElementById("iroh-dial-row");
const fillPeerRelayBtn = document.getElementById("fill-peer-relay");

/** Keeps the iroh endpoint alive for the tab lifetime. */
let irohNode = null;

function syncSigModeUi() {
  const mode = sigModeEl.value;
  if (irohDialRow) {
    irohDialRow.style.display = mode === "iroh-dial" ? "flex" : "none";
  }
}

function log(line) {
  logEl.textContent += line + "\n";
  logEl.scrollTop = logEl.scrollHeight;
}

function setChatEnabled(on) {
  msgEl.disabled = !on;
  sendBtn.disabled = !on;
}

function wsUrl() {
  if (
    typeof window.SIGNALING_WS_URL === "string" &&
    window.SIGNALING_WS_URL.length > 0
  ) {
    return window.SIGNALING_WS_URL;
  }
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/ws`;
}

function waitIceComplete(pc) {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise((resolve) => {
    const done = () => {
      if (pc.iceGatheringState === "complete") {
        pc.removeEventListener("icegatheringstatechange", done);
        resolve();
      }
    };
    pc.addEventListener("icegatheringstatechange", done);
  });
}

async function startIroh() {
  irohStatusEl.textContent = "loading WASM…";
  irohRelayEl.textContent = "…";
  try {
    await initWasm();
    initPanicHook();
    irohNode = await IrohBrowserNode.connect_n0();
    const id = irohNode.nodeId();
    irohStatusEl.textContent = id;
    const relay = irohNode.homeRelayUrl();
    irohRelayEl.textContent = relay || "(none yet)";
    log(`iroh endpoint online: ${id}`);
    if (relay) {
      log(`Home relay (share with whoever dials you): ${relay}`);
      log(`Native offerer: cargo run --bin iroh-jsep-chat -- ${id} ${relay}`);
      log(`Browser offerer: other tab → “dial peer” and paste this node id + relay.`);
    }
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    irohStatusEl.textContent = "failed (see log)";
    irohRelayEl.textContent = "—";
    log(`iroh WASM failed: ${msg}`);
    log("Build with: bash scripts/build-browser-wasm.sh (needs wasm32 target + wasm-bindgen)");
  }
}

async function main() {
  const params = new URLSearchParams(location.search);
  const qRoom = params.get("room");
  if (qRoom) roomEl.value = qRoom;
  const qSig = params.get("sig");
  if (qSig === "ws") sigModeEl.value = "ws";
  else if (qSig === "iroh") sigModeEl.value = "iroh-accept";
  else if (qSig === "dial") sigModeEl.value = "iroh-dial";
  const qPeer = params.get("peer");
  if (qPeer && peerNodeEl) peerNodeEl.value = qPeer;
  const qRelay = params.get("relay");
  if (qRelay && peerRelayEl) peerRelayEl.value = qRelay;

  if (wsUrlDisplayEl) {
    wsUrlDisplayEl.textContent = wsUrl();
  }

  sigModeEl.addEventListener("change", syncSigModeUi);
  syncSigModeUi();

  if (fillPeerRelayBtn && peerRelayEl) {
    fillPeerRelayBtn.addEventListener("click", () => {
      const r = irohRelayEl.textContent?.trim();
      if (
        r &&
        r !== "…" &&
        r !== "—" &&
        r !== "(none yet)" &&
        !r.includes("failed")
      ) {
        peerRelayEl.value = r;
        log("Filled peer relay from this tab’s home relay (ok when both use the same relay).");
      }
    });
  }

  await startIroh();

  connectBtn.addEventListener("click", () => connect());
  sendBtn.addEventListener("click", () => sendLine());
  msgEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") sendLine();
  });
}

let ws = null;
let dc = null;
let pc = null;

function sendLine() {
  const t = msgEl.value;
  if (!dc || dc.readyState !== "open") return;
  msgEl.value = "";
  dc.send(t + "\n");
}

async function connectViaIrohAccept() {
  if (!irohNode) {
    log("iroh not ready");
    connectBtn.disabled = false;
    return;
  }
  log("\n--- JSEP over iroh QUIC (answerer) — waiting for dialer ---\n");
  statusEl.textContent = "waiting for peer…";
  let sig;
  try {
    sig = await irohNode.acceptJsepSignaling();
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    log(`acceptJsepSignaling: ${msg}`);
    connectBtn.disabled = false;
    statusEl.textContent = "";
    return;
  }
  log("Signaling stream accepted; reading offer…");

  pc = new RTCPeerConnection({
    iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
  });
  pc.addEventListener("iceconnectionstatechange", () =>
    log(`ICE: ${pc.iceConnectionState}`)
  );
  pc.addEventListener("datachannel", (ev) => {
    dc = ev.channel;
    wireDc(dc);
  });

  let offerLine;
  try {
    offerLine = await sig.recvLine();
  } catch (e) {
    log(`recv offer: ${e}`);
    connectBtn.disabled = false;
    statusEl.textContent = "";
    return;
  }
  const offerMsg = JSON.parse(offerLine.trim());
  if (offerMsg.type !== "offer") {
    log(`expected offer, got ${offerMsg.type}`);
    connectBtn.disabled = false;
    return;
  }
  await pc.setRemoteDescription({ type: "offer", sdp: offerMsg.sdp });
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  await waitIceComplete(pc);
  const answerJson =
    JSON.stringify({ type: "answer", sdp: pc.localDescription.sdp }) + "\n";
  try {
    await sig.sendLine(answerJson);
  } catch (e) {
    log(`send answer: ${e}`);
  }
  statusEl.textContent = "jsep done";
  connectBtn.disabled = false;
}

async function connectViaIrohDial() {
  if (!irohNode) {
    log("iroh not ready");
    connectBtn.disabled = false;
    return;
  }
  const peerNode = peerNodeEl?.value?.trim() || "";
  const peerRelay = peerRelayEl?.value?.trim() || "";
  if (!peerNode || !peerRelay) {
    log("Enter the peer’s node id and their home relay URL (both shown on their page).");
    connectBtn.disabled = false;
    return;
  }
  const myId = irohNode.nodeId();
  if (peerNode === myId) {
    log("That node id is this tab — use the other browser’s id.");
    connectBtn.disabled = false;
    return;
  }

  log("\n--- JSEP over iroh QUIC (offerer) — dialing peer ---\n");
  statusEl.textContent = "dialing…";
  let sig;
  try {
    sig = await irohNode.dialJsepSignaling(peerNode, peerRelay);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    log(`dialJsepSignaling: ${msg}`);
    connectBtn.disabled = false;
    statusEl.textContent = "";
    return;
  }
  log("Signaling stream open; sending WebRTC offer…");

  pc = new RTCPeerConnection({
    iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
  });
  pc.addEventListener("iceconnectionstatechange", () =>
    log(`ICE: ${pc.iceConnectionState}`)
  );
  dc = pc.createDataChannel(DC_LABEL, { ordered: true });
  wireDc(dc);

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  await waitIceComplete(pc);
  const offerJson =
    JSON.stringify({ type: "offer", sdp: pc.localDescription.sdp }) + "\n";
  try {
    await sig.sendLine(offerJson);
  } catch (e) {
    log(`send offer: ${e}`);
    connectBtn.disabled = false;
    statusEl.textContent = "";
    return;
  }

  let answerLine;
  try {
    answerLine = await sig.recvLine();
  } catch (e) {
    log(`recv answer: ${e}`);
    connectBtn.disabled = false;
    statusEl.textContent = "";
    return;
  }
  const answerMsg = JSON.parse(answerLine.trim());
  if (answerMsg.type !== "answer") {
    log(`expected answer, got ${answerMsg.type}`);
    connectBtn.disabled = false;
    return;
  }
  await pc.setRemoteDescription({ type: "answer", sdp: answerMsg.sdp });
  statusEl.textContent = "jsep done";
  connectBtn.disabled = false;
}

async function connectViaWs() {
  const room = roomEl.value.trim() || "demo";
  ws = new WebSocket(wsUrl());

  ws.addEventListener("open", () => {
    ws.send(JSON.stringify({ cmd: "join", room }));
    log(`paired in room "${room}" (JSEP over WebSocket)`);
  });

  ws.addEventListener("close", () => {
    statusEl.textContent = "disconnected";
    connectBtn.disabled = false;
    setChatEnabled(false);
  });

  ws.addEventListener("error", () => log("WebSocket error"));

  ws.addEventListener("message", async (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      log("non-JSON ws message");
      return;
    }

    if (msg.cmd === "error") {
      log("Error: " + (msg.message || ""));
      connectBtn.disabled = false;
      return;
    }

    if (msg.cmd === "assigned") {
      statusEl.textContent = msg.role;
      log(`JSEP role: ${msg.role}`);
      if (msg.role === "offer") {
        log(
          "Browser↔browser: open this page in another tab (same room), choose WebSocket room, Connect."
        );
      } else {
        log("Answerer: offer will arrive from the other tab (or ws-chat).");
      }
      pc = new RTCPeerConnection({
        iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
      });

      pc.addEventListener("iceconnectionstatechange", () =>
        log(`ICE: ${pc.iceConnectionState}`)
      );

      if (msg.role === "offer") {
        dc = pc.createDataChannel(DC_LABEL, { ordered: true });
        wireDc(dc);
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        await waitIceComplete(pc);
        ws.send(
          JSON.stringify({ type: "offer", sdp: pc.localDescription.sdp })
        );
      } else {
        pc.addEventListener("datachannel", (ev) => {
          dc = ev.channel;
          wireDc(dc);
        });
      }
      return;
    }

    if (!pc) return;

    if (msg.type === "offer") {
      await pc.setRemoteDescription({ type: "offer", sdp: msg.sdp });
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      await waitIceComplete(pc);
      ws.send(
        JSON.stringify({ type: "answer", sdp: pc.localDescription.sdp })
      );
      return;
    }

    if (msg.type === "answer") {
      await pc.setRemoteDescription({ type: "answer", sdp: msg.sdp });
    }
  });
}

async function connect() {
  connectBtn.disabled = true;
  statusEl.textContent = "";
  setChatEnabled(false);
  const mode = sigModeEl.value;
  if (mode === "iroh-accept") {
    await connectViaIrohAccept();
  } else if (mode === "iroh-dial") {
    await connectViaIrohDial();
  } else {
    log("\n--- JSEP over WebSocket ---\n");
    await connectViaWs();
  }
}

function wireDc(channel) {
  channel.addEventListener("open", () => {
    log("Data channel open");
    setChatEnabled(true);
    msgEl.focus();
  });
  channel.addEventListener("close", () => {
    log("Data channel closed");
    setChatEnabled(false);
  });
  channel.addEventListener("message", (ev) => {
    const text = typeof ev.data === "string" ? ev.data : new TextDecoder().decode(ev.data);
    log("[peer] " + text.replace(/\n$/, ""));
  });
}

main();
