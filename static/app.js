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

/** Keeps the iroh endpoint alive for the tab lifetime. */
let irohNode = null;

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
      log(`relay for native dial: ${relay}`);
      log(`run: cargo run --bin iroh-jsep-chat -- ${id} ${relay}`);
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
  if (qSig === "ws" || qSig === "iroh") sigModeEl.value = qSig;

  if (wsUrlDisplayEl) {
    wsUrlDisplayEl.textContent = wsUrl();
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

async function connectViaIrohQuic() {
  if (!irohNode) {
    log("iroh not ready");
    connectBtn.disabled = false;
    return;
  }
  log("\n--- JSEP over iroh QUIC (answerer); open native offerer ---\n");
  statusEl.textContent = "waiting for peer…";
  let sig;
  try {
    sig = await irohNode.acceptJsepSignaling();
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    log(`accept_jsep_signaling: ${msg}`);
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
  if (mode === "iroh") {
    await connectViaIrohQuic();
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
