//! WebSocket **JSEP relay only**: forwards JSON SDP offer/answer envelopes between two sockets in a room.
//! Serves **no static files** — use `static-server` for the browser UI.
//!
//! Run: `cargo run --bin signaling-server`
//! Static UI: `cargo run --bin static-server` → http://127.0.0.1:8080/
//! Native chat (WebSocket JSEP): `cargo run --bin ws-chat -- ws://127.0.0.1:3000/ws demo`
//! Native chat (iroh JSEP): static page “iroh QUIC” + `cargo run --bin iroh-jsep-chat -- <node-id> <relay>`

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;

#[derive(Deserialize)]
struct JoinCmd {
    cmd: String,
    room: String,
}

#[derive(Default)]
struct Room {
    to_peer: [Option<tokio::sync::mpsc::UnboundedSender<Message>>; 2],
    buffered_offer: Option<Message>,
}

#[derive(Clone, Default)]
struct AppState {
    rooms: Arc<Mutex<HashMap<String, Room>>>,
}

fn assigned_json(role: &str) -> String {
    json!({ "cmd": "assigned", "role": role }).to_string()
}

fn err_json(msg: &str) -> String {
    json!({ "cmd": "error", "message": msg }).to_string()
}

fn msg_text(m: &Message) -> Option<&str> {
    match m {
        Message::Text(t) => Some(t.as_str()),
        _ => None,
    }
}

fn looks_like_offer(text: &str) -> bool {
    text.trim_start().starts_with("{\"type\":\"offer\"")
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut ws_send, mut ws_recv) = socket.split();
    let (to_me_tx, mut to_me_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

    let first = match ws_recv.next().await {
        Some(Ok(m)) => m,
        _ => return,
    };

    let text = match first {
        Message::Text(t) => t.to_string(),
        Message::Binary(b) => match String::from_utf8(b.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                let _ = ws_send
                    .send(Message::Text(err_json("join must be UTF-8 JSON").into()))
                    .await;
                return;
            }
        },
        _ => {
            let _ = ws_send
                .send(Message::Text(
                    err_json("first message must be join JSON").into(),
                ))
                .await;
            return;
        }
    };

    let join: JoinCmd = match serde_json::from_str::<JoinCmd>(text.trim()) {
        Ok(j) if j.cmd == "join" => j,
        _ => {
            let _ = ws_send
                .send(Message::Text(
                    err_json("expected {\"cmd\":\"join\",\"room\":\"...\"}").into(),
                ))
                .await;
            return;
        }
    };

    let room_key = join.room;
    let my_slot: usize;

    {
        let mut map = state.rooms.lock().await;
        let room = map.entry(room_key.clone()).or_default();

        if room.to_peer[0].is_none() {
            my_slot = 0;
            room.to_peer[0] = Some(to_me_tx.clone());
            let _ = ws_send
                .send(Message::Text(assigned_json("offer").into()))
                .await;
        } else if room.to_peer[1].is_none() {
            my_slot = 1;
            room.to_peer[1] = Some(to_me_tx.clone());
            let _ = ws_send
                .send(Message::Text(assigned_json("answer").into()))
                .await;
            if let Some(buf) = room.buffered_offer.take() {
                let _ = to_me_tx.send(buf);
            }
        } else {
            let _ = ws_send
                .send(Message::Text(err_json("room full (max 2 peers)").into()))
                .await;
            return;
        }
    }

    loop {
        tokio::select! {
            in_msg = ws_recv.next() => {
                let Some(in_msg) = in_msg else { break };
                let Ok(in_msg) = in_msg else { break };
                match in_msg {
                    Message::Text(_) | Message::Binary(_) => {
                        let mut map = state.rooms.lock().await;
                        let Some(room) = map.get_mut(&room_key) else { break };
                        let other = 1 - my_slot;
                        if let Some(tx) = room.to_peer[other].as_ref() {
                            let _ = tx.send(in_msg);
                        } else if my_slot == 0 {
                            if msg_text(&in_msg).is_some_and(|t| looks_like_offer(t)) {
                                room.buffered_offer = Some(in_msg);
                            }
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            out_msg = to_me_rx.recv() => {
                let Some(out_msg) = out_msg else { break };
                if ws_send.send(out_msg).await.is_err() {
                    break;
                }
            }
        }
    }

    let mut map = state.rooms.lock().await;
    if let Some(room) = map.get_mut(&room_key) {
        room.to_peer[my_slot] = None;
        if room.to_peer[0].is_none() && room.to_peer[1].is_none() {
            map.remove(&room_key);
        }
    }
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let st = state.clone();
    ws.on_upgrade(move |socket| handle_socket(socket, st))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let state = Arc::new(AppState::default());
    let app = Router::new()
        .route("/ws", get(ws_upgrade))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("JSEP WebSocket relay: ws://127.0.0.1:3000/ws");
    println!("Static UI: cargo run --bin static-server (http://127.0.0.1:8080/)");
    println!("ws-chat: cargo run --bin ws-chat -- ws://127.0.0.1:3000/ws demo");

    axum::serve(listener, app).await?;
    Ok(())
}
