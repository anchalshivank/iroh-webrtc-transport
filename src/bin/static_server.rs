//! Serves `static/` (HTML, `app.js`, `pkg/*.wasm`). No WebRTC signaling here.
//!
//! Run: `cargo run --bin static-server`
//! Then open http://127.0.0.1:8080/
//!
//! For “WebSocket room” mode, run `signaling-server` separately and keep
//! `window.SIGNALING_WS_URL` in `index.html` in sync (default `ws://127.0.0.1:3000/ws`).

use axum::Router;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let static_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/static");
    let app = Router::new()
        .fallback_service(ServeDir::new(static_dir))
        .layer(TraceLayer::new_for_http());

    let addr = "0.0.0.0:8080";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Static site: http://127.0.0.1:8080/");
    println!("WebSocket JSEP (if used): run signaling-server → default ws://127.0.0.1:3000/ws");
    println!("WASM: bash scripts/build-browser-wasm.sh");

    axum::serve(listener, app).await?;
    Ok(())
}
