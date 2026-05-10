#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/browser-iroh"
cargo build --release --target wasm32-unknown-unknown
wasm-bindgen target/wasm32-unknown-unknown/release/iroh_browser_node.wasm \
  --out-dir "$ROOT/static/pkg" \
  --target web
echo "OK: $ROOT/static/pkg/iroh_browser_node_bg.wasm"
echo "Serve UI: cargo run --bin static-server  → http://127.0.0.1:8080/"
