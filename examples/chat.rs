//! Two-party chat over Nym mixnet.
//!
//! This demonstrates how two clients can communicate through the mixnet.
//! Run with: cargo run --example chat

use nym_sdk::mixnet::{MixnetClient, MixnetMessageSender, Recipient};
use std::str::FromStr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("alice") => run_alice(args.get(2)).await,
        Some("bob") => run_bob().await,
        _ => {
            println!("Nym Chat Example");
            println!();
            println!("Usage:");
            println!("  1. Start Bob first:    cargo run --example chat bob");
            println!("  2. Copy Bob's address");
            println!("  3. Start Alice:        cargo run --example chat alice <bob-address>");
            println!();
            println!("Alice will send a message to Bob through the mixnet.");
            Ok(())
        }
    }
}

async fn run_bob() -> anyhow::Result<()> {
    println!("Bob: Connecting to Nym mixnet...");

    let mut client = MixnetClient::connect_new().await?;
    let address = client.nym_address();

    println!();
    println!("===========================================");
    println!("Bob's Nym address (copy this for Alice):");
    println!("{address}");
    println!("===========================================");
    println!();
    println!("Bob: Waiting for messages...");

    loop {
        if let Some(messages) = client.wait_for_messages().await {
            for msg in messages {
                let text = String::from_utf8_lossy(&msg.message);
                println!("Bob received: {text}");

                // If Alice said goodbye, we're done
                if text.to_lowercase().contains("bye") {
                    println!("Bob: Goodbye!");
                    client.disconnect().await;
                    return Ok(());
                }
            }
        }
    }
}

async fn run_alice(bob_address: Option<&String>) -> anyhow::Result<()> {
    let bob_address = match bob_address {
        Some(addr) => addr,
        None => {
            anyhow::bail!(
                "Alice needs Bob's address. Usage: cargo run --example chat alice <bob-address>"
            );
        }
    };

    println!("Alice: Connecting to Nym mixnet...");

    let client = MixnetClient::connect_new().await?;
    let our_address = client.nym_address();

    println!("Alice's address: {our_address}");

    // Parse Bob's address
    let recipient = Recipient::from_str(bob_address)?;

    println!("Alice: Sending message to Bob...");

    client
        .send_plain_message(
            recipient,
            "Hello Bob! This message traveled through the Nym mixnet. Bye!",
        )
        .await?;

    println!("Alice: Message sent! (it may take a few seconds to arrive)");

    // Give the message time to be fully sent before disconnecting
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    client.disconnect().await;
    println!("Alice: Done!");

    Ok(())
}
