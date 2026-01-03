// RustMQ Consumer - Production consumer for Talos cluster
// Continuously reads messages from RustMQ and displays data from crypto and transit producers

use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;
use std::env;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info, warn};

// RustMQ Message structure
#[derive(Debug, Deserialize)]
struct RustMQMessage {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u64,
    #[serde(default)]
    headers: Vec<(String, String)>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("rustmq_consumer=info")
        .init();

    info!("📥 RustMQ Consumer starting...");
    
    // Read configuration from environment
    let server_host = env::var("RUSTMQ_HOST")
        .unwrap_or_else(|_| "rustmq-0.rustmq-headless.data-pipeline.svc.cluster.local".to_string());
    let server_port = env::var("RUSTMQ_PORT")
        .unwrap_or_else(|_| "9093".to_string())
        .parse::<u16>()
        .unwrap_or(9093);
    let poll_interval_ms = env::var("POLL_INTERVAL_MS")
        .unwrap_or_else(|_| "1000".to_string())
        .parse::<u64>()
        .unwrap_or(1000);
    
    let addr = format!("{}:{}", server_host, server_port);
    
    info!("Configuration:");
    info!("  Server: {}", addr);
    info!("  Poll interval: {}ms", poll_interval_ms);
    info!("  Partitions: 0-11 (12 total)");
    info!("");

    info!("Connecting to {}...", addr);
    let mut stream = TcpStream::connect(&addr).await?;
    info!("✅ Connected to RustMQ consumer server");
    info!("⏳ Starting consumer loop...\n");

    // Track state for each partition
    let mut partition_offsets = vec![0u64; 12];
    let mut total_consumed = 0u64;
    let mut crypto_count = 0u64;
    let mut transit_count = 0u64;
    let mut unknown_count = 0u64;
    let mut last_status = Instant::now();

    // Main consumer loop
    loop {
        let mut batch_consumed = 0u64;

        // Poll each partition
        for partition in 0..12 {
            let current_offset = partition_offsets[partition];
            
            match consume_messages(&mut stream, partition, current_offset, 100).await {
                Ok(messages) => {
                    if !messages.is_empty() {
                        batch_consumed += messages.len() as u64;
                        
                        // Process and classify messages
                        for msg in &messages {
                            let value_str = String::from_utf8_lossy(&msg.value);
                            
                            // Try to parse as JSON and detect type
                            if let Ok(json) = serde_json::from_str::<Value>(&value_str) {
                                if let Some(source) = json.get("source").and_then(|s| s.as_str()) {
                                    if source.contains("coinbase") {
                                        crypto_count += 1;
                                        if crypto_count % 100 == 0 {
                                            info!("💱 Crypto trade #{}: {}", crypto_count, 
                                                json.get("data")
                                                    .and_then(|d| d.get("symbol"))
                                                    .and_then(|s| s.as_str())
                                                    .unwrap_or("unknown"));
                                        }
                                    } else if source.contains("mta") {
                                        transit_count += 1;
                                        if transit_count % 10 == 0 {
                                            info!("🚇 Transit position #{}: vehicle {}", transit_count,
                                                json.get("data")
                                                    .and_then(|d| d.get("vehicle_id"))
                                                    .and_then(|s| s.as_str())
                                                    .unwrap_or("unknown"));
                                        }
                                    } else {
                                        unknown_count += 1;
                                    }
                                }
                            } else {
                                unknown_count += 1;
                            }
                        }
                        
                        // Update offset for this partition
                        partition_offsets[partition] += messages.len() as u64;
                    }
                }
                Err(e) => {
                    warn!("⚠️  Partition {}: {}", partition, e);
                }
            }
        }

        total_consumed += batch_consumed;

        // Status update every 10 seconds
        if last_status.elapsed().as_secs() >= 10 {
            let rate = total_consumed as f64 / last_status.elapsed().as_secs_f64();
            info!("📊 Consumed: {} total | Crypto: {} | Transit: {} | Rate: {:.1} msg/s", 
                total_consumed, crypto_count, transit_count, rate);
            last_status = Instant::now();
        }

        // Sleep before next poll
        tokio::time::sleep(tokio::time::Duration::from_millis(poll_interval_ms)).await;
    }
}

async fn consume_messages(
    stream: &mut TcpStream,
    partition: u32,
    offset: u64,
    max_messages: u32,
) -> Result<Vec<RustMQMessage>> {
    // Send request: partition (u32) + offset (u64) + max_bytes (u32)
    // Assume ~500 bytes per message on average
    let max_bytes = max_messages * 500;
    
    stream.write_u32(partition).await?;
    stream.write_u64(offset).await?;
    stream.write_u32(max_bytes).await?;
    
    // Read response: length (u32) + bincode(Vec<Message>)
    let response_len = stream.read_u32().await?;
    
    if response_len == 0 {
        // No messages available
        return Ok(Vec::new());
    }
    
    let mut response_bytes = vec![0u8; response_len as usize];
    stream.read_exact(&mut response_bytes).await?;
    
    // Deserialize messages
    let messages: Vec<RustMQMessage> = bincode::deserialize(&response_bytes)?;
    
    Ok(messages)
}
