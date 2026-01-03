# RustMQ Consumer

Production consumer application for RustMQ message queue on Talos cluster.

## Overview

This Rust application continuously reads messages from RustMQ and processes data from multiple producers:
- **Crypto trades** from Coinbase WebSocket
- **Transit positions** from MTA GTFS-RT API

## Architecture

```
RustMQ (Port 9093) → Consumer → Logs / Processing
```

## Features

- **Continuous polling:** All 12 partitions every 1 second
- **Offset tracking:** Maintains read position per partition
- **Message classification:** Detects crypto vs transit data
- **Performance stats:** Reports consumption rate every 10 seconds
- **Graceful handling:** Skips errors, continues processing

## Protocol

Uses RustMQ's custom consumer protocol:
- Transport: TCP
- Port: 9093
- Request: partition (u32) + offset (u64) + max_bytes (u32)
- Response: length (u32) + bincode(Vec<Message>)

## Message Classification

### Crypto Trades
Detected by: `source: "coinbase_websocket_rust"`

Example:
```json
{
  "event_type": "crypto_trade",
  "source": "coinbase_websocket_rust",
  "data": {
    "symbol": "BTC-USD",
    "price": 45123.45,
    ...
  }
}
```

### Transit Positions
Detected by: `source: "mta_gtfs_rt_rust"`

Example:
```json
{
  "event_type": "vehicle_position",
  "source": "mta_gtfs_rt_rust",
  "data": {
    "vehicle_id": "1234",
    "route_id": "1",
    ...
  }
}
```

## Configuration

Environment variables:
- `RUSTMQ_HOST`: RustMQ consumer server (default: rustmq-0.rustmq-headless.data-pipeline.svc.cluster.local)
- `RUSTMQ_PORT`: Consumer port (default: 9093)
- `POLL_INTERVAL_MS`: Polling interval in milliseconds (default: 1000)
- `RUST_LOG`: Log level (default: rustmq_consumer=info)

## Deployment

### Current Configuration
- **Replicas:** 1
- **Resources:**
  - CPU: 50m request, 200m limit
  - Memory: 64Mi request, 256Mi limit
- **Namespace:** data-pipeline

## Development

### Build Locally
```bash
cargo build --release
```

### Run Locally (requires RustMQ connection)
```bash
RUST_LOG=rustmq_consumer=info cargo run
```

### Docker Build
```bash
docker build -t rustmq-consumer:local .
```

## Sample Output

```
INFO rustmq_consumer: 📥 RustMQ Consumer starting...
INFO rustmq_consumer: ✅ Connected to RustMQ consumer server
INFO rustmq_consumer: ⏳ Starting consumer loop...

INFO rustmq_consumer: 💱 Crypto trade #100: BTC-USD
INFO rustmq_consumer: 💱 Crypto trade #200: ETH-USD
INFO rustmq_consumer: 🚇 Transit position #10: vehicle 1234
INFO rustmq_consumer: 📊 Consumed: 250 total | Crypto: 240 | Transit: 10 | Rate: 25.0 msg/s
```

## Dependencies

- **tokio:** Async runtime
- **bincode:** RustMQ message deserialization
- **serde/serde_json:** Data parsing
- **tracing:** Logging
- **anyhow:** Error handling

## Status

- ✅ Production consumer implementation
- ✅ Updated for AMD64 (AWS EC2)
- ✅ Kubernetes manifest ready
- ⏳ Awaiting deployment

## Links

- **RustMQ:** https://github.com/noelmcmichael/talos-rustmq
- **Crypto Producer:** https://github.com/noelmcmichael/talos-crypto-producer
- **Transit Producer:** https://github.com/noelmcmichael/talos-transit-producer
- **Harbor:** harbor.int-talos-poc.pocketcove.net/library/rustmq-consumer
- **Namespace:** data-pipeline

---

**Status:** Ready to deploy  
**Platform:** Talos Kubernetes v1.31.2  
**Last Updated:** January 2, 2026
