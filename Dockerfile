# Multi-stage build for RustMQ consumer
# Stage 1: Build
# Explicitly target AMD64 for AWS EC2 deployment
FROM --platform=linux/amd64 rust:1.83-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Copy manifests
COPY Cargo.toml ./

# Create dummy source to cache dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy actual source
COPY src ./src

# Build for release (dependencies are cached)
RUN touch src/main.rs && \
    cargo build --release

# Stage 2: Runtime
FROM --platform=linux/amd64 debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /usr/src/app/target/release/rustmq-consumer /usr/local/bin/

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    chown appuser:appuser /usr/local/bin/rustmq-consumer

USER appuser

# Run the binary
CMD ["rustmq-consumer"]
