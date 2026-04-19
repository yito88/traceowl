# Installation

## TraceOwl Proxy/Diff

### Prerequisites

- Rust toolchain (for building from source)
- Docker (optional, for running in a container)

### Build binaries

```bash
# Build both binaries from the workspace root
cargo build --release -p traceowl-proxy -p traceowl-diff

# Binaries land at:
#   target/release/traceowl-proxy
#   target/release/traceowl-diff
```

### Build Docker image (Proxy)

```bash
docker build -f crates/traceowl-proxy/Dockerfile -t traceowl-proxy .
```

## TraceOwl Analyzer (Commercial)

TraceOwl Analyzer is provided as a Docker image.

It is a commercial product. You can request a trial or purchase a license by contacting the TraceOwl team.
