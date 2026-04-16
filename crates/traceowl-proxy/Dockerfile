# syntax=docker/dockerfile:1.7

FROM rust:1.94-bookworm AS builder

WORKDIR /src

COPY . .

RUN cargo build --release -p traceowl-proxy


FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /data/output /etc/traceowl

COPY --from=builder /src/target/release/traceowl-proxy /usr/local/bin/traceowl-proxy

RUN chmod +x /usr/local/bin/traceowl-proxy

EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--", "traceowl-proxy", "/etc/traceowl/config.toml"]
