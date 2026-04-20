# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM rust:1.94-bookworm AS builder

WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p traceowl-proxy


FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /src/target/release/traceowl-proxy /usr/local/bin/traceowl-proxy

RUN chmod +x /usr/local/bin/traceowl-proxy

EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--", "traceowl-proxy"]
