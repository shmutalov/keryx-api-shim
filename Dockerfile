# Build stage — protoc is needed for the vendored protowire codegen.
FROM rust:1.95-slim-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked

# Runtime — same Debian base as the builder so glibc matches.
FROM debian:bookworm-slim
COPY --from=builder /build/target/release/keryx-api-shim /usr/local/bin/keryx-api-shim
# A loopback bind would be unreachable through container port mapping.
ENV KERYX_SHIM_LISTEN=0.0.0.0:8787
EXPOSE 8787
USER nobody
ENTRYPOINT ["/usr/local/bin/keryx-api-shim"]
