# Stage 1: Build frontend
FROM node:26-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Build static Rust binary
FROM rust:alpine AS rust-builder
WORKDIR /app

RUN apk add --no-cache \
    musl-dev \
    openssl-dev \
    pkgconfig \
    curl

# Force static OpenSSL linkage so the binary has no dynamic library deps
ENV OPENSSL_STATIC=1

# Cache dependencies by copying manifests first
COPY Cargo.toml Cargo.lock ./
COPY aum-core/Cargo.toml aum-core/
COPY aum-cli/Cargo.toml aum-cli/
COPY aum-api/Cargo.toml aum-api/
COPY aum-macros/Cargo.toml aum-macros/

# Dummy build to cache all Cargo deps (including bundle-frontend's rust-embed)
RUN mkdir -p aum-core/src aum-cli/src aum-api/src aum-macros/src && \
    echo "fn main() {}" > aum-cli/src/main.rs && \
    echo "fn main() {}" > aum-api/src/main.rs && \
    touch aum-core/src/lib.rs && \
    touch aum-macros/src/lib.rs && \
    cargo build --release --bin aum --features bundle-frontend || true

# Copy actual source and embedded frontend assets, then do the real build
COPY aum-core/ aum-core/
COPY aum-cli/ aum-cli/
COPY aum-api/ aum-api/
COPY aum-macros/ aum-macros/
COPY --from=frontend-builder /app/frontend/dist frontend/dist

RUN touch aum-core/src/lib.rs aum-macros/src/lib.rs aum-cli/src/main.rs aum-api/src/main.rs && \
    cargo build --release --bin aum --features bundle-frontend

# Stage 3: Minimal Alpine runtime (musl binary + CA certs only)
FROM alpine:3
RUN apk add --no-cache ca-certificates

COPY --from=rust-builder /app/target/release/aum /usr/local/bin/aum

ENV AUM_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8000

ENTRYPOINT ["aum"]
CMD ["serve"]
