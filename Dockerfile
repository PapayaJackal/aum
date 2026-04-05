# Stage 1: Build frontend
FROM node:22-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Build Rust binaries
FROM rust:slim AS rust-builder
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies by copying manifests first
COPY Cargo.toml Cargo.lock ./
COPY aum-core/Cargo.toml aum-core/
COPY aum-cli/Cargo.toml aum-cli/
COPY aum-api/Cargo.toml aum-api/
COPY aum-macros/Cargo.toml aum-macros/

# Create dummy source files to cache dependencies
RUN mkdir -p aum-core/src aum-cli/src aum-api/src aum-macros/src && \
    echo "fn main() {}" > aum-cli/src/main.rs && \
    echo "fn main() {}" > aum-api/src/main.rs && \
    touch aum-core/src/lib.rs && \
    touch aum-macros/src/lib.rs && \
    cargo build --release --bin aum || true

# Copy actual source and build
COPY aum-core/ aum-core/
COPY aum-cli/ aum-cli/
COPY aum-api/ aum-api/
COPY aum-macros/ aum-macros/
RUN touch aum-core/src/lib.rs aum-macros/src/lib.rs aum-cli/src/main.rs aum-api/src/main.rs && \
    cargo build --release --bin aum

# Stage 3: Runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=rust-builder /app/target/release/aum /usr/local/bin/aum
COPY --from=frontend-builder /app/frontend/dist frontend/dist

ENV AUM_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8000

ENTRYPOINT ["aum"]
CMD ["serve"]
