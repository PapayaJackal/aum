# Stage 1: Build frontend
FROM node:25-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Application
FROM python:3.14-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/

WORKDIR /app

# Install Python dependencies first for layer caching
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy source and install the project
COPY src/ src/
COPY tests/ tests/
RUN uv sync --frozen --no-dev

# Copy built frontend (served at runtime via FastAPI StaticFiles)
COPY --from=frontend-builder /app/frontend/dist frontend/dist

ENV AUM_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8000 9090

ENTRYPOINT ["uv", "run", "aum"]
CMD ["serve"]
