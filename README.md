# PositionStream

A portfolio project for building a streaming data pipeline that validates trade and market-price events, detects data-quality issues, and serves portfolio positions and price analytics through an API.

## Project goals

- Ingest synthetic trade and price events from multiple simulated sources.
- Validate and normalize inconsistent data into canonical schemas.
- Detect duplicates, late events, invalid records, and reconciliation breaks.
- Route valid events and rejected events to separate Kafka topics.
- Store time-series data in TimescaleDB and cache current positions in Redis.
- Expose portfolio, OHLC, health, and data-quality endpoints through FastAPI.
- Test reliability, idempotency, and data-quality behavior.

## Planned architecture

- **Python 3.12** — application code
- **Kafka (KRaft)** — event streaming
- **Polars and PyArrow** — data processing
- **TimescaleDB** — time-series storage
- **Redis** — caching and idempotency support
- **FastAPI** — HTTP API
- **Docker Compose** — local development infrastructure
- **Pytest, Hypothesis, Ruff, and mypy** — testing and code quality

## Local setup

### Prerequisites

- Docker Desktop with its Linux container engine running
- Python 3.12, managed through `uv`
- Git

### 1. Configure environment variables

Copy the example configuration:

```powershell
Copy-Item .env.example .env
```

Review `.env` and change the local development password if needed.

### 2. Install dependencies

```powershell
uv sync --group dev
```

### 3. Start infrastructure

```powershell
docker compose up -d
docker compose ps
```

### 4. Run tests

```powershell
uv run pytest
```

The application and infrastructure are being built incrementally. Commands that depend on unfinished components may not work until their implementation is complete.

## Data-quality reason codes

The planned dead-letter queue (DLQ) will use reason codes such as:

- `SCHEMA_INVALID`
- `DUPLICATE`
- `LATE`
- `RECONCILIATION_BREAK`

## API endpoints

Planned endpoints:

- `GET /health`
- `GET /ready`
- `GET /metrics`
- `GET /positions/{portfolio}`
- `GET /prices/{symbol}/ohlc`
- `GET /quality`

## Cloud architecture mapping

The project will document possible AWS equivalents, including Amazon MSK for Kafka, ElastiCache for Redis, and an appropriate PostgreSQL hosting option for TimescaleDB.

These are architectural mappings, not a claim that the project has been deployed to AWS.

## Project status

Under development. See `TASKS.md` for implementation progress.
