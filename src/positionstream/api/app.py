from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

import psycopg
import redis
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict

from positionstream.api.config import get_settings
from positionstream.storage.postgres import PostgresStorage
from positionstream.storage.redis_store import RedisStorage

REQUESTS = Counter(
    "positionstream_http_requests_total",
    "Total HTTP requests handled by the API.",
    ["method", "endpoint", "status"],
)
POSITIONS_RETURNED = Gauge(
    "positionstream_positions_returned",
    "Number of portfolio position rows returned by the last successful request.",
)
API_UP = Gauge("positionstream_api_up", "Whether the API process is running.")


class PositionResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    portfolio_id: str
    symbol: str
    quantity: int
    latest_price: Decimal | None
    market_value: Decimal | None
    price_time: datetime | None
    trade_count: int
    quality: str


class OHLCResponse(BaseModel):
    bucket: datetime
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    event_count: int


class HealthResponse(BaseModel):
    status: str
    services: dict[str, str]


def create_app(
    postgres: PostgresStorage | None = None,
    redis_store: RedisStorage | None = None,
) -> FastAPI:
    settings = get_settings()
    database_url = settings.database_url
    redis_url = settings.redis_url

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.postgres = postgres or (PostgresStorage(database_url) if database_url else None)
        app.state.redis = redis_store or RedisStorage(redis_url)
        API_UP.set(1)
        try:
            yield
        finally:
            API_UP.set(0)
            if postgres is None and app.state.postgres is not None:
                close = getattr(app.state.postgres, "close", None)
                if callable(close):
                    close()
            if redis_store is None and app.state.redis is not None:
                app.state.redis.close()

    app = FastAPI(
        title="PositionStream API",
        version="0.1.0",
        description=(
            "Portfolio positions use signed quantities: BUY adds quantity, "
            "SELL subtracts quantity. Market value is signed quantity "
            "multiplied by the latest available price."
        ),
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def count_requests(request: Request, call_next: Any) -> Response:
        response = await call_next(request)
        route = request.scope.get("route")
        endpoint = getattr(route, "path", request.url.path)
        REQUESTS.labels(
            request.method,
            endpoint,
            str(response.status_code),
        ).inc()
        return response

    def get_postgres(request: Request) -> PostgresStorage:
        storage = getattr(request.app.state, "postgres", None)
        if storage is None:
            raise HTTPException(
                status_code=503,
                detail="Database is not configured",
            )
        return storage

    def get_redis(request: Request) -> RedisStorage:
        storage = getattr(request.app.state, "redis", None)
        if storage is None:
            raise HTTPException(
                status_code=503,
                detail="Redis is not configured",
            )
        return storage

    @app.get("/positions/{portfolio}", response_model=list[PositionResponse])
    def get_positions(
        portfolio: Annotated[str, Path(min_length=1, max_length=128)],
        storage: Annotated[PostgresStorage, Depends(get_postgres)],
    ) -> list[dict[str, Any]]:
        try:
            rows = storage.fetch_portfolio_positions()
            positions = [row for row in rows if row["portfolio_id"] == portfolio]
            POSITIONS_RETURNED.set(len(positions))
            return positions
        except (psycopg.Error, OSError) as exc:
            raise HTTPException(
                status_code=503,
                detail="Unable to retrieve portfolio positions",
            ) from exc

    @app.get("/prices/{symbol}/ohlc", response_model=list[OHLCResponse])
    def get_ohlc(
        symbol: Annotated[
            str,
            Path(min_length=1, max_length=5, pattern=r"^[A-Za-z0-9.-]+$"),
        ],
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        storage: Annotated[PostgresStorage, Depends(get_postgres)] = None,  # type: ignore[assignment]
    ) -> list[dict[str, Any]]:
        query = """
            SELECT bucket, symbol, open, high, low, close,
                   event_count::int AS event_count
            FROM price_ohlc_1m
            WHERE symbol = %s
            ORDER BY bucket DESC
            LIMIT %s
        """
        try:
            with psycopg.connect(storage._dsn, row_factory=dict_row) as connection:
                rows = connection.execute(
                    query,
                    (symbol.upper(), limit),
                ).fetchall()
            return [dict(row) for row in reversed(rows)]
        except (psycopg.Error, OSError) as exc:
            raise HTTPException(
                status_code=503,
                detail="Unable to retrieve OHLC prices",
            ) from exc

    @app.get("/quality")
    def get_quality(
        storage: Annotated[RedisStorage, Depends(get_redis)],
    ) -> dict[str, dict[str, int]]:
        try:
            return {"counters": storage.get_quality_counters()}
        except (redis.RedisError, OSError) as exc:
            raise HTTPException(
                status_code=503,
                detail="Unable to retrieve quality counters",
            ) from exc

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok", services={"api": "ok"})

    @app.get("/ready", response_model=HealthResponse)
    def ready(request: Request) -> HealthResponse:
        results: dict[str, str] = {}
        services = {
            "postgres": getattr(request.app.state, "postgres", None),
            "redis": getattr(request.app.state, "redis", None),
        }

        for name, service in services.items():
            if service is None:
                results[name] = "not_configured"
                continue
            try:
                results[name] = "ok" if service.health_check() else "unhealthy"
            except Exception:
                results[name] = "unhealthy"

        if not all(value == "ok" for value in results.values()):
            raise HTTPException(
                status_code=503,
                detail={"status": "not_ready", "services": results},
            )

        return HealthResponse(status="ready", services=results)

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )

    return app


app = create_app()
