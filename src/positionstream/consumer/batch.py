from typing import Any

import polars as pl
import pyarrow as pa


def transform_clean_batch(
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Transform canonical events through Polars and PyArrow."""
    if not events:
        return []

    frame = pl.DataFrame(events)

    if "symbol" not in frame.columns or "timestamp" not in frame.columns:
        raise ValueError("Batch events must contain symbol and timestamp")

    frame = frame.with_columns(
        pl.col("symbol").str.to_uppercase(),
        pl.col("timestamp").str.to_datetime(
            time_zone="UTC",
            strict=True,
        ),
    )

    arrow_table: pa.Table = frame.to_arrow()
    return arrow_table.to_pylist()
