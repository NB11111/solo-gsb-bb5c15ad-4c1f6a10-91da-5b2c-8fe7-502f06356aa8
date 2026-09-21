"""FastAPI application: ingest, timeline/sample query and reset endpoints."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from . import __version__
from .config import get_settings
from .engine import evaluate_batch, evaluate_sample
from .models import (
    BatchResult,
    HealthResponse,
    IngestResponse,
    MeasurementEvent,
    ResetResponse,
    SamplePointResponse,
)
from .storage import EventStore, IdempotencyConflict, StoredEvent

logger = logging.getLogger("calibration")

settings = get_settings()
store = EventStore()


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    logging.basicConfig(level=settings.log_level.upper())
    logger.info(
        "calibration-timeline-service %s started on %s:%s (storage=memory)",
        __version__,
        settings.host,
        settings.port,
    )
    yield
    logger.info("calibration-timeline-service shutting down")


app = FastAPI(
    title="Factory Calibration Timeline Service",
    version=__version__,
    description=(
        "Ingests out-of-order, overlapping and withdrawable measurement-fragment "
        "revisions for a sensor in a calibration batch and produces a deterministic "
        "effective calibration timeline with backfillable gap reports."
    ),
    lifespan=lifespan,
)


@app.exception_handler(IdempotencyConflict)
async def conflict_handler(_: Request, exc: IdempotencyConflict) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": (
                f"idempotency_key {exc.args[0]!r} was already used with a "
                "different event payload"
            )
        },
    )


@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health() -> HealthResponse:
    return HealthResponse(status="ok", storage="memory", version=__version__)


@app.post(
    "/api/v1/batches/{batch_id}/events",
    response_model=IngestResponse,
    status_code=200,
    tags="calibration",
    summary="Submit one measurement-fragment revision",
)
def submit_event(batch_id: str, event: MeasurementEvent) -> IngestResponse:
    if event.batch_id != batch_id:
        raise HTTPException(
            status_code=422,
            detail="batch_id in path and body must match",
        )
    stored = StoredEvent(
        batch_id=event.batch_id,
        sensor_id=event.sensor_id,
        revision_id=event.revision_id,
        parent_id=event.parent_id,
        idempotency_key=event.idempotency_key,
        start_ns=event.start_ns,
        end_ns=event.end_ns,
        value=None if event.withdrawn else float(event.value),
        unit=event.unit,
        withdrawn=event.withdrawn,
    )
    with store.lock:
        status = store.add(stored)
        batch = store.get_batch(batch_id)
        result = evaluate_batch(batch_id, batch)
    return IngestResponse(
        status=status,
        idempotency_key=event.idempotency_key,
        batch_id=batch_id,
        sensor_id=event.sensor_id,
        revision_id=event.revision_id,
        result=BatchResult(**result),
    )


@app.get(
    "/api/v1/batches/{batch_id}/result",
    response_model=BatchResult,
    tags=["calibration"],
    summary="Query the effective calibration timeline for a batch",
)
def get_result(batch_id: str) -> BatchResult:
    batch = store.get_batch(batch_id)
    if batch is None or not batch.events:
        raise HTTPException(status_code=404, detail=f"batch {batch_id!r} not found")
    with store.lock:
        result = evaluate_batch(batch_id, batch)
    return BatchResult(**result)


@app.get(
    "/api/v1/batches/{batch_id}/sensors/{sensor_id}/sample",
    response_model=SamplePointResponse,
    tags=["calibration"],
    summary="Query the effective calibration state at one nanosecond",
    responses={404: {"description": "Unknown batch or sensor"}},
)
def get_sample(
    batch_id: str,
    sensor_id: str,
    timestamp_ns: int = Query(
        ...,
        ge=0,
        description="Batch-local nanosecond instant to resolve (half-open intervals).",
    ),
) -> SamplePointResponse:
    batch = store.get_batch(batch_id)
    if batch is None or not batch.events:
        raise HTTPException(status_code=404, detail=f"batch {batch_id!r} not found")
    with store.lock:
        doc = evaluate_sample(batch_id, sensor_id, timestamp_ns, batch)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=f"sensor {sensor_id!r} not found in batch {batch_id!r}",
        )
    return SamplePointResponse(**doc)


@app.post(
    "/admin/reset",
    response_model=ResetResponse,
    tags=["ops"],
    summary="Clear all in-memory state",
)
def reset_state() -> ResetResponse:
    batches_removed, events_removed = store.reset()
    logger.warning(
        "in-memory state reset: removed %d events across %d batches",
        events_removed,
        batches_removed,
    )
    return ResetResponse(
        status="reset",
        batches_removed=batches_removed,
        events_removed=events_removed,
    )
