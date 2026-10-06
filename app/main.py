"""HTTP API: submit readings, read windows, health and operational state."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime

from .config import Settings
from .engine import BadRequestError, ConflictError, Engine, to_ms
from .models import ReadingIn
from .storage import Storage


def create_app(settings: Settings) -> FastAPI:
    storage = Storage(settings.database_path)
    engine = Engine(storage, settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        storage.close()

    app = FastAPI(
        title="LEO Radiation Monitor",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(ConflictError)
    async def conflict_handler(_request, exc: ConflictError):
        return JSONResponse(
            status_code=409,
            content={"error": exc.code, "message": exc.message, "detail": exc.detail},
        )

    @app.exception_handler(BadRequestError)
    async def bad_request_handler(_request, exc: BadRequestError):
        return JSONResponse(
            status_code=400,
            content={"error": exc.code, "message": exc.message},
        )

    @app.post("/readings")
    def submit_reading(reading: ReadingIn, response: Response):
        """Submit one dose reading.  Identical retransmissions replay the
        original ack (with ``X-Idempotent-Replay: true``); conflicting
        content, reused sequence numbers, and data for sealed windows
        are rejected with 409."""
        ack, replayed = engine.submit(reading)
        if replayed:
            response.headers["X-Idempotent-Replay"] = "true"
        return ack

    @app.get("/windows/{window_start}")
    def get_window(window_start: AwareDatetime):
        """Read the result for the five-minute window containing
        ``window_start`` (aligned down to the window boundary)."""
        return engine.get_window(to_ms(window_start))

    @app.get("/windows")
    def list_windows(since: AwareDatetime | None = None, until: AwareDatetime | None = None):
        """List sealed windows, optionally bounded by ``since``/``until``."""
        return {
            "windows": engine.list_windows(
                to_ms(since) if since else None,
                to_ms(until) if until else None,
            )
        }

    @app.get("/state")
    def state():
        """Operational snapshot: watermark, per-probe progress, counters."""
        return engine.state()

    @app.get("/health")
    def health():
        engine.health_check()
        return {"status": "ok"}

    return app


app = create_app(Settings.from_env())
