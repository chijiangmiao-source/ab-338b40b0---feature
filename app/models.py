"""Request schemas for the HTTP API."""
from __future__ import annotations

from pydantic import AwareDatetime, BaseModel, Field


class ReadingIn(BaseModel):
    """One dose reading submitted by a probe.

    - ``event_id`` is the stable event identifier used for idempotent
      retransmission: same id + same content replays the original ack,
      same id + different content is a conflict.
    - ``seq`` is the per-probe strictly increasing sequence number.
    - ``observed_at`` must be timezone-aware.
    """

    event_id: str = Field(min_length=1, max_length=128)
    probe: str = Field(min_length=1, max_length=64)
    seq: int = Field(ge=1)
    observed_at: AwareDatetime
    dose: float = Field(ge=0.0, le=1e9)
