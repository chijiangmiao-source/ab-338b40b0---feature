"""Core monitoring engine.

Invariants enforced here:

* Idempotent retransmission — ``event_id`` is the stable identity.  A
  retransmission with identical content replays the originally stored
  ack verbatim; identical id with different content is a conflict.
* Watermark honesty — a probe only contributes the max observed time of
  its *contiguous* sequence prefix (no gaps, starting at seq 1).  The
  watermark is ``min(frontier_alpha, frontier_beta) - allowed_lateness``
  and only exists once both probes have reported.  Sequence jumps,
  regressions, or an absent probe can never fabricate a watermark.
* Transactional sealing — a window is sealed (total, peak, level, first
  violation, both probes' progress) only when its end is not later than
  the watermark, and always inside the same persistent transaction as
  the reading that made it sealable.  ``window_start_ms`` is the primary
  key, so concurrent submissions and restarts leave exactly one record.
* Incident continuity — consecutive sealed ELEVATED/CRITICAL windows form one
  continuous irradiation incident.  The unique open incident is advanced
  in window-start order inside the sealing transaction: an
  ELEVATED/CRITICAL window opens or extends it, and the next sealed
  NORMAL window (including an empty window the watermark crossed)
  ends it.  Ended incidents are immutable; late readings, replays, and
  window reads can never rewrite them.
* Immutability — a reading whose observation falls into an already
  sealed window is rejected and changes nothing.
"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone

from .config import Settings
from .models import ReadingIn
from .storage import Storage

LEVELS = ("NORMAL", "ELEVATED", "CRITICAL")


class ConflictError(Exception):
    """Raised for 409-class rejections (event conflict, sealed window, seq reuse)."""

    def __init__(self, code: str, message: str, detail: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}


class BadRequestError(Exception):
    """Raised for 400-class rejections (e.g. unknown probe)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def to_ms(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def _content_hash(reading: ReadingIn, observed_ms: int) -> str:
    payload = {
        "event_id": reading.event_id,
        "probe": reading.probe,
        "seq": reading.seq,
        "observed_at_ms": observed_ms,
        "dose": reading.dose,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


class Engine:
    def __init__(self, storage: Storage, settings: Settings):
        self.storage = storage
        self.settings = settings

    # ------------------------------------------------------------------ ingest

    def submit(self, reading: ReadingIn) -> tuple[dict, bool]:
        """Ingest one reading.  Returns ``(ack, replayed)``.

        Raises :class:`BadRequestError` for an unknown probe and
        :class:`ConflictError` for event conflicts, sequence reuse, or
        data landing in an already sealed window.
        """
        s = self.settings
        if reading.probe not in s.probes:
            raise BadRequestError(
                "unknown_probe",
                f"probe {reading.probe!r} is not configured; expected one of {sorted(s.probes)}",
            )
        observed_ms = to_ms(reading.observed_at)
        content_hash = _content_hash(reading, observed_ms)
        window_start = (observed_ms // s.window_ms) * s.window_ms
        now_ms = round(time.time() * 1000)

        with self.storage.write_txn() as conn:
            row = conn.execute(
                "SELECT * FROM readings WHERE event_id = ?", (reading.event_id,)
            ).fetchone()
            if row is not None:
                if row["content_hash"] == content_hash:
                    # Identical retransmission: replay the original ack verbatim.
                    return json.loads(row["ack_json"]), True
                raise ConflictError(
                    "event_conflict",
                    f"event_id {reading.event_id!r} already exists with different content",
                    detail={
                        "stored": {
                            "probe": row["probe"],
                            "seq": row["seq"],
                            "observed_at": iso(row["observed_at_ms"]),
                            "dose": row["dose"],
                        },
                        "received": {
                            "probe": reading.probe,
                            "seq": reading.seq,
                            "observed_at": iso(observed_ms),
                            "dose": reading.dose,
                        },
                    },
                )

            seq_owner = conn.execute(
                "SELECT event_id FROM readings WHERE probe = ? AND seq = ?",
                (reading.probe, reading.seq),
            ).fetchone()
            if seq_owner is not None:
                raise ConflictError(
                    "seq_conflict",
                    f"probe {reading.probe!r} seq {reading.seq} is already bound to "
                    f"event {seq_owner['event_id']!r}",
                )

            sealed = conn.execute(
                "SELECT 1 FROM windows WHERE window_start_ms = ?", (window_start,)
            ).fetchone()
            if sealed is not None:
                raise ConflictError(
                    "window_sealed",
                    f"window starting {iso(window_start)} is already sealed; "
                    "late reading rejected without altering the published summary",
                    detail={"window_start": iso(window_start), "reason": "sealed"},
                )
            # Watermark gate: a window whose end is at or before the current
            # watermark is closed even if no physical record exists for it
            # (e.g. it predates the first observed event).  Late packets can
            # never create or rewrite a published risk level.
            current_wm = self._watermark(conn)
            if current_wm is not None and window_start + s.window_ms <= current_wm:
                raise ConflictError(
                    "window_sealed",
                    f"window starting {iso(window_start)} ended at "
                    f"{iso(window_start + s.window_ms)}, at or before the current "
                    f"watermark {iso(current_wm)}; late reading rejected",
                    detail={
                        "window_start": iso(window_start),
                        "watermark": iso(current_wm),
                        "reason": "past_watermark",
                    },
                )

            conn.execute(
                "INSERT INTO readings (event_id, probe, seq, observed_at_ms, dose,"
                " content_hash, ack_json, received_at_ms) VALUES (?,?,?,?,?,?,?,?)",
                (
                    reading.event_id,
                    reading.probe,
                    reading.seq,
                    observed_ms,
                    reading.dose,
                    content_hash,
                    "{}",
                    now_ms,
                ),
            )

            self._advance_probe(conn, reading.probe, reading.seq, observed_ms, now_ms)
            watermark_ms = self._watermark(conn)
            sealed_summaries = self._seal_windows(conn, watermark_ms, now_ms)
            ack = {
                "event_id": reading.event_id,
                "probe": reading.probe,
                "seq": reading.seq,
                "status": "accepted",
                "window_start": iso(window_start),
                "window_end": iso(window_start + s.window_ms),
                "watermark": iso(watermark_ms),
                "sealed_windows": sealed_summaries,
                "probe_progress": self._progress(conn),
            }
            # Persist the ack so identical retransmissions replay it exactly,
            # even after a process restart.
            conn.execute(
                "UPDATE readings SET ack_json = ? WHERE event_id = ?",
                (json.dumps(ack, sort_keys=True), reading.event_id),
            )
            return ack, False

    def _advance_probe(self, conn, probe: str, seq: int, observed_ms: int, now_ms: int) -> None:
        row = conn.execute(
            "SELECT * FROM probe_state WHERE probe = ?", (probe,)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO probe_state (probe, contiguous_seq, frontier_time_ms,"
                " max_seq, updated_at_ms) VALUES (?,?,?,?,?)",
                (probe, 0, None, 0, now_ms),
            )
            contiguous, frontier, max_seq = 0, None, 0
        else:
            contiguous, frontier, max_seq = (
                row["contiguous_seq"],
                row["frontier_time_ms"],
                row["max_seq"],
            )

        max_seq = max(max_seq, seq)
        if seq > contiguous:
            # Extend the contiguous prefix as far as buffered readings allow.
            # A gap simply leaves the frontier where it was: no fabricated
            # progress from out-of-order or missing sequence numbers.
            cur = contiguous
            while True:
                nxt = conn.execute(
                    "SELECT observed_at_ms FROM readings WHERE probe = ? AND seq = ?",
                    (probe, cur + 1),
                ).fetchone()
                if nxt is None:
                    break
                cur += 1
                frontier = nxt["observed_at_ms"] if frontier is None else max(
                    frontier, nxt["observed_at_ms"]
                )
            contiguous = cur

        conn.execute(
            "UPDATE probe_state SET contiguous_seq = ?, frontier_time_ms = ?,"
            " max_seq = ?, updated_at_ms = ? WHERE probe = ?",
            (contiguous, frontier, max_seq, now_ms, probe),
        )

    def _watermark(self, conn) -> int | None:
        rows = conn.execute("SELECT probe, frontier_time_ms FROM probe_state").fetchall()
        frontiers = {r["probe"]: r["frontier_time_ms"] for r in rows}
        times = []
        for probe in self.settings.probes:
            t = frontiers.get(probe)
            if t is None:
                return None  # probe absent or only gapped data: no watermark
            times.append(t)
        return min(times) - self.settings.lateness_ms

    # ----------------------------------------------------------------- sealing

    def _seal_windows(self, conn, watermark_ms: int | None, now_ms: int) -> list[dict]:
        if watermark_ms is None:
            return []
        s = self.settings
        first = conn.execute("SELECT MIN(observed_at_ms) AS m FROM readings").fetchone()["m"]
        if first is None:
            return []
        start = (first // s.window_ms) * s.window_ms
        sealed = []
        w = start
        # Only windows whose end is not later than the watermark may seal.
        while w + s.window_ms <= watermark_ms:
            exists = conn.execute(
                "SELECT 1 FROM windows WHERE window_start_ms = ?", (w,)
            ).fetchone()
            if exists is None:
                summary = self._seal_one(conn, w, watermark_ms, now_ms)
                sealed.append(summary)
                # Advance the single open incident within the *same* transaction,
                # so windows and incidents commit atomically — concurrency and
                # restarts can never leave a half-advanced incident.
                self._advance_incident(conn, w, summary, now_ms)
            w += s.window_ms
        return sealed

    def _seal_one(self, conn, window_start: int, watermark_ms: int, now_ms: int) -> dict:
        s = self.settings
        rows = conn.execute(
            "SELECT event_id, probe, seq, observed_at_ms, dose FROM readings"
            " WHERE observed_at_ms >= ? AND observed_at_ms < ?"
            " ORDER BY observed_at_ms, probe, seq",
            (window_start, window_start + s.window_ms),
        ).fetchall()
        total = sum(r["dose"] for r in rows)
        peak = max((r["dose"] for r in rows), default=0.0)
        level = self._level(total, peak)
        violation = self._first_violation(rows) if level != "NORMAL" else None
        progress = self._progress(conn)
        conn.execute(
            "INSERT INTO windows (window_start_ms, window_end_ms, total_dose, peak_dose,"
            " event_count, level, first_violation_json, watermark_ms, progress_json,"
            " sealed_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                window_start,
                window_start + s.window_ms,
                total,
                peak,
                len(rows),
                level,
                json.dumps(violation) if violation else None,
                watermark_ms,
                json.dumps(progress, sort_keys=True),
                now_ms,
            ),
        )
        return {
            "window_start": iso(window_start),
            "window_end": iso(window_start + s.window_ms),
            "level": level,
            "total_dose": total,
            "peak_dose": peak,
            "event_count": len(rows),
        }

    def _level(self, total: float, peak: float) -> str:
        s = self.settings
        if total >= s.critical_total or peak >= s.critical_peak:
            return "CRITICAL"
        if total >= s.elevated_total or peak >= s.elevated_peak:
            return "ELEVATED"
        return "NORMAL"

    def _first_violation(self, rows) -> dict | None:
        """First event (observation order) that pushed the window out of NORMAL.

        Either its own dose breaches the elevated peak threshold, or the
        running total crosses the elevated total threshold at that event.
        """
        s = self.settings
        running = 0.0
        for r in rows:
            running += r["dose"]
            if r["dose"] >= s.elevated_peak or running >= s.elevated_total:
                return {
                    "event_id": r["event_id"],
                    "probe": r["probe"],
                    "seq": r["seq"],
                    "observed_at": iso(r["observed_at_ms"]),
                    "dose": r["dose"],
                }
        return None

    # ---------------------------------------------------------------- incidents

    def _open_incident(self, conn) -> object | None:
        """The at-most-one incident that is still ``ongoing`` (or None)."""
        return conn.execute(
            "SELECT * FROM incidents WHERE status = 'ongoing'"
        ).fetchone()

    def _advance_incident(self, conn, window_start: int, summary: dict, now_ms: int) -> None:
        """Fold one freshly sealed window into the incident stream.

        Driven strictly in window-start order, inside the sealing
        transaction.  An ELEVATED/CRITICAL window creates the unique open
        incident or extends it; the next NORMAL window (empty
        watermark-crossed windows included) closes it.  Windows are
        sealed at most once, so this runs at most once per window;
        nothing afterwards can reopen an ended incident.
        """
        level = summary["level"]
        window_end = window_start + self.settings.window_ms
        incident = self._open_incident(conn)
        if level != "NORMAL":
            if incident is None:
                conn.execute(
                    "INSERT INTO incidents (incident_start_ms, last_window_start_ms,"
                    " status, window_count, total_dose, peak_dose, highest_level,"
                    " closed_at_ms, updated_at_ms) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        window_start,
                        window_start,
                        "ongoing",
                        1,
                        summary["total_dose"],
                        summary["peak_dose"],
                        level,
                        None,
                        now_ms,
                    ),
                )
            else:
                highest = self._higher_level(incident["highest_level"], level)
                conn.execute(
                    "UPDATE incidents SET last_window_start_ms = ?,"
                    " window_count = ?, total_dose = ?, peak_dose = ?,"
                    " highest_level = ?, updated_at_ms = ?"
                    " WHERE incident_start_ms = ? AND status = 'ongoing'",
                    (
                        window_start,
                        incident["window_count"] + 1,
                        incident["total_dose"] + summary["total_dose"],
                        max(incident["peak_dose"], summary["peak_dose"]),
                        highest,
                        now_ms,
                        incident["incident_start_ms"],
                    ),
                )
        elif incident is not None:
            # A NORMAL window terminates the contiguous run — including an
            # empty window that the watermark merely crossed.
            conn.execute(
                "UPDATE incidents SET status = 'ended', closed_at_ms = ?,"
                " updated_at_ms = ? WHERE incident_start_ms = ? AND status = 'ongoing'",
                (window_end, now_ms, incident["incident_start_ms"]),
            )

    @staticmethod
    def _higher_level(a: str, b: str) -> str:
        rank = {"NORMAL": 0, "ELEVATED": 1, "CRITICAL": 2}
        return a if rank[a] >= rank[b] else b

    def reconcile_incidents(self) -> None:
        """Backfill incident from already-sealed windows on startup.

        Only runs when no incident row exists yet (fresh feature
        rollout over a volume that already has sealed windows), and
        replays the same in-order state machine.  Once any incident
        exists, incremental sealing already keeps the table correct, so
        this never rewrites or duplicates anything.
        """
        with self.storage.write_txn() as conn:
            present = conn.execute("SELECT 1 FROM incidents LIMIT 1").fetchone()
            if present is not None:
                return
            rows = conn.execute(
                "SELECT window_start_ms, window_end_ms, total_dose,"
                " peak_dose, event_count, level, sealed_at_ms"
                " FROM windows ORDER BY window_start_ms"
            ).fetchall()
            now_ms = max((r["sealed_at_ms"] for r in rows), default=0)
            for r in rows:
                summary = {
                    "total_dose": r["total_dose"],
                    "peak_dose": r["peak_dose"],
                    "event_count": r["event_count"],
                    "level": r["level"],
                }
                self._advance_incident(conn, r["window_start_ms"], summary, now_ms)

    # ------------------------------------------------------------------ reads

    def _progress(self, conn) -> dict:
        rows = conn.execute("SELECT * FROM probe_state").fetchall()
        by_probe = {r["probe"]: r for r in rows}
        out = {}
        for probe in self.settings.probes:
            r = by_probe.get(probe)
            out[probe] = {
                "contiguous_seq": r["contiguous_seq"] if r else 0,
                "frontier_time": iso(r["frontier_time_ms"])
                if r and r["frontier_time_ms"] is not None
                else None,
                "max_seq": r["max_seq"] if r else 0,
            }
        return out

    def _window_events(self, conn, window_start: int) -> list[dict]:
        rows = conn.execute(
            "SELECT event_id, probe, seq, observed_at_ms, dose FROM readings"
            " WHERE observed_at_ms >= ? AND observed_at_ms < ?"
            " ORDER BY observed_at_ms, probe, seq",
            (window_start, window_start + self.settings.window_ms),
        ).fetchall()
        return [
            {
                "event_id": r["event_id"],
                "probe": r["probe"],
                "seq": r["seq"],
                "observed_at": iso(r["observed_at_ms"]),
                "dose": r["dose"],
            }
            for r in rows
        ]

    def get_window(self, when_ms: int) -> dict:
        """Read one window.  Sealed windows return the immutable published
        record; open windows return a clearly-marked provisional view."""
        s = self.settings
        w = (when_ms // s.window_ms) * s.window_ms
        with self.storage.read_txn() as conn:
            row = conn.execute(
                "SELECT * FROM windows WHERE window_start_ms = ?", (w,)
            ).fetchone()
            events = self._window_events(conn, w)
            if row is not None:
                return {
                    "window_start": iso(w),
                    "window_end": iso(w + s.window_ms),
                    "status": "sealed",
                    "final": True,
                    "level": row["level"],
                    "total_dose": row["total_dose"],
                    "peak_dose": row["peak_dose"],
                    "event_count": row["event_count"],
                    "first_violation": json.loads(row["first_violation_json"])
                    if row["first_violation_json"]
                    else None,
                    "progress_at_seal": json.loads(row["progress_json"]),
                    "watermark_at_seal": iso(row["watermark_ms"]),
                    "sealed_at": iso(row["sealed_at_ms"]),
                    "events": events,
                }
            total = sum(e["dose"] for e in events)
            peak = max((e["dose"] for e in events), default=0.0)
            watermark_ms = self._watermark(conn)
            return {
                "window_start": iso(w),
                "window_end": iso(w + s.window_ms),
                "status": "open",
                "final": False,
                # a window at or behind the watermark no longer accepts data,
                # even if it was never physically sealed (empty, pre-anchor)
                "accepting": watermark_ms is None or (w + s.window_ms) > watermark_ms,
                "level": self._level(total, peak),
                "total_dose": total,
                "peak_dose": peak,
                "event_count": len(events),
                "first_violation": None,
                "progress_at_seal": None,
                "watermark_at_seal": None,
                "sealed_at": None,
                "current_watermark": iso(watermark_ms),
                "events": events,
            }

    def list_windows(self, since_ms: int | None, until_ms: int | None) -> list[dict]:
        query = (
            "SELECT window_start_ms, window_end_ms, level, total_dose, peak_dose,"
            " event_count, sealed_at_ms FROM windows"
        )
        clauses, params = [], []
        if since_ms is not None:
            clauses.append("window_start_ms >= ?")
            params.append(since_ms)
        if until_ms is not None:
            clauses.append("window_start_ms <= ?")
            params.append(until_ms)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY window_start_ms"
        with self.storage.read_txn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "window_start": iso(r["window_start_ms"]),
                "window_end": iso(r["window_end_ms"]),
                "status": "sealed",
                "level": r["level"],
                "total_dose": r["total_dose"],
                "peak_dose": r["peak_dose"],
                "event_count": r["event_count"],
                "sealed_at": iso(r["sealed_at_ms"]),
            }
            for r in rows
        ]

    def list_incidents(
        self,
        since_ms: int | None = None,
        until_ms: int | None = None,
        status: str | None = None,
    ) -> list[dict]:
        """Incidents with their constituent windows, sorted by event start.

        Only sealed windows are grouped, and ended incidents are never
        recomputed here: every aggregate is read straight from the
        immutable ``incidents`` row, so late readings and replays
        cannot change a finished incident.
        """
        clauses, params = [], []
        if since_ms is not None:
            clauses.append("i.incident_start_ms >= ?")
            params.append(since_ms)
        if until_ms is not None:
            clauses.append("i.incident_start_ms <= ?")
            params.append(until_ms)
        if status is not None:
            clauses.append("i.status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        query = (
            "SELECT i.*, w.window_start_ms, w.window_end_ms, w.level,"
            " w.total_dose AS w_total_dose, w.peak_dose AS w_peak_dose,"
            " w.event_count"
            " FROM incidents i JOIN windows w"
            " ON w.window_start_ms BETWEEN i.incident_start_ms"
            " AND i.last_window_start_ms"
            + where
            + " ORDER BY i.incident_start_ms, w.window_start_ms"
        )
        with self.storage.read_txn() as conn:
            rows = conn.execute(query, params).fetchall()
        return self._assemble_incidents(rows)

    def get_incident(self, when_ms: int) -> dict | None:
        """The incident covering the five-minute window containing ``when_ms``.

        Returns the incident with its constituent windows, or ``None`` when
        that window is not part of any incident (NORMAL, or
        still unsealed).
        """
        s = self.settings
        w = (when_ms // s.window_ms) * s.window_ms
        query = (
            "SELECT i.*, wn.window_start_ms, wn.window_end_ms, wn.level,"
            " wn.total_dose AS w_total_dose, wn.peak_dose AS w_peak_dose,"
            " wn.event_count"
            " FROM incidents i JOIN windows wn"
            " ON wn.window_start_ms BETWEEN i.incident_start_ms"
            " AND i.last_window_start_ms"
            " WHERE i.incident_start_ms <= ? AND i.last_window_start_ms >= ?"
            " ORDER BY wn.window_start_ms"
        )
        with self.storage.read_txn() as conn:
            rows = conn.execute(query, (w, w)).fetchall()
        incidents = self._assemble_incidents(rows)
        return incidents[0] if incidents else None

    @staticmethod
    def _assemble_incidents(rows) -> list[dict]:
        incidents: dict[int, dict] = {}
        order: list[int] = []
        for r in rows:
            start = r["incident_start_ms"]
            if start not in incidents:
                order.append(start)
                incidents[start] = {
                    "incident_start": iso(start),
                    "incident_end": None,  # end of the last constituent window
                    "status": r["status"],
                    "ongoing": r["status"] == "ongoing",
                    "window_count": r["window_count"],
                    "total_dose": r["total_dose"],
                    "peak_dose": r["peak_dose"],
                    "highest_level": r["highest_level"],
                    "closed_at": iso(r["closed_at_ms"]),
                    "windows": [],
                }
            incidents[start]["windows"].append(
                {
                    "window_start": iso(r["window_start_ms"]),
                    "window_end": iso(r["window_end_ms"]),
                    "level": r["level"],
                    "total_dose": r["w_total_dose"],
                    "peak_dose": r["w_peak_dose"],
                    "event_count": r["event_count"],
                }
            )
        out = [incidents[k] for k in order]
        for inc in out:
            inc["incident_end"] = inc["windows"][-1]["window_end"]
        return out

    def state(self) -> dict:
        with self.storage.read_txn() as conn:
            watermark_ms = self._watermark(conn)
            progress = self._progress(conn)
            sealed = conn.execute("SELECT COUNT(*) AS c FROM windows").fetchone()["c"]
            readings = conn.execute("SELECT COUNT(*) AS c FROM readings").fetchone()["c"]
            ongoing = conn.execute(
                "SELECT COUNT(*) AS c FROM incidents WHERE status = 'ongoing'"
            ).fetchone()["c"]
            ended = conn.execute(
                "SELECT COUNT(*) AS c FROM incidents WHERE status = 'ended'"
            ).fetchone()["c"]
        s = self.settings
        return {
            "watermark": iso(watermark_ms),
            "probes": progress,
            "sealed_window_count": sealed,
            "reading_count": readings,
            "ongoing_incident_count": ongoing,
            "ended_incident_count": ended,
            "config": {
                "window_seconds": s.window_seconds,
                "allowed_lateness_seconds": s.allowed_lateness_seconds,
                "elevated_total": s.elevated_total,
                "elevated_peak": s.elevated_peak,
                "critical_total": s.critical_total,
                "critical_peak": s.critical_peak,
                "probes": list(s.probes),
            },
        }

    def health_check(self) -> None:
        with self.storage.read_txn() as conn:
            conn.execute("SELECT 1").fetchone()
