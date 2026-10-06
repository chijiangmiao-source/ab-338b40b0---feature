"""SQLite persistence layer.

A single connection guarded by a re-entrant lock serializes all access;
every ingest + watermark advance + window sealing happens inside one
``BEGIN IMMEDIATE`` transaction, so a sealed record is either fully
committed or not at all — even under concurrent submissions or a crash.
State lives entirely in the database file, so a process restart resumes
from exactly what was committed.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    event_id        TEXT PRIMARY KEY,
    probe           TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    observed_at_ms  INTEGER NOT NULL,
    dose            REAL NOT NULL,
    content_hash    TEXT NOT NULL,
    ack_json        TEXT NOT NULL,
    received_at_ms  INTEGER NOT NULL,
    UNIQUE (probe, seq)
);
CREATE INDEX IF NOT EXISTS idx_readings_window ON readings (observed_at_ms);

CREATE TABLE IF NOT EXISTS probe_state (
    probe            TEXT PRIMARY KEY,
    contiguous_seq   INTEGER NOT NULL DEFAULT 0,
    frontier_time_ms INTEGER,
    max_seq          INTEGER NOT NULL DEFAULT 0,
    updated_at_ms    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS windows (
    window_start_ms      INTEGER PRIMARY KEY,
    window_end_ms        INTEGER NOT NULL,
    total_dose           REAL NOT NULL,
    peak_dose            REAL NOT NULL,
    event_count          INTEGER NOT NULL,
    level                TEXT NOT NULL,
    first_violation_json TEXT,
    watermark_ms         INTEGER NOT NULL,
    progress_json        TEXT NOT NULL,
    sealed_at_ms         INTEGER NOT NULL
);

-- One irradiation event per run of adjacent non-NORMAL sealed windows.
-- Maintained incrementally inside the sealing transaction; ended events
-- are never rewritten.
CREATE TABLE IF NOT EXISTS irradiation_events (
    event_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    start_window_ms    INTEGER NOT NULL,
    end_window_ms      INTEGER NOT NULL,
    window_count       INTEGER NOT NULL,
    total_dose         REAL NOT NULL,
    peak_dose          REAL NOT NULL,
    max_level          TEXT NOT NULL,
    status             TEXT NOT NULL,              -- 'ongoing' | 'ended'
    ended_by_window_ms INTEGER,                    -- NORMAL window that closed it
    created_at_ms      INTEGER NOT NULL,
    updated_at_ms      INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_irradiation_events_start
    ON irradiation_events (start_window_ms);
-- At most one open (still-accruing) event may exist at any time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_irradiation_events_open
    ON irradiation_events (status) WHERE status = 'ongoing';
"""


class Storage:
    def __init__(self, path: str):
        self._path = path
        if path != ":memory:":
            directory = os.path.dirname(os.path.abspath(path))
            os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)

    @contextmanager
    def write_txn(self) -> Iterator[sqlite3.Connection]:
        """Exclusive write transaction; commit on success, rollback on error."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            try:
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    @contextmanager
    def read_txn(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            yield self._conn

    def close(self) -> None:
        with self._lock:
            self._conn.close()
