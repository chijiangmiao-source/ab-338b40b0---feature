"""Irradiation-event rollup: adjacent non-NORMAL sealed windows merge into
one continuous event, maintained transactionally at seal time, immutable
once ended, and readable stably sorted by event start."""
from __future__ import annotations

import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.engine import Engine
from app.main import create_app
from app.models import ReadingIn
from app.storage import Storage

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)  # aligned to a 5-minute boundary


def ts(sec: float) -> str:
    return (BASE + timedelta(seconds=sec)).isoformat().replace("+00:00", "Z")


def make_settings(db_path, **overrides) -> Settings:
    params = dict(
        database_path=str(db_path),
        window_seconds=300,
        allowed_lateness_seconds=0,
        elevated_total=100.0,
        elevated_peak=40.0,
        critical_total=250.0,
        critical_peak=80.0,
    )
    params.update(overrides)
    return Settings(**params)


def reading(event_id, probe, seq, t, dose):
    return {
        "event_id": event_id,
        "probe": probe,
        "seq": seq,
        "observed_at": ts(t),
        "dose": dose,
    }


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "radiation.db"


@pytest.fixture
def client(db_path):
    with TestClient(create_app(make_settings(db_path))) as c:
        yield c


def _post(client, *readings):
    for body in readings:
        assert client.post("/readings", json=body).status_code == 200


# ------------------------------------------------------------- merging


def test_adjacent_non_normal_windows_merge_into_single_event(client):
    # window [0,300): ELEVATED via peak; window [300,600): CRITICAL via peak;
    # window [600,900): NORMAL — closes the event.
    _post(
        client,
        reading("a1", "alpha", 1, 10, 45.0),
        reading("b1", "beta", 1, 20, 5.0),
        reading("a2", "alpha", 2, 310, 85.0),
        reading("b2", "beta", 2, 320, 1.0),
        reading("a3", "alpha", 3, 610, 1.0),
        reading("b3", "beta", 3, 620, 1.0),
        reading("a4", "alpha", 4, 950, 1.0),
        reading("b4", "beta", 4, 950, 1.0),
    )

    events = client.get("/events").json()["events"]
    assert len(events) == 1
    ev = events[0]
    assert ev["start_window"] == ts(0)
    assert ev["end_window"] == ts(300)
    assert ev["window_count"] == 2
    assert ev["total_dose"] == 136.0  # (45 + 5) + (85 + 1)
    assert ev["peak_dose"] == 85.0
    assert ev["max_level"] == "CRITICAL"
    assert ev["status"] == "ended"
    assert ev["ended_by_window"] == ts(600)
    # constituent windows, in window order
    assert [w["window_start"] for w in ev["windows"]] == [ts(0), ts(300)]
    assert [w["level"] for w in ev["windows"]] == ["ELEVATED", "CRITICAL"]
    assert ev["windows"][0]["total_dose"] == 50.0
    assert ev["windows"][1]["total_dose"] == 86.0


def test_event_stays_open_until_normal_window_seals(client):
    _post(
        client,
        reading("a1", "alpha", 1, 10, 45.0),
        reading("b1", "beta", 1, 20, 5.0),
        reading("a2", "alpha", 2, 305, 1.0),
        reading("b2", "beta", 2, 310, 1.0),
    )
    # only window [0,300) sealed so far: the event is still accruing
    (ev,) = client.get("/events").json()["events"]
    assert ev["status"] == "ongoing"
    assert ev["ended_by_window"] is None
    assert ev["start_window"] == ts(0)
    assert ev["end_window"] == ts(0)
    assert ev["window_count"] == 1
    assert ev["total_dose"] == 50.0
    assert ev["max_level"] == "ELEVATED"

    # a second non-NORMAL window extends the same event, then a NORMAL
    # window ends it
    _post(
        client,
        reading("a3", "alpha", 3, 320, 50.0),
        reading("b3", "beta", 3, 330, 1.0),
        reading("a4", "alpha", 4, 605, 1.0),
        reading("b4", "beta", 4, 615, 1.0),
        reading("a5", "alpha", 5, 905, 1.0),
        reading("b5", "beta", 5, 910, 1.0),
    )
    events = client.get("/events").json()["events"]
    assert len(events) == 1  # still one event, not split per window
    ev = events[0]
    assert ev["status"] == "ended"
    assert ev["window_count"] == 2
    assert ev["total_dose"] == 103.0  # 50 + (1 + 1 + 50 + 1)
    assert ev["peak_dose"] == 50.0
    assert ev["max_level"] == "ELEVATED"
    assert ev["end_window"] == ts(300)
    assert ev["ended_by_window"] == ts(600)


def test_empty_window_crossed_by_watermark_truncates_event(client):
    _post(
        client,
        reading("a1", "alpha", 1, 10, 45.0),  # window [0,300) ELEVATED
        reading("b1", "beta", 1, 20, 5.0),
        # jump far ahead: the watermark steps over two empty windows
        reading("a2", "alpha", 2, 1000, 1.0),
        reading("b2", "beta", 2, 1000, 1.0),
    )
    assert client.get(f"/windows/{ts(300)}").json()["event_count"] == 0
    assert client.get(f"/windows/{ts(300)}").json()["level"] == "NORMAL"

    (ev,) = client.get("/events").json()["events"]
    assert ev["status"] == "ended"
    assert ev["window_count"] == 1
    # the empty window [300,600) participates as NORMAL and closes the event
    assert ev["ended_by_window"] == ts(300)
    assert [w["window_start"] for w in ev["windows"]] == [ts(0)]


def test_separate_episodes_become_separate_events_sorted_by_start(client):
    _post(
        client,
        reading("a1", "alpha", 1, 10, 45.0),   # window 0: ELEVATED
        reading("b1", "beta", 1, 20, 1.0),
        reading("a2", "alpha", 2, 310, 1.0),   # window 1: NORMAL
        reading("b2", "beta", 2, 320, 1.0),
        reading("a3", "alpha", 3, 610, 90.0),  # window 2: CRITICAL
        reading("b3", "beta", 3, 620, 1.0),
        reading("a4", "alpha", 4, 950, 1.0),
        reading("b4", "beta", 4, 960, 1.0),
    )
    events = client.get("/events").json()["events"]
    assert [e["start_window"] for e in events] == [ts(0), ts(600)]
    first, second = events
    assert first["status"] == "ended"
    assert first["ended_by_window"] == ts(300)
    assert first["max_level"] == "ELEVATED"
    assert second["status"] == "ongoing"  # no NORMAL window sealed after it yet
    assert second["max_level"] == "CRITICAL"
    assert second["window_count"] == 1

    # bounded by event start
    assert [e["start_window"] for e in client.get(f"/events?since={ts(600)}").json()["events"]] == [ts(600)]
    assert [e["start_window"] for e in client.get(f"/events?until={ts(0)}").json()["events"]] == [ts(0)]


def test_events_empty_until_first_non_normal_window_seals(client):
    assert client.get("/events").json() == {"events": []}
    _post(
        client,
        reading("a1", "alpha", 1, 10, 1.0),  # NORMAL window only
        reading("b1", "beta", 1, 20, 1.0),
        reading("a2", "alpha", 2, 305, 1.0),
        reading("b2", "beta", 2, 310, 1.0),
    )
    assert client.get(f"/windows/{ts(0)}").json()["status"] == "sealed"
    assert client.get("/events").json() == {"events": []}


# ---------------------------------------------------------- immutability


def test_ended_event_untouched_by_late_data_replays_and_reads(client):
    _post(
        client,
        reading("a1", "alpha", 1, 10, 45.0),
        reading("b1", "beta", 1, 20, 5.0),
        reading("a2", "alpha", 2, 1000, 1.0),
        reading("b2", "beta", 2, 1000, 1.0),
    )
    before = client.get("/events").json()
    assert before["events"][0]["status"] == "ended"

    # late reading for a sealed window: rejected, changes nothing
    r = client.post("/readings", json=reading("a3", "alpha", 3, 100, 500.0))
    assert r.status_code == 409
    assert r.json()["error"] == "window_sealed"
    # identical retransmission replays the ack without re-driving the rollup
    r = client.post("/readings", json=reading("a1", "alpha", 1, 10, 45.0))
    assert r.status_code == 200
    assert r.headers["x-idempotent-replay"] == "true"
    # window reads are pure reads
    client.get(f"/windows/{ts(0)}")
    client.get("/windows")

    assert client.get("/events").json() == before


# --------------------------------------------------------------- recovery


def test_restart_preserves_events_and_continues_open_event(db_path):
    with TestClient(create_app(make_settings(db_path))) as c1:
        _post(
            c1,
            reading("a1", "alpha", 1, 10, 45.0),
            reading("b1", "beta", 1, 20, 5.0),
            reading("a2", "alpha", 2, 305, 1.0),
            reading("b2", "beta", 2, 310, 1.0),
        )
        before = c1.get("/events").json()
        assert before["events"][0]["status"] == "ongoing"

    # brand-new engine + app over the same database file: a "restart"
    with TestClient(create_app(make_settings(db_path))) as c2:
        assert c2.get("/events").json() == before  # recovered verbatim

        # idempotent replay of a pre-restart submission does not re-drive
        # the rollup
        r = c2.post("/readings", json=reading("a1", "alpha", 1, 10, 45.0))
        assert r.headers["x-idempotent-replay"] == "true"
        assert c2.get("/events").json() == before

        # the still-open event keeps accruing across the restart, then ends
        _post(
            c2,
            reading("a3", "alpha", 3, 320, 85.0),  # window 1: CRITICAL
            reading("b3", "beta", 3, 330, 1.0),
            reading("a4", "alpha", 4, 605, 1.0),   # window 2: NORMAL
            reading("b4", "beta", 4, 615, 1.0),
            reading("a5", "alpha", 5, 905, 1.0),
            reading("b5", "beta", 5, 910, 1.0),
        )
        events = c2.get("/events").json()["events"]
        assert len(events) == 1  # exactly one accumulated record
        ev = events[0]
        assert ev["event_id"] == before["events"][0]["event_id"]
        assert ev["window_count"] == 2
        assert ev["total_dose"] == 138.0  # 50 + (1 + 1 + 85 + 1)
        assert ev["peak_dose"] == 85.0
        assert ev["max_level"] == "CRITICAL"
        assert ev["status"] == "ended"
        assert ev["ended_by_window"] == ts(600)


# ------------------------------------------------------------ concurrency


def test_concurrent_sealing_leaves_single_correct_event(db_path):
    settings = make_settings(db_path)
    engine = Engine(Storage(settings.database_path), settings)

    jobs = []
    for i in range(1, 26):
        jobs.append((f"a{i}", "alpha", i, 10 + i))    # window 0
        jobs.append((f"b{i}", "beta", i, 310 + i))    # window 1
    jobs.append(("a26", "alpha", 26, 1000))
    jobs.append(("b26", "beta", 26, 1000))
    random.Random(7).shuffle(jobs)

    def submit(job):
        event_id, probe, seq, t = job
        return engine.submit(
            ReadingIn(
                event_id=event_id,
                probe=probe,
                seq=seq,
                observed_at=BASE + timedelta(seconds=t),
                dose=5.0,
            )
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, jobs))
    assert all(replayed is False for _, replayed in results)

    # windows 0 and 1 both ELEVATED (total 125 each), then an empty NORMAL
    # window ends the event — exactly once, under concurrency
    events = engine.list_events(None, None)
    assert len(events) == 1
    ev = events[0]
    assert ev["start_window"] == ts(0)
    assert ev["end_window"] == ts(300)
    assert ev["window_count"] == 2
    assert ev["total_dose"] == 250.0
    assert ev["peak_dose"] == 5.0
    assert ev["max_level"] == "ELEVATED"
    assert ev["status"] == "ended"
    assert ev["ended_by_window"] == ts(600)
    assert [w["window_start"] for w in ev["windows"]] == [ts(0), ts(300)]

    # a second engine over the same file (a "restart") sees the same single
    # accumulated record
    engine2 = Engine(Storage(settings.database_path), settings)
    assert engine2.list_events(None, None) == events
