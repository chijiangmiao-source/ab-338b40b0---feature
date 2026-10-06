"""End-to-end tests: retransmission, out-of-order sealing, watermark
honesty, immutability, recovery across restarts, and concurrency."""
from __future__ import annotations

import json
import random
import threading
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


# ------------------------------------------------------------ retransmission

def test_identical_retransmission_replays_original_ack(client):
    body = reading("e1", "alpha", 1, 10, 5.0)
    r1 = client.post("/readings", json=body)
    assert r1.status_code == 200
    assert r1.json()["status"] == "accepted"

    r2 = client.post("/readings", json=body)
    assert r2.status_code == 200
    assert r2.headers.get("x-idempotent-replay") == "true"
    assert r2.json() == r1.json()  # replayed verbatim

    assert client.get("/state").json()["reading_count"] == 1


def test_conflicting_retransmission_rejected(client):
    assert client.post("/readings", json=reading("e1", "alpha", 1, 10, 5.0)).status_code == 200
    r = client.post("/readings", json=reading("e1", "alpha", 1, 10, 6.0))
    assert r.status_code == 409
    assert r.json()["error"] == "event_conflict"
    assert r.json()["detail"]["stored"]["dose"] == 5.0
    assert r.json()["detail"]["received"]["dose"] == 6.0


def test_same_probe_seq_with_different_event_id_rejected(client):
    assert client.post("/readings", json=reading("e1", "alpha", 1, 10, 5.0)).status_code == 200
    r = client.post("/readings", json=reading("e2", "alpha", 1, 20, 5.0))
    assert r.status_code == 409
    assert r.json()["error"] == "seq_conflict"


def test_unknown_probe_rejected(client):
    r = client.post("/readings", json=reading("e1", "gamma", 1, 10, 5.0))
    assert r.status_code == 400
    assert r.json()["error"] == "unknown_probe"


def test_naive_observed_at_rejected(client):
    r = client.post(
        "/readings",
        json={
            "event_id": "e1",
            "probe": "alpha",
            "seq": 1,
            "observed_at": "2026-01-01T00:00:10",  # no timezone
            "dose": 1.0,
        },
    )
    assert r.status_code == 422


# ------------------------------------------------------- out-of-order sealing

def _seal_first_window(client):
    """Events arrive out of observation order; the fourth submission
    pushes both frontiers past the end of window [00:00, 00:05)."""
    assert client.post("/readings", json=reading("a1", "alpha", 1, 290, 10.0)).status_code == 200
    assert client.post("/readings", json=reading("a2", "alpha", 2, 320, 5.0)).status_code == 200
    assert client.post("/readings", json=reading("b1", "beta", 1, 100, 20.0)).status_code == 200
    return client.post("/readings", json=reading("b2", "beta", 2, 305, 7.0))


def test_out_of_order_data_merges_before_sealing(client):
    r = _seal_first_window(client)
    assert r.status_code == 200
    ack = r.json()
    assert ack["watermark"] == ts(305)
    assert len(ack["sealed_windows"]) == 1
    summary = ack["sealed_windows"][0]
    assert summary["window_start"] == ts(0)
    assert summary["window_end"] == ts(300)
    assert summary["total_dose"] == 30.0  # 10 + 20, merged despite arrival order
    assert summary["peak_dose"] == 20.0
    assert summary["event_count"] == 2
    assert summary["level"] == "NORMAL"

    w = client.get(f"/windows/{ts(0)}").json()
    assert w["status"] == "sealed" and w["final"] is True
    assert w["total_dose"] == 30.0
    assert w["peak_dose"] == 20.0
    assert w["event_count"] == 2
    assert w["level"] == "NORMAL"
    assert w["first_violation"] is None
    # both probes' progress is recorded for audit at seal time
    assert w["progress_at_seal"]["alpha"]["contiguous_seq"] == 2
    assert w["progress_at_seal"]["beta"]["contiguous_seq"] == 2
    assert w["progress_at_seal"]["alpha"]["frontier_time"] == ts(320)
    assert w["progress_at_seal"]["beta"]["frontier_time"] == ts(305)
    assert w["watermark_at_seal"] == ts(305)
    assert {e["event_id"] for e in w["events"]} == {"a1", "b1"}


def test_sealed_window_rejects_late_data_without_changing_summary(client):
    _seal_first_window(client)
    r = client.post("/readings", json=reading("a3", "alpha", 3, 200, 50.0))
    assert r.status_code == 409
    assert r.json()["error"] == "window_sealed"

    w = client.get(f"/windows/{ts(0)}").json()
    assert w["total_dose"] == 30.0
    assert w["event_count"] == 2
    assert client.get("/state").json()["reading_count"] == 4  # nothing persisted


def test_open_window_is_provisional(client):
    client.post("/readings", json=reading("a1", "alpha", 1, 10, 12.5))
    w = client.get(f"/windows/{ts(0)}").json()
    assert w["status"] == "open"
    assert w["final"] is False
    assert w["total_dose"] == 12.5
    assert w["event_count"] == 1
    assert w["sealed_at"] is None
    assert w["current_watermark"] is None  # beta has not reported yet


# -------------------------------------------------------- watermark honesty

def test_missing_probe_blocks_watermark(client):
    for i, t in enumerate((100, 400, 700), start=1):
        assert client.post("/readings", json=reading(f"a{i}", "alpha", i, t, 1.0)).status_code == 200
    st = client.get("/state").json()
    assert st["watermark"] is None
    assert st["sealed_window_count"] == 0


def test_sequence_gap_blocks_watermark_until_filled(client):
    client.post("/readings", json=reading("a1", "alpha", 1, 1000, 1.0))
    client.post("/readings", json=reading("a3", "alpha", 3, 2000, 1.0))  # gap at seq 2
    client.post("/readings", json=reading("b1", "beta", 1, 3000, 1.0))
    client.post("/readings", json=reading("b2", "beta", 2, 3100, 1.0))

    st = client.get("/state").json()
    assert st["probes"]["alpha"]["contiguous_seq"] == 1
    assert st["probes"]["alpha"]["max_seq"] == 3
    assert st["watermark"] == ts(1000)  # min(1000, 3100): the gap caps alpha
    # first observed event is at t=1000 (window [900,1200)); its end is
    # past the watermark, so nothing has sealed yet
    assert st["sealed_window_count"] == 0
    assert client.get(f"/windows/{ts(900)}").json()["status"] == "open"

    # filling the gap extends the contiguous frontier and the watermark
    r = client.post("/readings", json=reading("a2", "alpha", 2, 1500, 1.0))
    assert r.status_code == 200
    st = client.get("/state").json()
    assert st["probes"]["alpha"]["contiguous_seq"] == 3
    assert st["watermark"] == ts(2000)
    # windows [900,1200), [1200,1500), [1500,1800) sealed in one transaction
    assert client.get(f"/windows/{ts(900)}").json()["status"] == "sealed"
    assert client.get(f"/windows/{ts(900)}").json()["total_dose"] == 1.0
    empty = client.get(f"/windows/{ts(1200)}").json()
    assert empty["status"] == "sealed"
    assert empty["event_count"] == 0
    assert empty["level"] == "NORMAL"
    assert client.get(f"/windows/{ts(1500)}").json()["event_count"] == 1  # a2
    assert client.get(f"/windows/{ts(1800)}").json()["status"] == "open"

    # a late event for the just-sealed window is rejected
    r = client.post("/readings", json=reading("a4", "alpha", 4, 950, 9.0))
    assert r.status_code == 409
    assert r.json()["error"] == "window_sealed"


def test_watermark_gate_rejects_pre_anchor_late_data(client):
    client.post("/readings", json=reading("a1", "alpha", 1, 1000, 1.0))
    client.post("/readings", json=reading("b1", "beta", 1, 1200, 1.0))
    assert client.get("/state").json()["watermark"] == ts(1000)
    # window [0,300) predates the first observed event and was never
    # physically sealed, but its end is behind the watermark: rejected
    r = client.post("/readings", json=reading("a2", "alpha", 2, 100, 5.0))
    assert r.status_code == 409
    assert r.json()["error"] == "window_sealed"
    assert r.json()["detail"]["reason"] == "past_watermark"
    w = client.get(f"/windows/{ts(0)}").json()
    assert w["status"] == "open"
    assert w["accepting"] is False
    assert w["event_count"] == 0


def test_initial_sequence_gap_fabricates_no_watermark(client):
    client.post("/readings", json=reading("a5", "alpha", 5, 5000, 1.0))  # seq 1..4 never seen
    client.post("/readings", json=reading("b1", "beta", 1, 5000, 1.0))
    st = client.get("/state").json()
    assert st["probes"]["alpha"]["contiguous_seq"] == 0
    assert st["watermark"] is None
    assert st["sealed_window_count"] == 0


def test_regressing_sequence_never_moves_frontier_backwards(client):
    client.post("/readings", json=reading("a1", "alpha", 1, 100, 1.0))
    client.post("/readings", json=reading("a2", "alpha", 2, 900, 1.0))
    client.post("/readings", json=reading("b1", "beta", 1, 950, 1.0))
    assert client.get("/state").json()["watermark"] == ts(900)
    # a late, older-observation event for an open window must not drag
    # the watermark down
    client.post("/readings", json=reading("a3", "alpha", 3, 500, 1.0))
    assert client.get("/state").json()["watermark"] == ts(900)


def test_allowed_lateness_shifts_watermark(db_path):
    with TestClient(create_app(make_settings(db_path, allowed_lateness_seconds=60))) as client:
        client.post("/readings", json=reading("a1", "alpha", 1, 400, 1.0))
        client.post("/readings", json=reading("b1", "beta", 1, 500, 1.0))
        st = client.get("/state").json()
        assert st["watermark"] == ts(340)  # min(400, 500) - 60
        # window [300,600) ends at 600 > 340: still open and accepting
        w1 = client.get(f"/windows/{ts(300)}").json()
        assert w1["status"] == "open"
        assert w1["accepting"] is True

        client.post("/readings", json=reading("a2", "alpha", 2, 700, 1.0))
        r = client.post("/readings", json=reading("b2", "beta", 2, 800, 1.0))
        assert r.json()["watermark"] == ts(640)  # min(700, 800) - 60
        assert [w["window_start"] for w in r.json()["sealed_windows"]] == [ts(300)]
        w1 = client.get(f"/windows/{ts(300)}").json()
        assert w1["status"] == "sealed"
        assert w1["total_dose"] == 2.0


# --------------------------------------------------------- levels & audit

def test_first_violation_by_peak_and_progress_snapshot(client):
    client.post("/readings", json=reading("a1", "alpha", 1, 10, 45.0))  # >= elevated_peak
    client.post("/readings", json=reading("b1", "beta", 1, 20, 5.0))
    client.post("/readings", json=reading("a2", "alpha", 2, 400, 1.0))
    client.post("/readings", json=reading("b2", "beta", 2, 400, 1.0))

    w = client.get(f"/windows/{ts(0)}").json()
    assert w["status"] == "sealed"
    assert w["level"] == "ELEVATED"
    assert w["first_violation"]["event_id"] == "a1"
    assert w["first_violation"]["dose"] == 45.0
    assert w["progress_at_seal"]["alpha"]["contiguous_seq"] == 2
    assert w["progress_at_seal"]["beta"]["contiguous_seq"] == 2


def test_first_violation_by_running_total(client):
    # four 30 µSv events: each below elevated_peak, total 120 >= elevated_total
    client.post("/readings", json=reading("a1", "alpha", 1, 10, 30.0))
    client.post("/readings", json=reading("b1", "beta", 1, 15, 30.0))
    client.post("/readings", json=reading("a2", "alpha", 2, 20, 30.0))
    client.post("/readings", json=reading("b2", "beta", 2, 25, 30.0))
    client.post("/readings", json=reading("a3", "alpha", 3, 400, 1.0))
    client.post("/readings", json=reading("b3", "beta", 3, 400, 1.0))

    w = client.get(f"/windows/{ts(0)}").json()
    assert w["level"] == "ELEVATED"
    assert w["total_dose"] == 120.0
    # running total crosses 100 at the 4th event in observation order
    assert w["first_violation"]["event_id"] == "b2"


def test_critical_level_by_total(client):
    for i, t in enumerate((10, 15, 20, 25, 30, 35, 40, 45, 50), start=1):
        probe = "alpha" if i % 2 == 1 else "beta"
        seq = (i + 1) // 2
        assert client.post(
            "/readings", json=reading(f"{probe}{seq}", probe, seq, t, 30.0)
        ).status_code == 200
    client.post("/readings", json=reading("a6", "alpha", 6, 400, 1.0))
    client.post("/readings", json=reading("b5", "beta", 5, 400, 1.0))

    w = client.get(f"/windows/{ts(0)}").json()
    assert w["level"] == "CRITICAL"  # total 270 >= 250, peak 30 < 80
    assert w["total_dose"] == 270.0
    assert w["first_violation"]["event_id"] == "beta2"  # running total hits 120 >= 100


# ----------------------------------------------------------------- recovery

def test_restart_recovers_sealed_windows_and_idempotency(db_path):
    with TestClient(create_app(make_settings(db_path))) as c1:
        c1.post("/readings", json=reading("a1", "alpha", 1, 290, 10.0))
        c1.post("/readings", json=reading("b1", "beta", 1, 100, 20.0))
        c1.post("/readings", json=reading("a2", "alpha", 2, 320, 5.0))
        ack = c1.post("/readings", json=reading("b2", "beta", 2, 305, 7.0)).json()
        sealed_before = c1.get(f"/windows/{ts(0)}").json()
        state_before = c1.get("/state").json()

    # brand-new engine + app over the same database file: a "restart"
    with TestClient(create_app(make_settings(db_path))) as c2:
        assert c2.get("/health").json() == {"status": "ok"}
        assert c2.get(f"/windows/{ts(0)}").json() == sealed_before
        st = c2.get("/state").json()
        assert st["watermark"] == state_before["watermark"]
        assert st["sealed_window_count"] == state_before["sealed_window_count"]
        assert st["reading_count"] == state_before["reading_count"]

        # retransmission still replays the pre-restart ack
        r = c2.post("/readings", json=reading("b2", "beta", 2, 305, 7.0))
        assert r.status_code == 200
        assert r.headers.get("x-idempotent-replay") == "true"
        assert r.json() == ack

        # the sealed window is still immutable
        r = c2.post("/readings", json=reading("a3", "alpha", 3, 200, 50.0))
        assert r.status_code == 409
        assert r.json()["error"] == "window_sealed"
        assert c2.get(f"/windows/{ts(0)}").json() == sealed_before

        # and new data keeps flowing after the restart
        r = c2.post("/readings", json=reading("a3", "alpha", 3, 500, 4.0))
        assert r.status_code == 200


# --------------------------------------------------------------- concurrency

def _engine(db_path) -> Engine:
    settings = make_settings(db_path)
    return Engine(Storage(settings.database_path), settings)


def _reading_in(event_id, probe, seq, t, dose):
    return ReadingIn(
        event_id=event_id,
        probe=probe,
        seq=seq,
        observed_at=BASE + timedelta(seconds=t),
        dose=dose,
    )


def test_concurrent_submissions_leave_unique_sealed_records(db_path):
    engine = _engine(db_path)
    jobs = []
    for i in range(1, 26):
        jobs.append((f"a{i}", "alpha", i, 100 + i))
        jobs.append((f"b{i}", "beta", i, 200 + i))
    jobs.append(("a26", "alpha", 26, 1000))
    jobs.append(("b26", "beta", 26, 1000))
    random.Random(7).shuffle(jobs)

    def submit(job):
        return engine.submit(_reading_in(job[0], job[1], job[2], job[3], 2.0))

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, jobs))

    assert all(replayed is False for _, replayed in results)
    st = engine.state()
    assert st["reading_count"] == 52
    assert st["probes"]["alpha"]["contiguous_seq"] == 26
    assert st["probes"]["beta"]["contiguous_seq"] == 26
    assert st["watermark"] == ts(1000)

    w0 = engine.get_window(round(BASE.timestamp() * 1000))
    assert w0["status"] == "sealed"
    assert w0["event_count"] == 50
    assert w0["total_dose"] == 100.0
    # exactly one sealed record per window, no duplicates under concurrency
    starts = [w["window_start"] for w in engine.list_windows(None, None)]
    assert len(starts) == len(set(starts)) == 3


def test_concurrent_duplicate_storm_yields_single_accept(db_path):
    engine = _engine(db_path)
    body = _reading_in("dup", "alpha", 1, 10, 1.0)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: engine.submit(body), range(16)))

    acks = {json.dumps(ack, sort_keys=True) for ack, _ in results}
    assert len(acks) == 1  # every caller observes the identical ack
    assert sum(1 for _, replayed in results if not replayed) == 1
    assert engine.state()["reading_count"] == 1


def test_concurrent_conflicting_event_has_single_winner(db_path):
    engine = _engine(db_path)
    barrier = threading.Barrier(8)

    def submit(i):
        barrier.wait()
        try:
            engine.submit(_reading_in("race", "alpha", 1, 10, float(i)))
            return "accepted"
        except Exception as exc:
            return getattr(exc, "code", "error")

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(submit, range(8)))

    assert outcomes.count("accepted") == 1
    assert outcomes.count("event_conflict") == 7
    assert engine.state()["reading_count"] == 1


# -------------------------------------------------------------------- misc

def test_window_lookup_aligns_to_boundary(client):
    client.post("/readings", json=reading("a1", "alpha", 1, 10, 3.0))
    w = client.get(f"/windows/{ts(299)}").json()  # inside window [0, 300)
    assert w["window_start"] == ts(0)
    assert w["event_count"] == 1


def test_list_windows(client):
    _seal_first_window(client)
    listing = client.get("/windows").json()["windows"]
    assert len(listing) == 1
    assert listing[0]["window_start"] == ts(0)
    assert listing[0]["status"] == "sealed"
    bounded = client.get(f"/windows?since={ts(300)}").json()["windows"]
    assert bounded == []


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}
