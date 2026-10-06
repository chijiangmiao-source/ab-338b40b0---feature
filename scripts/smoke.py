"""HTTP smoke test executed inside the one-shot ``verify`` container.

Covers the three acceptance themes against the live service:

1. Retransmission  — identical resubmission replays the original ack,
   conflicting content is rejected with 409.
2. Out-of-order sealing — out-of-order readings merge into the correct
   five-minute window, which seals exactly once the watermark passes its
   end; late data for the sealed window is rejected without effect.
3. Recovery — the app container is restarted through the Docker socket;
   sealed windows, idempotency records, and rejections must survive.

Exits 0 on success, 1 on any failure.
"""
from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = os.environ.get("APP_BASE_URL", "http://app:8000").rstrip("/")
DOCKER_SOCKET = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
APP_SERVICE = os.environ.get("APP_SERVICE_NAME", "app")

CHECKS = {"passed": 0, "failed": 0}


def check(condition: bool, label: str, context: str = "") -> None:
    if condition:
        CHECKS["passed"] += 1
        print(f"[PASS] {label}")
    else:
        CHECKS["failed"] += 1
        print(f"[FAIL] {label} {context}")


def req(method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as resp:
            payload = resp.read()
            headers = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, headers, json.loads(payload) if payload else {}
    except urllib.error.HTTPError as exc:
        payload = exc.read()
        try:
            parsed = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            parsed = {"raw": payload.decode(errors="replace")}
        headers = {k.lower(): v for k, v in exc.headers.items()}
        return exc.code, headers, parsed


def wait_healthy(timeout_s: float = 90.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            status, _, _ = req("GET", "/health")
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


# --------------------------------------------------------------- docker api

class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str):
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(30)
        self.sock.connect(self.socket_path)


def docker_request(method: str, path: str):
    conn = UnixHTTPConnection(DOCKER_SOCKET)
    try:
        conn.request(method, path)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def restart_app_container() -> None:
    if not os.path.exists(DOCKER_SOCKET):
        raise RuntimeError(f"docker socket not available at {DOCKER_SOCKET}")
    filters = urllib.parse.quote(
        json.dumps({"label": [f"com.docker.compose.service={APP_SERVICE}"]})
    )
    status, payload = docker_request("GET", f"/containers/json?filters={filters}")
    containers = json.loads(payload or b"[]")
    if status != 200 or not containers:
        raise RuntimeError(f"app container not found (status={status}, matches={containers})")
    container_id = containers[0]["Id"]
    status, payload = docker_request("POST", f"/containers/{container_id}/restart?t=5")
    if status not in (200, 204):
        raise RuntimeError(f"restart failed: {status} {payload!r}")


# -------------------------------------------------------------------- smoke

def iso(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def main() -> int:
    print(f"[smoke] target={BASE_URL}")
    if not wait_healthy():
        print("[FAIL] service did not become healthy")
        return 1
    print("[PASS] service becomes healthy")
    CHECKS["passed"] += 1

    status, _, state = req("GET", "/state")
    if status != 200:
        print(f"[FAIL] GET /state responded {status}")
        return 1
    print("[PASS] GET /state responds")
    CHECKS["passed"] += 1
    cfg = state.get("config", {})
    window_ms = int(cfg.get("window_seconds", 300)) * 1000
    lateness_ms = int(cfg.get("allowed_lateness_seconds", 60)) * 1000
    if lateness_ms >= 70_000:
        print(f"[FAIL] allowed lateness {lateness_ms}ms too large for smoke scenario")
        return 1

    run = f"{int(time.time())}-{os.getpid()}"
    eid = lambda name: f"smoke-{run}-{name}"  # noqa: E731
    alpha_seq = state["probes"]["alpha"]["max_seq"] + 1
    beta_seq = state["probes"]["beta"]["max_seq"] + 1
    base_ms = (int(time.time() * 1000) // window_ms) * window_ms
    t = lambda offset_s: iso(base_ms + offset_s * 1000)  # noqa: E731

    def reading(event_id, probe, seq, offset_s, dose):
        return {
            "event_id": event_id,
            "probe": probe,
            "seq": seq,
            "observed_at": t(offset_s),
            "dose": dose,
        }

    # -- 1. retransmission -------------------------------------------------
    first = reading(eid("a1"), "alpha", alpha_seq, 10, 10.0)
    status, _, ack1 = req("POST", "/readings", first)
    check(status == 200 and ack1.get("status") == "accepted", "first reading accepted")

    status, headers, ack2 = req("POST", "/readings", first)
    check(
        status == 200 and ack2 == ack1 and headers.get("x-idempotent-replay") == "true",
        "identical retransmission replays the original ack",
    )

    conflict = dict(first, dose=11.0)
    status, _, body = req("POST", "/readings", conflict)
    check(
        status == 409 and body.get("error") == "event_conflict",
        "conflicting retransmission rejected with 409",
        context=f"got {status} {body}",
    )

    # -- 2. out-of-order sealing -------------------------------------------
    req("POST", "/readings", reading(eid("a2"), "alpha", alpha_seq + 1, 290, 15.0))
    req("POST", "/readings", reading(eid("b1"), "beta", beta_seq, 60, 20.0))
    req("POST", "/readings", reading(eid("a3"), "alpha", alpha_seq + 2, 370, 5.0))
    status, _, ack = req("POST", "/readings", reading(eid("b2"), "beta", beta_seq + 1, 400, 5.0))
    sealed = ack.get("sealed_windows", [])
    w0_start = iso(base_ms)
    check(
        status == 200
        and len(sealed) == 1
        and sealed[0]["window_start"] == w0_start
        and sealed[0]["total_dose"] == 45.0
        and sealed[0]["peak_dose"] == 20.0
        and sealed[0]["event_count"] == 3,
        "out-of-order readings merge and seal the window in the submit response",
        context=f"got {sealed}",
    )

    status, _, window = req("GET", f"/windows/{w0_start}")
    check(
        status == 200
        and window.get("status") == "sealed"
        and window.get("total_dose") == 45.0
        and window.get("event_count") == 3
        and window.get("progress_at_seal", {}).get("alpha", {}).get("contiguous_seq")
        == alpha_seq + 2,
        "sealed window readable with totals and both probes' progress",
        context=f"got {window}",
    )

    late = reading(eid("a4"), "alpha", alpha_seq + 3, 120, 50.0)
    status, _, body = req("POST", "/readings", late)
    check(
        status == 409 and body.get("error") == "window_sealed",
        "late reading for sealed window rejected",
        context=f"got {status} {body}",
    )
    _, _, window_after = req("GET", f"/windows/{w0_start}")
    check(
        window_after.get("total_dose") == 45.0 and window_after.get("event_count") == 3,
        "rejected late reading leaves the published summary untouched",
    )

    # the sealed window is NORMAL (45 total, 20 peak): it forms
    # no irradiation incident, and the read endpoints are live
    status, _, incs = req("GET", "/incidents")
    check(
        status == 200 and incs.get("incidents") == [],
        "normal sealed window forms no incident",
        context=f"got {status} {incs}",
    )
    status, _, body = req("GET", f"/incidents/{w0_start}")
    check(
        status == 404 and body.get("detail", {}).get("error") == "no_incident",
        "normal window lookup yields 404 no_incident",
        context=f"got {status} {body}",
    )

    # -- 3. recovery --------------------------------------------------------
    saved_window = window_after
    saved_ack = ack1
    try:
        restart_app_container()
        restarted = wait_healthy()
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] could not restart app container: {exc}")
        restarted = False
    check(restarted, "app container restarted and healthy again")

    if restarted:
        status, _, window = req("GET", f"/windows/{w0_start}")
        check(
            status == 200 and window == saved_window,
            "sealed window identical after restart",
            context=f"got {window}",
        )
        status, headers, ack = req("POST", "/readings", first)
        check(
            status == 200 and ack == saved_ack and headers.get("x-idempotent-replay") == "true",
            "idempotent replay survives restart",
        )
        status, _, body = req("POST", "/readings", late)
        check(
            status == 409 and body.get("error") == "window_sealed",
            "sealed window still rejects late data after restart",
        )
        status, _, state = req("GET", "/state")
        expected_wm = iso(base_ms + 370_000 - lateness_ms)
        check(
            status == 200 and state.get("watermark") == expected_wm,
            "watermark recovered from persistent state",
            context=f"got {state.get('watermark')}, want {expected_wm}",
        )

    print(f"[smoke] passed={CHECKS['passed']} failed={CHECKS['failed']}")
    if CHECKS["failed"]:
        print("[smoke] SMOKE FAILED")
        return 1
    print("[smoke] SMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
