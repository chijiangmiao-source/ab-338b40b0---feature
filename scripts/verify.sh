#!/bin/sh
# One-shot acceptance: build checks, code tests, HTTP smoke.
# Any failure aborts with a non-zero exit code (set -e).
set -eu
cd /srv

# the module-level app instance created on import must not touch real data
export DATABASE_PATH="${DATABASE_PATH:-/tmp/radiation-verify-import.db}"

echo "[verify] 1/3 build check: byte-compile sources and import the app"
python -m compileall -q app tests scripts
python -c "import app.main, app.engine, app.storage, app.config, app.models; print('[verify] imports OK')"

echo "[verify] 2/3 code tests (pytest)"
python -m pytest tests -q

echo "[verify] 3/3 HTTP smoke: retransmission, out-of-order sealing, recovery"
python scripts/smoke.py

echo "[verify] ALL CHECKS PASSED"
