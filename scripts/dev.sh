#!/bin/bash
# Start a disposable dev environment: Docker Redis + seeded demo data +
# dramatiq-monitor with actions ENABLED (this is throwaway demo data, not a
# real deployment — see docs/how-to/connect-to-a-real-deployment.md for that
# case, which should start read-only instead).
#
# Usage:
#   ./scripts/dev.sh                # serve on :8321
#   ./scripts/dev.sh --port 9000    # different port
#
# Tear down with: ./scripts/stop.sh

set -eo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER_NAME="dramatiq-monitor-demo"
REDIS_PORT="${REDIS_PORT:-6391}"
DM_PORT="${DM_PORT:-8321}"
CONDA_ENV="${CONDA_ENV:-dramatiq}"

# Absolute env binary path, not `conda run -n ...` — conda run relies on PATH
# lookup, and a shell profile that pre-activates a different default env
# (common — e.g. `conda activate` in .bashrc) makes it silently run the WRONG
# env's python/dramatiq-monitor instead of failing loudly. See Makefile.
CONDA_BASE="$(conda info --base 2>/dev/null)"
if [ -z "$CONDA_BASE" ] || [ ! -x "$CONDA_BASE/envs/$CONDA_ENV/bin/python" ]; then
    echo "ERROR: conda env '$CONDA_ENV' not found (checked $CONDA_BASE/envs/$CONDA_ENV)."
    echo "Create it with: conda create -n $CONDA_ENV python=3.11 && make install"
    exit 1
fi
PYTHON="$CONDA_BASE/envs/$CONDA_ENV/bin/python"
DRAMATIQ_MONITOR="$CONDA_BASE/envs/$CONDA_ENV/bin/dramatiq-monitor"

for arg in "$@"; do
    case "$arg" in
        --port) shift ;;
        --port=*) DM_PORT="${arg#--port=}" ;;
        *) if [[ "$arg" =~ ^[0-9]+$ ]]; then DM_PORT="$arg"; fi ;;
    esac
done

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker is required for the demo Redis container. Install Docker, or run manually per docs/how-to/run-locally.md."
    exit 1
fi

if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "Reusing running demo Redis container '$CONTAINER_NAME' on port $REDIS_PORT"
else
    echo "Starting demo Redis container '$CONTAINER_NAME' on 127.0.0.1:$REDIS_PORT"
    docker run -d --rm --name "$CONTAINER_NAME" -p "127.0.0.1:${REDIS_PORT}:6379" redis:7-alpine >/dev/null
    # give the container a moment to accept connections
    for _ in $(seq 1 20); do
        if docker exec "$CONTAINER_NAME" redis-cli ping >/dev/null 2>&1; then break; fi
        sleep 0.25
    done
fi

echo "Seeding demo data..."
DM_SEED_URL="redis://127.0.0.1:${REDIS_PORT}" "$PYTHON" "$REPO_ROOT/scripts/seed_demo.py"

echo "Starting dramatiq-monitor [WRITE — demo data, actions enabled] on http://127.0.0.1:${DM_PORT}"
exec "$DRAMATIQ_MONITOR" \
    --redis-host 127.0.0.1 --redis-port "$REDIS_PORT" \
    --dbs 0 \
    --dead-message-ttl-ms 7200000 \
    --host 127.0.0.1 --port "$DM_PORT"
