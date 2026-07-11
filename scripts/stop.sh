#!/bin/bash
# Tear down what scripts/dev.sh started: the demo Redis container and any
# running dramatiq-monitor dev server process.

CONTAINER_NAME="dramatiq-monitor-demo"

if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$CONTAINER_NAME"; then
    echo "Stopping demo Redis container '$CONTAINER_NAME'..."
    docker stop "$CONTAINER_NAME" >/dev/null
else
    echo "No demo Redis container running."
fi

if pgrep -f "dramatiq-monitor --redis-host" >/dev/null 2>&1; then
    echo "Stopping dramatiq-monitor dev server..."
    pkill -f "dramatiq-monitor --redis-host" || true
else
    echo "No dramatiq-monitor dev server running."
fi
