#!/usr/bin/env bash
# End-to-end test driver.
# Builds the Docker image, starts the container, runs e2e_test.py, then cleans up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONTAINER="nc-e2e-$(date +%s)"
PORT=19000

cleanup() { docker rm -f "$CONTAINER" 2>/dev/null || true; }
trap cleanup EXIT

echo "▶  Building Docker image..."
docker build -t netcdf-merge-server "$REPO_ROOT"

echo "▶  Starting container on port $PORT..."
docker run -d --name "$CONTAINER" -p "$PORT:8000" netcdf-merge-server

echo "▶  Running E2E tests..."
BASE_URL="http://localhost:$PORT" python3 "${SCRIPT_DIR}/e2e_test.py"
