#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_PORT="${API_PORT:-8000}"
DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://${USER}@127.0.0.1:5432/kenya_dashboard_dev}"

cd "${ROOT_DIR}"

cleanup() {
    if [[ -n "${API_PID:-}" ]]; then
        kill "${API_PID}" 2>/dev/null || true
        wait "${API_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

env DATABASE_URL="${DATABASE_URL}" \
    python -m uvicorn dashboard.api.app.main:app \
    --reload --host 127.0.0.1 --port "${API_PORT}" &
API_PID=$!

if [[ "${API_PORT}" != "8000" ]]; then
    echo "The Vite development proxy is configured for API port 8000." >&2
    echo "Use API_PORT=8000 for the local combined launcher." >&2
    exit 2
fi

cd "${ROOT_DIR}/dashboard/web"
npm run dev
