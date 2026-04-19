#!/usr/bin/env bash
#
# Start a local backend (optional) and run CLI Python integration tests.
#
# Usage:
#   ./tests/integration/run.sh
#   ./tests/integration/run.sh --compose-file ./tests/integration/docker-compose.integration.yml
#   ./tests/integration/run.sh --no-compose
#   ./tests/integration/run.sh --keep
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv-tests"
KEEP=false
NO_COMPOSE=false
NO_BUILD_CLI=false
COMPOSE_FILE="${RAWTREE_COMPOSE_FILE:-$PROJECT_ROOT/tests/integration/docker-compose.integration.yml}"
PYTEST_ARGS=()
PYTHON_BIN="${PYTHON_BIN:-}"

if [ -z "$PYTHON_BIN" ]; then
    if command -v python >/dev/null 2>&1 && python -c 'import sys; raise SystemExit(0 if sys.version_info.major == 3 else 1)'; then
        PYTHON_BIN="python"
    elif command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN="python3"
    else
        echo "ERROR: Python 3 is required but neither 'python' nor 'python3' is available."
        exit 1
    fi
fi

while [ "$#" -gt 0 ]; do
    case "$1" in
        --keep)
            KEEP=true
            shift
            ;;
        --no-compose)
            NO_COMPOSE=true
            shift
            ;;
        --no-build-cli)
            NO_BUILD_CLI=true
            shift
            ;;
        --compose-file)
            if [ "$#" -lt 2 ]; then
                echo "ERROR: --compose-file requires a path argument"
                exit 1
            fi
            COMPOSE_FILE="$2"
            shift 2
            ;;
        *)
            PYTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

if [ "${COMPOSE_FILE#/}" = "$COMPOSE_FILE" ]; then
    COMPOSE_FILE="$PROJECT_ROOT/$COMPOSE_FILE"
fi

if [ "$NO_COMPOSE" = false ]; then
    if [ ! -f "$COMPOSE_FILE" ]; then
        echo "ERROR: compose file not found: $COMPOSE_FILE"
        exit 1
    fi

    if docker compose version >/dev/null 2>&1; then
        COMPOSE_CMD=(docker compose)
    elif command -v docker-compose >/dev/null 2>&1; then
        COMPOSE_CMD=(docker-compose)
    else
        echo "ERROR: Docker Compose is required."
        exit 1
    fi
fi

compose() {
    "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
    local exit_code=$?

    if [ "$NO_COMPOSE" = false ]; then
        if [ "$exit_code" -ne 0 ]; then
            echo ""
            echo "==> Dumping backend compose logs (failure)..."
            compose logs --no-color || true
        fi

        if [ "$KEEP" = false ]; then
            echo ""
            echo "==> Stopping backend stack..."
            compose down -v --timeout 10 || true
        else
            echo ""
            echo "==> Backend stack left running (--keep)."
        fi
    fi

    exit "$exit_code"
}
trap cleanup EXIT

if [ ! -d "$VENV_DIR" ]; then
    echo "==> Creating virtual environment at $VENV_DIR..."
    "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

echo "==> Installing Python test dependencies..."
pip install -q --upgrade pip
pip install -q pytest requests

if [ "$NO_BUILD_CLI" = false ]; then
    echo "==> Building rtree CLI binary..."
    (cd "$PROJECT_ROOT" && cargo build --locked --release)
else
    echo "==> Skipping CLI build (--no-build-cli)."
fi

export RTREE_BIN="${RTREE_BIN:-$PROJECT_ROOT/target/release/rtree}"
if [ ! -x "$RTREE_BIN" ]; then
    echo "ERROR: RTREE_BIN is not executable: $RTREE_BIN"
    exit 1
fi

export RAWTREE_URL="${RAWTREE_URL:-http://localhost:3000}"
export RAWTREE_FRONTEND_URL="${RAWTREE_FRONTEND_URL:-http://localhost:8080}"
export RAWTREE_GITHUB_CLIENT_ID="${RAWTREE_GITHUB_CLIENT_ID:-ci-github-client-id}"
export RAWTREE_GITHUB_CLIENT_SECRET="${RAWTREE_GITHUB_CLIENT_SECRET:-ci-github-client-secret}"
export RAWTREE_GOOGLE_CLIENT_ID="${RAWTREE_GOOGLE_CLIENT_ID:-ci-google-client-id}"
export RAWTREE_GOOGLE_CLIENT_SECRET="${RAWTREE_GOOGLE_CLIENT_SECRET:-ci-google-client-secret}"

if [ "$NO_COMPOSE" = false ]; then
    export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-rawtree-cli-it-$$}"

    echo "==> Pulling deterministic backend stack images..."
    compose pull

    echo "==> Starting backend stack..."
    compose up -d --wait backend
else
    echo "==> Skipping backend startup (--no-compose)."
fi

echo "==> Waiting for backend health..."
MAX_WAIT=120
ELAPSED=0
while true; do
    STATUS="$(curl -s -o /dev/null -w "%{http_code}" "$RAWTREE_URL/health" 2>/dev/null || echo "000")"
    if [ "$STATUS" = "200" ]; then
        echo "    Backend healthy (${ELAPSED}s)"
        break
    fi
    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
        echo "ERROR: Backend not healthy after ${MAX_WAIT}s (last status: $STATUS)"
        exit 1
    fi
    sleep 1
    ELAPSED=$((ELAPSED + 1))
done

echo ""
echo "==> Running CLI integration tests..."
echo ""
pytest "$SCRIPT_DIR" -v --tb=short ${PYTEST_ARGS[@]+"${PYTEST_ARGS[@]}"}

echo ""
echo "==> CLI integration tests passed."
