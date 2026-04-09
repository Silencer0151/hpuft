#!/usr/bin/env bash
# hpuft test runner for Linux / macOS
#
# Usage:
#   ./tests/run_tests.sh [extra args passed to test_hpuft.py]
#
# Examples:
#   ./tests/run_tests.sh
#   ./tests/run_tests.sh --no-build
#   ./tests/run_tests.sh --addr 192.168.50.9:9001 --recv-addr 192.168.50.9:9000
#   ./tests/run_tests.sh --keep

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Activate mamba/conda dev_env if available ─────────────────────────────────
if command -v mamba &>/dev/null; then
    eval "$(mamba shell hook --shell bash 2>/dev/null || true)"
    mamba activate dev_env 2>/dev/null || true
elif command -v conda &>/dev/null; then
    eval "$(conda shell.bash hook 2>/dev/null || true)"
    conda activate dev_env 2>/dev/null || true
fi

# ── Prefer python3, fall back to python ──────────────────────────────────────
PYTHON=$(command -v python3 2>/dev/null || command -v python)

exec "$PYTHON" "$SCRIPT_DIR/test_hpuft.py" "$@"
