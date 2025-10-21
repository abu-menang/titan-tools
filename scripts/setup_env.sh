#!/usr/bin/env bash
# Simple helper to create/refresh the project virtual environment and install titan-tools.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "Using project root: $ROOT_DIR"
echo "Virtual environment: $VENV_DIR"

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment with $PYTHON_BIN..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
else
  echo "Virtual environment already exists; reusing."
fi

source "$VENV_DIR/bin/activate"

pip install --upgrade pip
pip install -e "$ROOT_DIR"

cat <<'EOF'

Environment ready.
Run the following to activate in this shell:
  source .venv/bin/activate

After activation, the CLI entry points (vid-mkv-clean, vid-mkv-scan, vid-rename)
will resolve to the virtual environment's bin directory.
EOF
