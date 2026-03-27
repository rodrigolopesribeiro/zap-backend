#!/usr/bin/env bash
set -euo pipefail
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-5000}"
python web_app.py --host "$HOST" --port "$PORT" --debug false
