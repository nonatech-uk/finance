#!/bin/bash
# Start both the Monzo OAuth server and the FastAPI API server.
set -euo pipefail

# Monzo OAuth server (port 9876) in background
python src/ingestion/monzo_auth.py &

# FastAPI API server (port 8000) in foreground
exec uvicorn src.api.app:app --host 0.0.0.0 --port 8000
