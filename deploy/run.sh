#!/bin/bash
# Build and run the finance-sync container.
#
# Prerequisites:
#   /opt/finance/secrets/.env       — DB creds, Wise token, Monzo client ID/secret
#   /opt/finance/secrets/tokens.json — Monzo OAuth tokens (created on first auth)
#
# Usage:
#   ./deploy/run.sh              # build + run
#   ./deploy/run.sh --build-only # just build

set -euo pipefail

IMAGE="finance-sync:latest"
CONTAINER="finance-sync"
SECRETS_DIR="/opt/finance/secrets"

# Build
echo "Building $IMAGE..."
podman build -t "$IMAGE" -f Containerfile .

if [ "${1:-}" = "--build-only" ]; then
    echo "Build complete."
    exit 0
fi

# Stop existing
if podman container exists "$CONTAINER" 2>/dev/null; then
    echo "Stopping existing container..."
    podman stop "$CONTAINER" || true
    podman rm "$CONTAINER" || true
fi

# Ensure tokens file exists (will be populated on first auth)
touch "$SECRETS_DIR/tokens.json"

echo "Starting $CONTAINER..."
podman run -d \
    --name "$CONTAINER" \
    --restart unless-stopped \
    --network host \
    -v "$SECRETS_DIR/.env:/app/config/.env:ro" \
    -v "$SECRETS_DIR/tokens.json:/app/tokens.json:rw" \
    -e MONZO_TOKEN_FILE=/app/tokens.json \
    -e MONZO_REDIRECT_URI="https://finance.mees.st/oauth/callback" \
    -e HEALTHCHECK_MONZO_URL="${HEALTHCHECK_MONZO_URL:-}" \
    -e HEALTHCHECK_WISE_URL="${HEALTHCHECK_WISE_URL:-}" \
    "$IMAGE"

echo "Container started. Auth UI at https://finance.mees.st/"
echo "Logs: podman logs -f $CONTAINER"
