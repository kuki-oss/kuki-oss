#!/usr/bin/env bash
# Stops the Kuki dev stack. --purge also deletes the database volumes and generated secrets.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--purge" ]]; then
  echo "🧹 Purging containers, volumes, secrets and .env..."
  docker compose down -v
  rm -rf secrets .env
  echo "✨ Clean slate."
else
  echo "🌙 Stopping the stack (data kept)..."
  docker compose down
  echo "💤 Stopped. Run dev-up.sh to bring it back."
fi
