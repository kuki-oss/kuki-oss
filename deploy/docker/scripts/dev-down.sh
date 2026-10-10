#!/usr/bin/env bash
# Stops the Kuki dev stack. --purge also deletes the database volumes and generated secrets.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${1:-}" == "--purge" ]]; then
  docker compose down -v
  rm -rf secrets .env
else
  docker compose down
fi
