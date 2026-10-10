#!/usr/bin/env bash
# Mints a dev API key with `kuki keys create`.
# Usage: mint-dev-key.sh [org-slug] [comma,separated,scopes]
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

slug="${1:-dev-org}"
scopes="${2:-ingest:write,ingest:read}"

# -T: no TTY, so stdout carries only what the CLI prints (the key) and no \r.
kuki() { docker compose run --rm -T admin "$@"; }

kuki orgs create --slug "$slug" >/dev/null
key="$(kuki keys create --org "$slug" --name dev --scopes "$scopes")"

echo
echo "API key (shown once):"
echo "  $key"
echo
echo "Try:  curl -s -H \"Authorization: Bearer $key\" -H 'content-type: application/json' -d '{\"query\":\"{ viewer { principalId scopes organisation { slug } } }\"}' localhost:${KUKI_PORT:-8080}/v1/graphql"
