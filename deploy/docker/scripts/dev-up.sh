#!/usr/bin/env bash
# Brings up the Kuki dev stack: Postgres, OpenBao (dev mode), migrations, API.
# Safe to re-run. DEV ONLY.
set -euo pipefail
cd "$(dirname "$0")/.."

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }; }
need docker; need openssl; need curl
docker compose version >/dev/null 2>&1 || { echo "docker compose v2 is required" >&2; exit 1; }

umask 077
mkdir -p secrets

if [[ ! -f .env ]]; then
  cat > .env <<EOF
HOST_UID=$(id -u)
HOST_GID=$(id -g)
BAO_ROOT_TOKEN=$(openssl rand -hex 16)
KUKI_KEY_ENV=live
EOF
fi
set -a; . ./.env; set +a

[[ -f secrets/pg_password ]] || printf %s "$(openssl rand -hex 24)" > secrets/pg_password
printf 'postgres://postgres:%s@postgres:5432/kuki' "$(cat secrets/pg_password)" > secrets/database_url
[[ -f secrets/bao_token ]] || : > secrets/bao_token   # must exist before compose starts

echo "==> building image"
docker compose build kuki

echo "==> starting postgres and openbao"
docker compose up -d --wait postgres openbao

bao() {
  docker compose exec -T -e BAO_ADDR=http://127.0.0.1:8200 -e BAO_TOKEN="$BAO_ROOT_TOKEN" openbao bao "$@"
}

echo "==> configuring openbao transit"
bao secrets list | grep -q '^transit/' || bao secrets enable transit
if ! bao read -field=name transit/keys/kuki-api-keys >/dev/null 2>&1; then
  bao write -f transit/keys/kuki-api-keys type=hmac key_size=32 >/dev/null 2>&1 \
    || { echo "   (hmac key type rejected, falling back to default key type)"; bao write -f transit/keys/kuki-api-keys >/dev/null; }
fi
printf 'path "transit/hmac/kuki-api-keys/*" {\n  capabilities = ["update"]\n}\n' \
  | bao policy write kuki-api - >/dev/null

# Dev token. No renewal loop yet: it lasts 32 days, and dev mode resets on restart anyway.
printf %s "$(bao token create -policy=kuki-api -period=768h -field=token)" > secrets/bao_token

echo "==> running migrations"
docker compose run --rm migrate

echo "==> starting kuki"
docker compose up -d --force-recreate kuki

port="${KUKI_PORT:-8080}"
ready=0
for _ in $(seq 1 40); do
  if curl -fsS "http://127.0.0.1:${port}/v1/health/ready" >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  echo "kuki did not become ready; last logs:" >&2
  docker compose logs --tail=50 kuki >&2
  exit 1
fi

echo
echo "Kuki is ready on http://127.0.0.1:${port}"
echo "Mint an API key:  ./scripts/mint-dev-key.sh"
