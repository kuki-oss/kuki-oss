#!/usr/bin/env bash
# Brings up the Kuki dev stack: Postgres, Kafka, OpenBao (dev mode), migrations,
# the API and its four background workers. Safe to re-run. DEV ONLY.
#
#   VERBOSE=1 ./dev-up.sh   stream raw docker output instead of the spinner
#   NO_COLOR=1 ./dev-up.sh  plain output
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
cd "$here"

# ---------- style ----------
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  C_RESET=$'\033[0m'; C_DIM=$'\033[2m'; C_BOLD=$'\033[1m'
  C_GREEN=$'\033[32m'; C_RED=$'\033[31m'; C_CYAN=$'\033[36m'; C_YELLOW=$'\033[33m'
else
  C_RESET=; C_DIM=; C_BOLD=; C_GREEN=; C_RED=; C_CYAN=; C_YELLOW=
fi

ok()   { printf '  %s✔%s %s\n' "$C_GREEN" "$C_RESET" "$1"; }
die()  { printf '  %s✘%s %s\n' "$C_RED" "$C_RESET" "$1" >&2; exit 1; }
say()  { printf '\n%s%s%s\n' "$C_BOLD" "$1" "$C_RESET"; }

# run_step "label" cmd...  -> spinner + elapsed time; captured output shown only on failure
run_step() {
  local label="$1"; shift
  if [[ -n "${VERBOSE:-}" ]]; then
    say "$label"; "$@"; return
  fi
  local log; log="$(mktemp)"
  local frames=(⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏) i=0 start=$SECONDS
  "$@" >"$log" 2>&1 &
  local pid=$!
  if [[ -t 1 ]]; then
    while kill -0 "$pid" 2>/dev/null; do
      printf '\r  %s%s%s %s %s(%ss)%s' "$C_CYAN" "${frames[i++ % 10]}" "$C_RESET" \
        "$label" "$C_DIM" "$((SECONDS - start))" "$C_RESET"
      sleep 0.1
    done
    printf '\r\033[K'
  fi
  if wait "$pid"; then
    ok "$label ${C_DIM}($((SECONDS - start))s)${C_RESET}"
    rm -f "$log"
  else
    printf '  %s✘%s %s\n\n' "$C_RED" "$C_RESET" "$label" >&2
    cat "$log" >&2
    rm -f "$log"
    return 1
  fi
}

quips=(
  "Waking the workers..."
  "Warming up the event bus..."
  "Teaching the evaluator to wake up..."
  "Untangling the outbox..."
  "Aligning the ontology..."
  "Reticulating recommendations..."
)
printf '%s' "$C_YELLOW"
cat <<'EOF'
     _  __     _    _
    | |/ /   _| | _(_)
    | ' / | | | |/ / |
    | . \ |_| |   <| |
    |_|\_\__,_|_|\_\_|   dev stack
EOF
printf '%s%s%s\n' "$C_DIM" "${quips[RANDOM % ${#quips[@]}]}" "$C_RESET"

# ---------- preflight ----------
say "Preflight"
need() { command -v "$1" >/dev/null 2>&1 || die "missing required tool: $1"; }
need docker; need openssl; need curl
docker compose version >/dev/null 2>&1 || die "docker compose v2 is required"
ok "docker, compose v2, openssl, curl"

umask 077
mkdir -p secrets

if [[ ! -f .env ]]; then
  cat > .env <<EOF
HOST_UID=$(id -u)
HOST_GID=$(id -g)
BAO_ROOT_TOKEN=$(openssl rand -hex 16)
KUKI_KEY_ENV=live
EOF
  ok "created .env"
fi
set -a; . ./.env; set +a

[[ -f secrets/pg_password ]] || printf %s "$(openssl rand -hex 24)" > secrets/pg_password
printf 'postgres://postgres:%s@postgres:5432/kuki' "$(cat secrets/pg_password)" > secrets/database_url
[[ -f secrets/bao_token ]] || : > secrets/bao_token   # must exist before compose starts
ok "secrets in place"

# ---------- steps ----------
bao() {
  docker compose exec -T -e BAO_ADDR=http://127.0.0.1:8200 -e BAO_TOKEN="$BAO_ROOT_TOKEN" openbao bao "$@"
}

configure_transit() {
  bao secrets list | grep -q '^transit/' || bao secrets enable transit
  if ! bao read -field=name transit/keys/kuki-api-keys >/dev/null 2>&1; then
    bao write -f transit/keys/kuki-api-keys type=hmac key_size=32 >/dev/null 2>&1 \
      || { echo "(hmac key type rejected, falling back to default key type)"; bao write -f transit/keys/kuki-api-keys >/dev/null; }
  fi
  printf 'path "transit/hmac/kuki-api-keys/*" {\n  capabilities = ["update"]\n}\n' \
    | bao policy write kuki-api - >/dev/null
  # Dev token. No renewal loop yet: it lasts 32 days, and dev mode resets on restart anyway.
  printf %s "$(bao token create -policy=kuki-api -period=768h -field=token)" > secrets/bao_token
}

port="${KUKI_PORT:-8080}"
workers=(outbox evaluate actions webhooks-fanout)

wait_ready() {
  for _ in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:${port}/v1/health/ready" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

say "Bringing it up"
run_step "Building the kuki image (the first build is the long one)" docker compose build kuki
run_step "Starting Postgres, Kafka and OpenBao"                      docker compose up -d --wait postgres kafka openbao
run_step "Configuring OpenBao transit and minting a token"           configure_transit
run_step "Running migrations"                                        docker compose run --rm migrate
run_step "Starting the API and workers"                              docker compose up -d --force-recreate kuki "${workers[@]}"
run_step "Waiting for the API to report ready"                       wait_ready || {
  docker compose logs --tail=50 kuki >&2
  die "kuki did not become ready"
}

# ---------- verify ----------
sleep 3
running="$(docker compose ps --status running --services)"
missing=()
for svc in kuki "${workers[@]}"; do
  grep -qx "$svc" <<<"$running" || missing+=("$svc")
done
if (( ${#missing[@]} )); then
  printf '\n  %s✘%s not running: %s\n' "$C_RED" "$C_RESET" "${missing[*]}" >&2
  docker compose logs --tail=30 "${missing[@]}" >&2
  exit 1
fi
ok "all services running: kuki ${workers[*]}"

printf '\n%s✨ Kuki is up on http://127.0.0.1:%s%s\n' "$C_GREEN$C_BOLD" "$port" "$C_RESET"
printf '   Mint an API key:  %s%s/scripts/mint-dev-key.sh%s\n' "$C_CYAN" "$here" "$C_RESET"
printf '   Tail the logs:    %sdocker compose -f %s/docker-compose.yml logs -f%s\n' "$C_DIM" "$here" "$C_RESET"
printf '   Tear it down:     %s%s/scripts/dev-down.sh%s\n\n' "$C_DIM" "$here" "$C_RESET"
