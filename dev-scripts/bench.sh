#!/usr/bin/env bash
# Usage: dev-scripts/bench.sh <dirA> <dirB> [rounds=5] [outdir]
# Alternates A/B clean builds (order flips each round), waits for an idle
# machine, logs vmstat per build, then prints a summary.
# Extra cargo flags: CARGO_ARGS="--release" dev-scripts/bench.sh ...
set -u
A=$(realpath "$1"); B=$(realpath "$2"); N=${3:-5}
OUT=${4:-$HOME/timings/bench-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
echo "$A" > "$OUT/a.dir"; echo "$B" > "$OUT/b.dir"

wait_idle() { until awk '{exit !($1<0.5)}' /proc/loadavg; do sleep 10; done; }

one() {  # one <dir> <label>
  ( cd "$1" || exit 1
    cargo clean >/dev/null 2>&1; sync; sleep 20; wait_idle
    uptime > "$OUT/$2.uptime"
    vmstat 5 > "$OUT/$2.vmstat" & VM=$!
    cargo build --timings ${CARGO_ARGS:-} > "$OUT/$2.log" 2>&1; RC=$?
    kill $VM
    if [ $RC -eq 0 ]; then
      cp target/cargo-timings/cargo-timing.html "$OUT/$2.html"
      echo "done $2"
    else
      echo "BUILD FAILED $2 (see $OUT/$2.log)"
    fi )
}

for i in $(seq 1 "$N"); do
  if (( i % 2 )); then one "$A" "a-$i"; one "$B" "b-$i"
  else                 one "$B" "b-$i"; one "$A" "a-$i"; fi
done

python3 "$(dirname "$0")/bench_summary.py" "$OUT"
