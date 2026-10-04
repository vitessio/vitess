#!/bin/bash
# Runs TLC on the configurations of GRSafety.tla and checks each outcome against the expected one:
# "pass" (no error), "deadlock" (the stuck-state check), or the invariant that must be violated.
#
# Usage: ./run.sh [config ...]      (default: every configuration below, in order)
#
# TLA2TOOLS_JAR  path of tla2tools.jar (default: tla2tools.jar next to this script; never commit it)
# TLC_HEAP       Java heap (default 8g)
# TLC_OUT        directory for the TLC outputs and state files (default: out/ next to this script)
# TLC_SIM_TRACES traces per worker for the simulated configurations (default 100000; seed 1, depth 120)
set -u
cd "$(dirname "$0")"
JAR=${TLA2TOOLS_JAR:-$PWD/tla2tools.jar}
HEAP=${TLC_HEAP:-8g}
OUT=${TLC_OUT:-$PWD/out}
if [ ! -f "$JAR" ]; then
  echo "tla2tools.jar not found at $JAR: set TLA2TOOLS_JAR (https://github.com/tlaplus/tlaplus/releases)" >&2
  exit 2
fi

# config:expected outcome
EXPECTED=(
  s7d_r2:NoDecisionAck
  new1:NoLostAck
  majority_boot:NoLostAck
  s7d_r3:deadlock
  dual_nofence:NoDualBootstrap
  new3:NoAsyncVoter
  fence_snapshot:FenceNotUndone
  no_cas:AdoptOnce
  minority:NoMinorityAck
  relay_cand:NoLostAck
  cand_strict:deadlock
  stale_rpc:NoDualBootstrap
  stale_rpc_timeout:NoDualBootstrap
  dup_record:NoDualBootstrap
  withdraw_any:NoDualBootstrap
  withdraw_starting:NoDualBootstrap
  refusal_stuck:deadlock
  reprobe_stuck:deadlock
  reprobe_noreq:NoLostAck
  current:pass
  s7d_r2_ma_off:pass
  s7d_r2_ma_off_leaves:NoDecisionAck
  s7d_r3_adopt:pass
  tablet:pass
  orcs:pass
  orcs_stall:pass
  stale_rpc_fixed:pass
  refusal_withdraw:pass
  withdraw_orcs:pass
  reprobe:pass
  stale_rec:pass
  tablet_tx2:pass
  integrated:pass
  integrated_core:pass
)

# Too large to explore exhaustively: random behaviors with a fixed seed (-simulate).
SIMULATED=(tablet_tx2 integrated integrated_core)

expected_of() {
  for e in "${EXPECTED[@]}"; do
    [ "${e%%:*}" = "$1" ] && { echo "${e#*:}"; return; }
  done
}

configs=("$@")
if [ ${#configs[@]} -eq 0 ]; then
  for e in "${EXPECTED[@]}"; do configs+=("${e%%:*}"); done
fi

mkdir -p "$OUT"
failed=0
for c in "${configs[@]}"; do
  want=$(expected_of "$c")
  if [ -z "$want" ]; then echo "$c: unknown configuration" >&2; failed=1; continue; fi
  args=(-workers auto -noGenerateSpecTE -config "$c.cfg" -metadir "$OUT/states/$c")
  # Every configuration but the stuck-state check ignores states without successors (budgets used up).
  [ "$want" != deadlock ] && args+=(-deadlock)
  sim=no
  for x in "${SIMULATED[@]}"; do [ "$x" = "$c" ] && sim=yes; done
  [ $sim = yes ] && args+=(-simulate "num=${TLC_SIM_TRACES:-100000}" -depth 120 -seed 1)
  rm -rf "$OUT/states/$c"
  start=$(date +%s)
  java -XX:+UseParallelGC -Xmx"$HEAP" -cp "$JAR" tlc2.TLC "${args[@]}" GRSafety.tla > "$OUT/$c.out" 2>&1
  secs=$(( $(date +%s) - start ))
  rm -rf "$OUT/states/$c"
  states=$(grep -Eo '[0-9,]+ distinct states found' "$OUT/$c.out" | tail -1)
  depth="depth $(grep -Eo 'depth of the complete state graph search is [0-9]+' "$OUT/$c.out" | grep -Eo '[0-9]+$')"
  if [ $sim = yes ]; then
    states="simulation: $(grep -Eo '[0-9]+ traces generated' "$OUT/$c.out" | tail -1), $(grep -Eo 'The number of states generated: [0-9,]+' "$OUT/$c.out" | grep -Eo '[0-9,]+$') states"
    depth="mean trace length $(grep -Eo 'mean=[0-9]+' "$OUT/$c.out" | tail -1 | grep -Eo '[0-9]+')"
  fi
  case "$want" in
    pass)     if [ $sim = yes ]; then grep -q '^Finished in' "$OUT/$c.out" && ! grep -q '^Error:' "$OUT/$c.out"
              else grep -q 'Model checking completed. No error has been found.' "$OUT/$c.out"; fi ;;
    deadlock) grep -q 'Error: Deadlock reached.' "$OUT/$c.out" ;;
    *)        grep -q "Error: Invariant $want is violated." "$OUT/$c.out" ;;
  esac
  if [ $? -eq 0 ]; then result=ok; else result=UNEXPECTED; failed=1; fi
  trace=$(grep -Ec '^State [0-9]+: ' "$OUT/$c.out")
  found=$(grep -Eo '^Error: (Invariant [A-Za-z]+ is violated|Deadlock reached)' "$OUT/$c.out" | head -1 | sed 's/^Error: //')
  if [ -n "$found" ]; then outcome="$found, counterexample of $trace states"; else outcome="no error"; fi
  printf '%-20s expected %-16s %-10s %6ss  %s, %s, %s\n' "$c" "$want" "$result" "$secs" "${states:-?}" "$depth" "$outcome"
done
exit $failed
