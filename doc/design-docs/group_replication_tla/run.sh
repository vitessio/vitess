#!/bin/bash
# Runs TLC on the configurations of GRSafety.tla and checks each outcome against the expected one:
# "pass" (no error), "deadlock" (the stuck-state check), "liveness" (a temporal property violated), or the invariant
# that must be violated.
#
# Usage: ./run.sh [config ...]      (default: every configuration below, in order)
#
# TLA2TOOLS_JAR  path of tla2tools.jar (default: tla2tools.jar next to this script; never commit it)
# TLC_HEAP       Java heap (default 8g)
# TLC_OUT        directory for the TLC outputs and state files (default: out/ next to this script)
# TLC_WORKERS    TLC workers (default auto)
# TLC_SIM_TRACES traces per worker for the simulated configurations (default 100000; seed 1, depth 120)
set -u
cd "$(dirname "$0")"
JAR=${TLA2TOOLS_JAR:-$PWD/tla2tools.jar}
HEAP=${TLC_HEAP:-8g}
WORKERS=${TLC_WORKERS:-auto}
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
  # second milestone: validation and findings (each stops at its first violation)
  voters_nogroup:NoLostAck
  voters_nosettle:NoLostAck
  voters_minority:NoVoterMinority
  voters_split_slow:NoLostAck
  voters_split_drop:NoLostAck
  voters_split_noreach:NoLostAck
  voters_split_nonvoter:NoLostAck
  undo_nocheck:NoLostAck
  setrw_nocheck:NoLostAck
  prs_demote_fail:NoDecisionAck
  prs_demote_restart:NoLostAck
  prs_demote_fence:FenceNotUndone
  init_rerun:NoDualBootstrap
  init_fault:OneWritablePrimary
  init_direct:NoLostAck
  init_orc:NoDualBootstrap
  init_orc_lost:NoLostAck
  init_orc_vgtid:NoDualBootstrap
  init_guard_intent:NoDualBootstrap
  init_guard_noint:NoDualBootstrap
  init_orc_unrec:NoLostAck
  # second milestone: the current design
  voters:pass
  voters_fixed:pass
  prs:pass
  ers:pass
  setrw:pass
  init_alone:pass
  live:pass
  live_init:pass
  live_init_prs:pass
  live_init_prs_fail:liveness
  live_init_prs_adopt:pass
  live_voters:pass
  live_voters_fence:pass
  # third milestone: the voter redesign
  swap_nosettle:NoLostAck

  swap_split_slow:NoLostAck
  delete_remove_nop1:NoVoterMinority
  live_nospare:liveness
  live_delete_active:pass
  delete_nogroup_lost:NoLostAck
  force_live:NoNonVoterServes
  force_lost:NoLostAck
  wit_force_grow:WitJoinSpare
  wit_prs_swap:WitPrsSwapDemoted
  wit_prs_swap_serves:WitPrsSwap
  delete_primary_window:NoLostAckAfterDelete
  live_delete_active_nomove:liveness
  delete_nogroup_nodel:NoLostAckExceptDeleted
  swap_fixed:pass
  swap_spare_active:pass
  swap_code:pass
  grow_fixed:pass
  swap_split_prompt:pass
  delete_remove:pass
  delete_nogroup:pass
  force_down:pass
  force_grow:pass
  prs_swap:pass
  prs_swap_nodemoted:NoNonVoterServes
  prs_swap_noholds:NoLostAck
  prs_swap_norevertcheck:NoVoterMinority
  swap_live:pass
  swap_live_small:pass
  delete_primary:pass
  delete_swap:pass
  delete_remove_noview:pass
  live_swap:pass
  live_nospare_op:pass
  live_nonvoter_primary:pass
  live_delete:pass
  init_direct_ro:pass
  voters_split:pass
  voters_split1:pass
  voters_split_prompt:pass
  voters_code:pass
  # fourth milestone: partial partitions
  remove_partial:pass
  remove_partial_noview:pass
  grow_partial:pass
  swap_partial:pass
  swap_partial_nop3:NoVoterMinority
  swap_partial_crash:pass
  swap_partial_nop3_crash:NoVoterMinority
  swap_partial_nop3_lost:pass
  flag1_stale:NoStalePrimaryRecorded
  flag1_safety:pass
  flag1_fixed:pass
  flag1_safety_faults:pass
  flag1_fixed_faults:NoStalePrimaryRecorded
  flag1_fixed_all:pass
  flag1_fixed_all_faults:pass
  live_flag1:pass
  live_flag1_fixed:pass
  live_flag1_fixed_all:pass
  # second milestone: the fixes of the three findings
  prs_fixed:pass
  prs_fixed_restart:pass
  init_orc_fixed:pass
  init_orc_fixed_vgtid:pass
  init_orc_guard:pass
  init_fault_fixed:pass
  init_orc_adopt:pass
  fixed:pass
)

# Too large to explore exhaustively: random behaviors with a fixed seed (-simulate).
SIMULATED=(tablet_tx2 integrated integrated_core voters_split fixed init_orc_guard swap_spare_active)

# Simulated configurations with fewer traces per worker than TLC_SIM_TRACES (four tablets, longer traces).
SIMNUM=(swap_spare_active:40000)

# Temporal properties: the module GRLiveness.tla, which adds fairness to GRSafety.tla.
LIVENESS=(live live_init live_init_prs live_init_prs_fail live_init_prs_adopt live_voters live_voters_fence live_delete_active live_delete_active_nomove live_swap live_nospare live_nospare_op live_nonvoter_primary live_delete live_flag1 live_flag1_fixed live_flag1_fixed_all)

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
  args=(-workers "$WORKERS" -noGenerateSpecTE -config "$c.cfg" -metadir "$OUT/states/$c")
  # A configuration without the stuck-state check (STUCK_CHECK) ignores states without successors (budgets
  # used up); one with it reports them, as a deadlock, unless Done marks them healthy.
  grep -q '^  STUCK_CHECK = TRUE$' "$c.cfg" || args+=(-deadlock)
  sim=no
  for x in "${SIMULATED[@]}"; do [ "$x" = "$c" ] && sim=yes; done
  num=${TLC_SIM_TRACES:-100000}
  for x in "${SIMNUM[@]}"; do [ "${x%%:*}" = "$c" ] && num=${x#*:}; done
  [ $sim = yes ] && args+=(-simulate "num=$num" -depth 120 -seed 1)
  module=GRSafety.tla
  for x in "${LIVENESS[@]}"; do [ "$x" = "$c" ] && module=GRLiveness.tla; done
  rm -rf "$OUT/states/$c"
  start=$(date +%s)
  java -XX:+UseParallelGC -Xmx"$HEAP" -cp "$JAR" tlc2.TLC "${args[@]}" "$module" > "$OUT/$c.out" 2>&1
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
    liveness) grep -Eq '^Error: Temporal propert(ies were|y [A-Za-z]+ was) violated' "$OUT/$c.out" ;;
    *)        grep -q "Error: Invariant $want is violated." "$OUT/$c.out" ;;
  esac
  if [ $? -eq 0 ]; then result=ok; else result=UNEXPECTED; failed=1; fi
  trace=$(grep -Ec '^State [0-9]+: ' "$OUT/$c.out")
  found=$(grep -Eo '^Error: (Invariant [A-Za-z]+ is violated|Deadlock reached|Temporal properties were violated|Temporal property [A-Za-z]+ was violated)' "$OUT/$c.out" | head -1 | sed 's/^Error: //')
  if [ -n "$found" ]; then outcome="$found, counterexample of $trace states"; else outcome="no error"; fi
  printf '%-20s expected %-16s %-10s %6ss  %s, %s, %s\n' "$c" "$want" "$result" "$secs" "${states:-?}" "$depth" "$outcome"
done
exit $failed
