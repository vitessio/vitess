#!/bin/bash
# Runs the scenario plan behind doc/failover-audit/SemiSyncFailover.md, one phase at a time.
# Must be run as root, like chaos_run.sh. See README.md for the prerequisites and for which
# Vitess binaries each phase needs in bin/.
#
#   chaos_plan.sh main     the matrix on main: every core scenario under semisync-3vtorc and under
#                          semisync-1vtorc with the VTOrc next to the primary (colo) and away from
#                          it (remote), the single-VTOrc scenarios, and the relay log scenarios.
#   chaos_plan.sh probe    P1 and P2 with the write probe, twice, under semisync-3vtorc and
#                          semisync-1vtorc (colo). Binaries of main.
#   chaos_plan.sh fixed    the scenarios that failed on main because of B1, 3A or B6, twice per
#                          profile. Binaries of this branch.
#
# Results go to $CHAOS_PLAN_RESULTS/<run>/ (default /home/$RUN_USER/chaos-results), one directory
# per profile and run; chaos_summary.py turns them into the report's tables.
set -u
DIR=$(cd "$(dirname "$0")" && pwd)
RUN_USER=${RUN_USER:-ubuntu}
OUT=${CHAOS_PLAN_RESULTS:-/home/$RUN_USER/chaos-results}
M="$DIR/chaos_matrix.sh"

CORE="TestS1KillPrimaryMysqld TestS1bKillPrimaryMysqldAutoRestart TestS2HangPrimary TestS3IsolatePrimary
  TestS4ReplicationPartition TestS5PrimaryCrashWithAckerDown TestS5bPrimaryCrashWithAckerIsolated
  TestS6OneVTOrcCutFromPrimary TestS6bAllVTOrcsCutFromPrimary TestS7DoubleFailure TestS7bKillDuringFailover
  TestS7cFlappingPrimary TestS7dFlappingPrimaryLong TestS8VTOrcCutFromTopoDuringERS
  TestS8bVTOrcCutFromTopoBeforeFailure TestS9PrimaryCellOutage TestS9iPrimaryCellPartition
  TestS10GlobalTopoHangDuringERS"
SINGLE="TestV1SingleVTOrcReplicaCellPartitioned TestV2SingleVTOrcWithPrimaryCutFromReplicas
  TestV3SingleVTOrcKillPrimary TestV4SingleVTOrcPrimaryCellOutage"
RELAY="TestS11RelayLogDiscardGraceful TestS11kRelayLogDiscardKill9 TestS11bRelayLogDiscardFixReplica
  TestS11cRelayLogDiscardFixReplicaCut TestS12aERSReplicaRestartToOldPrimary TestS12bERSPromotesRestartedReplica
  TestS12b2ERSPromotesRestartedReplicaLaggingApplier TestS12dPostERSStaleShardRecord TestS13T1TornLastTrx
  TestS13T2TornAckedTrx TestS13T3TornAckedTrxPrimaryDies TestS13T4ApplierDuplicateKey"
PROBE="TestP1IsolatedPrimaryAcksAfterFailover TestP2ReplicationPartitionAcksOnDeposedPrimary"
FIXED_3="$PROBE TestS3IsolatePrimary TestS7dFlappingPrimaryLong TestS9iPrimaryCellPartition TestS11kRelayLogDiscardKill9"
FIXED_1="$PROBE TestS3IsolatePrimary"

# run <results dir> <profile> <primary cell or ""> <scenarios...>
run() {
  local dir=$1 profile=$2 cell=$3
  shift 3
  echo "== $dir"
  # shellcheck disable=SC2068 # the scenario lists are split into words on purpose
  CHAOS_PROFILE=$profile CHAOS_PRIMARY_CELL=$cell CHAOS_RESULTS_DIR="$OUT/$dir" "$M" $@
  export CHAOS_SKIP_BUILD=1 # the test binary is built once, by the first run
}

case "${1:-}" in
main)
  run main-semisync-3vtorc semisync-3vtorc "" $CORE $SINGLE
  run main-semisync-1vtorc-colo semisync-1vtorc zone1 $CORE
  run main-semisync-1vtorc-remote semisync-1vtorc zone2 $CORE
  run main-semisync-3vtorc semisync-3vtorc "" $RELAY
  ;;
probe)
  for i in 1 2; do
    run main-probe-semisync-3vtorc-$i semisync-3vtorc "" $PROBE
    run main-probe-semisync-1vtorc-$i semisync-1vtorc zone1 $PROBE
  done
  ;;
fixed)
  for i in 1 2; do
    run fixed-semisync-3vtorc-$i semisync-3vtorc "" $FIXED_3
    run fixed-semisync-1vtorc-$i semisync-1vtorc zone1 $FIXED_1
  done
  ;;
*)
  echo "usage: $0 main|probe|fixed" >&2
  exit 2
  ;;
esac
