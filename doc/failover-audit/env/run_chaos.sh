#!/bin/bash
# Runs chaos harness scenarios (go/test/endtoend/vtorc/chaos) with a clean slate, as root:
#
#   doc/failover-audit/env/run_chaos.sh <label> <test-regex> [extra -test.* args]
#   doc/failover-audit/env/run_chaos.sh s1 '^TestS1KillPrimaryMysqld$'
#
# Reports go to $CHAOS_RESULTS_BASE/<label>/<scenario>/report.txt (with events.txt and logs/),
# and the test output to $CHAOS_RESULTS_BASE/<label>/run.log. CHAOS_RESULTS_BASE defaults to
# /home/$RUN_USER/chaos-results.
#
# Environment (all optional):
#   VTROOT              checkout whose harness is compiled (default: this checkout)
#   BINDIR              Vitess binaries the cluster runs (default: $VTROOT/bin), e.g. a build of
#                       claude/preserve-relay-logs-on-repoint in another worktree
#   RELAYLOG_SAFE=1     run every tablet's mysqld with relaylog-safe.cnf
#                       (relay_log_recovery=0, sync_relay_log=1); S13 sets this up itself
#   CHAOS_VTTABLET_HEARTBEAT=1   run vttablets with --heartbeat-enable --heartbeat-interval 1s
#   CHAOS_S11_CLEAR_DELAY=1      S11b/S11c: clear R1's apply delay before the primary dies
#                                (needed with binaries that keep the relay log on repoint)
#   S12_RESTORE_FROM_BACKUP=1    S12: keep --restore-from-backup when restarting the replica
set -uo pipefail

label=$1
shift
re=$1
shift
ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=e2e_env.sh
source "$ENV_DIR/e2e_env.sh" >/dev/null

if [ "${RELAYLOG_SAFE:-0}" = "1" ]; then
  export EXTRA_MY_CNF="$ENV_DIR/relaylog-safe.cnf"
fi
base=${CHAOS_RESULTS_BASE:-/home/$RUN_USER/chaos-results}
export CHAOS_RESULTS_DIR="$base/$label"
mkdir -p "$CHAOS_RESULTS_DIR"
chown -R "$RUN_USER" "$base"

e2e_clean
echo "label=$label tests=$re VTROOT=$VTROOT BINDIR=${BINDIR:-$VTROOT/bin} EXTRA_MY_CNF=${EXTRA_MY_CNF:-} results=$CHAOS_RESULTS_DIR"
"$VTROOT/go/test/endtoend/vtorc/chaos/chaos_run.sh" -test.run "$re" -test.v -test.timeout 30m "$@" \
  > "$CHAOS_RESULTS_DIR/run.log" 2>&1
rc=$?
echo "rc=$rc" >> "$CHAOS_RESULTS_DIR/run.log"
e2e_clean
for r in "$CHAOS_RESULTS_DIR"/*/report.txt; do
  [ -e "$r" ] && sed -n '1,/-- NOTES/p' "$r"
done
echo "rc=$rc (full output: $CHAOS_RESULTS_DIR/run.log)"
exit $rc
