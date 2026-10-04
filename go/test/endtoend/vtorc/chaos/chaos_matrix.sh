#!/bin/bash
# Runs a list of chaos scenarios one by one under a profile, each in its own test binary run, and
# prints a one-line summary per scenario. Must be run as root, like chaos_run.sh.
#
#   CHAOS_PROFILE=semisync-3vtorc go/test/endtoend/vtorc/chaos/chaos_matrix.sh TestS1KillPrimaryMysqld TestS3IsolatePrimary
#
# Reports go to $CHAOS_RESULTS_DIR/<scenario>/report.txt, the test output to
# $CHAOS_RESULTS_DIR/<test>.log. CHAOS_SCENARIO_TIMEOUT bounds each scenario (default 20m).
set -u
DIR=$(cd "$(dirname "$0")" && pwd)
RUN_USER=${RUN_USER:-ubuntu}
export CHAOS_RESULTS_DIR=${CHAOS_RESULTS_DIR:-/home/$RUN_USER/chaos-results/${CHAOS_PROFILE:-audit}}
mkdir -p "$CHAOS_RESULTS_DIR"
chown "$RUN_USER" "$CHAOS_RESULTS_DIR"
if [ "${CHAOS_SKIP_BUILD:-0}" != 1 ]; then
  (cd "$DIR/../../../../.." && source build.env >/dev/null && go test -c -o "/home/$RUN_USER/e2e-bins/chaos.test" ./go/test/endtoend/vtorc/chaos) || exit 1
fi
for t in "$@"; do
  log="$CHAOS_RESULTS_DIR/$t.log"
  start=$(date +%s)
  CHAOS_SKIP_BUILD=1 "$DIR/chaos_run.sh" -test.run "^$t\$" -test.v -test.timeout "${CHAOS_SCENARIO_TIMEOUT:-20m}" > "$log" 2>&1
  rc=$?
  viol=$(grep -m1 -oE -- '-- VIOLATIONS \([0-9]+\)' "$log" | grep -oE '[0-9]+')
  echo "$(date +%H:%M:%S) $t rc=$rc violations=${viol:-?} took=$(( $(date +%s) - start ))s"
done
