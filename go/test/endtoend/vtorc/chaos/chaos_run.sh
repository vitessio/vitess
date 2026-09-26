#!/bin/bash
# Runs the chaos scenarios. Must be run as root (it prepares cgroups and then drops to the
# unprivileged "vitess" user, keeping CAP_NET_ADMIN for iptables), e.g.:
#
#   go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestS1KillPrimaryMysqld$' -test.v -test.timeout 30m
#
# Arguments are passed verbatim to the compiled test binary (use -test.* flag names).
# Reports and log copies go to $CHAOS_RESULTS_DIR (default /home/vitess/chaos-results).
set -euo pipefail

VTROOT=${VTROOT:-/home/user/vitess}
cd "$VTROOT"
source build.env >/dev/null
export PATH="$VTROOT/bin:/usr/local/bin:$PATH"

RUN_USER=${RUN_USER:-vitess}
CG=/sys/fs/cgroup/unified/chaos
RESULTS=${CHAOS_RESULTS_DIR:-/home/$RUN_USER/chaos-results}
BIN=/home/$RUN_USER/e2e-bins/chaos.test

mkdir -p "$CG"
for g in harness infra vtgate tablet1 tablet2 tablet3 orc1 orc2 orc3 etcd1 etcd2 etcd3; do
  mkdir -p "$CG/$g"
  chown "$RUN_USER" "$CG/$g" "$CG/$g/cgroup.procs" "$CG/$g/cgroup.kill" 2>/dev/null || true
done
chown "$RUN_USER" "$CG" "$CG/cgroup.procs"

mkdir -p "$(dirname "$BIN")" "$RESULTS" "$VTDATAROOT" /home/$RUN_USER/tmp
go test -c -o "$BIN" ./go/test/endtoend/vtorc/chaos
chown "$RUN_USER" "$BIN" "$RESULTS" "$VTDATAROOT" /home/$RUN_USER/tmp

# Move this shell into the harness cgroup; the test process inherits it.
echo $$ > "$CG/harness/cgroup.procs"

cd go/test/endtoend/vtorc/chaos
exec env HOME=/home/$RUN_USER USER=$RUN_USER TMPDIR=/home/$RUN_USER/tmp CHAOS_E2E=1 CHAOS_RESULTS_DIR="$RESULTS" \
  setpriv --reuid="$RUN_USER" --regid="$RUN_USER" --init-groups \
  --inh-caps=+net_admin,+net_raw --ambient-caps=+net_admin,+net_raw \
  "$BIN" "$@"
